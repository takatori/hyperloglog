extern crate rand;

use rand::Rng;
use std::error::Error;
use std::hash::{Hash, Hasher};

/// SiphasherはRust1.13.0で非推奨になった。しかしそれを置き換えるSipHasher24は
/// 現状では非安定(unstable)なため、安定版のRustリリースは利用できない。
#[allow(deperaceted)]
use std::hash::SipHasher;

/// `HyperLogLog`オブジェクト
pub struct HyperLogLog {
    // レジスタのアドレッシングに使う2進数のビット数
    // 範囲は4以上、16以下で、大きいほど見積もり誤差が少なくなるが、その分メモリを使用する。
    b: u8,
    // usizes型のハッシュ値の右からbビットを取り出すためのマスク
    b_mask: usize,
    // レジスタの数(2のb乗)。例: b = 4 → 16、b = 16 → 65536
    m: usize,
    alpha: f64,
    // レジスタ。サイズが mバイトのバイト配列
    registers: Vec<u8>,
    // SipHasher の初期化に使うキー
    hasher_key0: u64,
    hasher_key1: u64,    
}



impl HyperLogLog {

    /// `HyperLogLog`オブジェクトを作成する。bで指定したビット数をレジスタの
    /// アドレッシングに使用する。bの範囲は4以上、16以下でなければならない
    /// 範囲外なら`Err`を返す
    pub fn new(b: u8) -> Result<Self, Box<Error>> {
        if b < 4 || b > 16 {
            return Err(From::from(format!("b must be between 4 and 16. b = {}", b)))
        }
        /// 構造体のフィールド`m`は2のb条。シフト演算で実装
        let m     = 1 << b;
        let alpha = get_alpha(b)?;
        // hasher_key0, key1を初期化するための乱数ジェネレータ
        let mut rng = rand::OsRng::new().map_err(|e| format!("Failed to create an OS RNG: {}", e))?;

        Ok(HyperLogLog {
            alpha: alpha,
            b: b,
            b_mask: m - 1,
            m: m,
            registers: vec![0; m],
            hasher_key0: rng.gen(),
            hasher_key1: rng.gen(),            
        })
    }

    /// 要素を追加する。要素は`std::hash::Hash`トレイトを実装していなければならない
    pub fn insert<H: Hash>(&mut self, value: &H) {
        let x = self.hash(value);
        let j = x as usize & self.b_mask;
        let w = x >> self.b;

        let p1 = position_of_leftmost_one_bit(w, 64 - self.b);
    }


    /// 与えられたvalueに対する64ビットのハッシュ値を求める。
    #[allow(deprecated)] // SipHasherがRust1.13.0で非推奨(deprecated)のため
    fn hash<H: Hash>(&self, value: &H) -> u64 {
        let mut hasher = SipHasher::new_with_keys(self.hasher_key0, self.hasher_key1);
        value.hash(&mut hasher);
        hasher.finish()
    }
}


/// ビット数bに対応するα値を返す。
fn get_alpha(b: u8) -> Result<f64, Box<Error>> {
    if b < 4 || b > 16 {
        Err(From::from(format!("b must be between 4 and 16. b = {}", b)))
    } else {
        Ok(match b {
            4 => 0.673, // α16
            5 => 0.697, // α32
            6 => 0.709, // α64
            _ => 0.7213 / (1.0 + 1.079 / (1 << b) as f64),
        })
    }
}


/// ハッシュ値(64ビット符号なしの2進数)の左端からみて最初に出現した1の位置を返す
/// 例: 10000... -> 1、00010... -> 4
fn position_of_leftmost_one_bit(s: u64, max_width: u8) -> u8 {
    count_leading_zeros(s, max_width) + 1
}

/// ハッシュ値(64ビット符号なし2進数)左端に連続して並んでいる0の個数を返す.
/// 10000... -> 0、00010... -> 3
fn count_leading_zeros(mut s: u64, max_width: u8) -> u8 {
    let mut lz = max_width;
    while s != 0 {
        lz -= 1;
        s >>= 1;
    }
    lz
}


// テストケース
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_hll() {
        
        use std::f64;
        
        assert!(HyperLogLog::new(3).is_err());
        assert!(HyperLogLog::new(17).is_err());

        let hll = HyperLogLog::new(4);
        assert!(hll.is_ok());

        let hll = hll.unwrap();
        assert_eq!(hll.b, 4);
        assert_eq!(hll.m, 2_f64.powi(4) as usize);
        assert_eq!(hll.alpha, 0.673);
        assert_eq!(hll.registers.len(), 2_f64.powi(4) as usize);

        assert!(HyperLogLog::new(16).is_ok());
    }
}
