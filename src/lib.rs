extern crate rand;

use rand::Rng;
use std::fmt;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::collections::BTreeMap;

/// SiphasherはRust1.13.0で非推奨になった。しかしそれを置き換えるSipHasher24は
/// 現状では非安定(unstable)なため、安定版のRustリリースは利用できない。
#[allow(deperaceted)]
use std::hash::SipHasher;

/// 推定アルゴリズム。デバッグ出力用
pub enum Estimator {
    HyperLogLog,
    LinerCounting  // スモールレンジの見積もりに使用する
}

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

/// `HyperLogLog`のデバッグ用文字列を返す。
impl fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (est, est_method) = estimate_cardinality(self);
        write!(f,
        r#"HyperLogLog
  estimated cardinality: {}
  estimation method:     {:?}
  -----------------------------------------------------
  b:      {} bits (typical error rate: {}%)
  m:      {} registers
  alpha:  {}
  hasher: ({}, {})"#,
               est,
               est_method,
               self.b,
               self.typical_error_rate() * 100.0,
               self.m,
               self.alpha,
               self.hasher_key0,
               self.hasher_key1)
    }
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
        let p2 = &mut self.registers[j];
        if *p2 < p1 {
            *p2 = p1;
        }
    }

    /// カーディナリティの見積もり値を返す
    pub fn cardinality(&self) -> f64 {
        estimate_cardinality(self).0
    }

    /// b から予想される典型的なエラー率を返す
    pub fn typical_error_rate(&self) -> f64 {
        1.04 / (self.m as f64).sqrt()
    }

    /// 与えられたvalueに対する64ビットのハッシュ値を求める。
    #[allow(deprecated)] // SipHasherがRust1.13.0で非推奨(deprecated)のため
    fn hash<H: Hash>(&self, value: &H) -> u64 {
        let mut hasher = SipHasher::new_with_keys(self.hasher_key0, self.hasher_key1);
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// レジスタに格納された値について、その分布を示すヒストグラムを返す。
    pub fn histgram_of_register_value_distribution(&self) -> String {
        let mut histgram = Vec::new();

        let mut map = BTreeMap::new();
        for x in &self.registers {
            let count = map.entry(*x).or_insert(0);
            *count += 1;
        }

        if let (Some(last_reg_value), Some(max_count)) = (map.keys().last(), map.values().max()) {
            // グラフの最大幅 = 40文字
            let width = 40.0;
            let rate  = width / (*max_count as f64);

            for i 0..(last_reg_value + 1) {
                let mut line = format!("{:3}: ", i);

                if let Some(count) = map.get(&i) {
                    // アスタリスク(*)で横棒を描く
                    let h_bar = str::iter::repeat("*")
                        .take((*count as f64 * rate).cell() as usize)
                        .collect::<String>();
                    line.push_str(&h_bar);
                    line.push_str(&format!("  {}", count));                    
                } else {
                    line.push_str("0");
                };

                histgram.push(line);
            }
        }
        histgram.join("\n")
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

/// カーディナリティを推定し、その値と見積もりに使用したアルゴリズムを返す
/// スモールレンジでは`Linear Counting`アルゴリズムを使用し、それを超えるレンジでは
/// `HyperLogLog`アルゴリズムを使用する。ここまでは論文の通り。
/// しかし、論文にあるラーレンジ補正は行わない。なぜなら、本実装では、32ビットの
/// ハッシュ値の代わりに64ビットのハッシュ値を使用しており、ハッシュ値が衝突する
/// 頻度が極めて低いと予想されるため
fn estimate_cardinality(hll: &HyperLogLog) -> (f64, Estimator) {
    let m_64 = hll.m as f64;
    // まず`HyperLogLog`アルゴリズムによる見積もり値を算出する
    let est = raw_hyperloglog_estimate(hll.alpha, m_64, &hll.registers);

    if est < (5.0 / 2.0 * m_64) {
        // スモールレンジの見積もりを行う。もし値が0のレジスタが一つでもあるならば
        // `Linear Counting`アルゴリズムで見積もりし直す。
        match count_zero_registers(&hll.registers) {
            0 => (est, Estimator::HyperLogLog),
            v => (linear_counting_estimate(m_f64, v as f64), Estimator::LinerCounting),
        }
    } else {
        (est, Estimator::HyperLogLog)
    }
}

/// 値が0のレジスタの個数を返す
fn count_zero_registers(registers: &[u8]) -> usize {
    registers.iter().filter(|&x| *x == 0).count()
}

/// `HyperLogLog`アルゴリズムによる未補正の見積もり値を計算する
fn raw_hyperloglog_estimate(alpha: f64, m: f64, registers: &[u8]) -> f64 {
    let sum = registers.iter().map(|&x| 2.0f64.powi(-(x as i32))).sum::<f64>();
    alpha * m * m / sum
}

/// `Linear Counting`アルゴリズムによる見積もり値を算出する
fn linear_counting_estimate(m: f64, number_of_zero_registers: f64) -> f64 {
    m * (m / number_of_zero_registers).ln()
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

    #[test]
    fn small_range() {
        let mut hll = HyperLogLog::new(12).unwrap();
        let items = ["test1", "test2", "test3", "test2", "test2", "test2"];

        println!("\n=== Loading {} items.\n", items.len());
        for item in &items {
            hll.insert(item);
        }
        
    }
}
