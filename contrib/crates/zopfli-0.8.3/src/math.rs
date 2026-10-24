#![cfg(not(feature = "std"))]
#![allow(dead_code)] // False-positive

/// Provides math operations for doubles on `no_std` targets that are not available on `core`.
pub trait F64MathExt {
    /// Computes the absolute value of `self`.
    // TODO this can be removed when the MSRV is equal or greater than 1.84: https://github.com/rust-lang/rust/issues/139066
    #[must_use = "method returns a new number and does not mutate the original value"]
    fn abs(self) -> Self;

    /// Returns the base 2 logarithm of the number.
    #[must_use = "method returns a new number and does not mutate the original value"]
    fn log2(self) -> Self;
}

impl F64MathExt for f64 {
    #[inline(always)]
    fn abs(self) -> Self {
        f64::from_bits(self.to_bits() & ((1u64 << 63) - 1))
    }

    // Function taken from:
    // https://github.com/rust-lang/libm/blob/ef3cc6be6a10ec24af9dba26a76a7831c1d11c70/src/math/log2.rs
    // See the link above for author and licensing information of this code snippet
    #[allow(clippy::eq_op, clippy::needless_late_init, clippy::excessive_precision)]
    fn log2(mut self) -> Self {
        const IVLN2HI: f64 = 1.44269504072144627571e+00; /* 0x3ff71547, 0x65200000 */
        const IVLN2LO: f64 = 1.67517131648865118353e-10; /* 0x3de705fc, 0x2eefa200 */
        const LG1: f64 = 6.666666666666735130e-01; /* 3FE55555 55555593 */
        const LG2: f64 = 3.999999999940941908e-01; /* 3FD99999 9997FA04 */
        const LG3: f64 = 2.857142874366239149e-01; /* 3FD24924 94229359 */
        const LG4: f64 = 2.222219843214978396e-01; /* 3FCC71C5 1D8E78AF */
        const LG5: f64 = 1.818357216161805012e-01; /* 3FC74664 96CB03DE */
        const LG6: f64 = 1.531383769920937332e-01; /* 3FC39A09 D078C69F */
        const LG7: f64 = 1.479819860511658591e-01; /* 3FC2F112 DF3E5244 */

        let x1p54 = f64::from_bits(0x4350000000000000); // 0x1p54 === 2 ^ 54

        let mut ui: u64 = self.to_bits();
        let hfsq: f64;
        let f: f64;
        let s: f64;
        let z: f64;
        let r: f64;
        let mut w: f64;
        let t1: f64;
        let t2: f64;
        let y: f64;
        let mut hi: f64;
        let lo: f64;
        let mut val_hi: f64;
        let mut val_lo: f64;
        let mut hx: u32;
        let mut k: i32;

        hx = (ui >> 32) as u32;
        k = 0;
        if hx < 0x00100000 || (hx >> 31) > 0 {
            if ui << 1 == 0 {
                return -1. / (self * self); /* log(+-0)=-inf */
            }
            if (hx >> 31) > 0 {
                return (self - self) / 0.0; /* log(-#) = NaN */
            }
            /* subnormal number, scale self up */
            k -= 54;
            self *= x1p54;
            ui = self.to_bits();
            hx = (ui >> 32) as u32;
        } else if hx >= 0x7ff00000 {
            return self;
        } else if hx == 0x3ff00000 && ui << 32 == 0 {
            return 0.;
        }

        /* reduce self into [sqrt(2)/2, sqrt(2)] */
        hx += 0x3ff00000 - 0x3fe6a09e;
        k += (hx >> 20) as i32 - 0x3ff;
        hx = (hx & 0x000fffff) + 0x3fe6a09e;
        ui = ((hx as u64) << 32) | (ui & 0xffffffff);
        self = f64::from_bits(ui);

        f = self - 1.0;
        hfsq = 0.5 * f * f;
        s = f / (2.0 + f);
        z = s * s;
        w = z * z;
        t1 = w * (LG2 + w * (LG4 + w * LG6));
        t2 = z * (LG1 + w * (LG3 + w * (LG5 + w * LG7)));
        r = t2 + t1;

        /* hi+lo = f - hfsq + s*(hfsq+R) ~ log(1+f) */
        hi = f - hfsq;
        ui = hi.to_bits();
        ui &= (-1i64 as u64) << 32;
        hi = f64::from_bits(ui);
        lo = f - hi - hfsq + s * (hfsq + r);

        val_hi = hi * IVLN2HI;
        val_lo = (lo + hi) * IVLN2LO + lo * IVLN2HI;

        /* spadd(val_hi, val_lo, y), except for not using double_t: */
        y = k.into();
        w = y + val_hi;
        val_lo += (y - w) + val_hi;
        val_hi = w;

        val_lo + val_hi
    }
}
