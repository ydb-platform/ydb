//! Utilities to compare floating-point numbers.

use float_cmp::ApproxEq;

// The following are copied from cairo/src/{cairo-fixed-private.h,
// cairo-fixed-type-private.h}

const CAIRO_FIXED_FRAC_BITS: u64 = 8;

const CAIRO_FIXED_MAX: i32 = i32::MAX;
const CAIRO_FIXED_MIN: i32 = i32::MIN;

/// The double that corresponds to (the number one in fixed-point representation)
const CAIRO_FIXED_ONE_DOUBLE: f64 = (1 << CAIRO_FIXED_FRAC_BITS) as f64;

/// The largest representable fixed-point number, as a double.  This is a bit over 8 million.
pub const CAIRO_FIXED_MAX_DOUBLE: f64 = (CAIRO_FIXED_MAX as f64) / CAIRO_FIXED_ONE_DOUBLE;

/// The most negative representable fixed-point number, as a double.  This is a bit less than -8 million.
pub const CAIRO_FIXED_MIN_DOUBLE: f64 = (CAIRO_FIXED_MIN as f64) / CAIRO_FIXED_ONE_DOUBLE;

/// Checks whether two floating-point numbers are approximately equal,
/// considering Cairo's limitations on numeric representation.
///
/// Cairo uses fixed-point numbers internally.  We implement this
/// trait for `f64`, so that two numbers can be considered "close
/// enough to equal" if their absolute difference is smaller than the
/// smallest fixed-point fraction that Cairo can represent.
///
/// Note that this trait is reliable even if the given numbers are
/// outside of the range that Cairo's fixed-point numbers can
/// represent.  In that case, we check for the absolute difference,
/// and finally allow a difference of 1 unit-in-the-last-place (ULP)
/// for very large f64 values.
pub trait ApproxEqCairo: ApproxEq {
    fn approx_eq_cairo(self, other: Self) -> bool;
}

impl ApproxEqCairo for f64 {
    fn approx_eq_cairo(self, other: f64) -> bool {
        let cairo_smallest_fraction = 1.0 / f64::from(1 << CAIRO_FIXED_FRAC_BITS);
        self.approx_eq(other, (cairo_smallest_fraction, 1))
    }
}

// Macro for usage in unit tests
#[doc(hidden)]
#[macro_export]
macro_rules! assert_approx_eq_cairo {
    ($left:expr, $right:expr) => {{
        match ($left, $right) {
            (l, r) => {
                if !l.approx_eq_cairo(r) {
                    panic!(
                        r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`"#,
                        l, r
                    )
                }
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_approx_equal() {
        // 0 == 1/256 - cairo can represent it, so not equal
        assert!(!0.0_f64.approx_eq_cairo(0.00390635_f64));

        // 1 == 1 + 1/256 - cairo can represent it, so not equal
        assert!(!1.0_f64.approx_eq_cairo(1.00390635_f64));

        // 0 == 1/256 - cairo can represent it, so not equal
        assert!(!0.0_f64.approx_eq_cairo(-0.00390635_f64));

        // 1 == 1 - 1/256 - cairo can represent it, so not equal
        assert!(!1.0_f64.approx_eq_cairo(0.99609365_f64));

        // 0 == 1/512 - cairo approximates to 0, so equal
        assert!(0.0_f64.approx_eq_cairo(0.001953125_f64));

        // 1 == 1 + 1/512 - cairo approximates to 1, so equal
        assert!(1.0_f64.approx_eq_cairo(1.001953125_f64));

        // 0 == -1/512 - cairo approximates to 0, so equal
        assert!(0.0_f64.approx_eq_cairo(-0.001953125_f64));

        // 1 == 1 - 1/512 - cairo approximates to 1, so equal
        assert!(1.0_f64.approx_eq_cairo(0.998046875_f64));

        // This is 2^53 compared to (2^53 + 2).  When represented as
        // f64, they are 1 unit-in-the-last-place (ULP) away from each
        // other, since the mantissa has 53 bits (52 bits plus 1
        // "hidden" bit).  The first number is an exact double, and
        // the second one is the next biggest double.  We consider a
        // difference of 1 ULP to mean that numbers are "equal", to
        // account for slight imprecision in floating-point
        // calculations.  Most of the time, for small values, we will
        // be using the cairo_smallest_fraction from the
        // implementation of approx_eq_cairo() above.  For large
        // values, we want the ULPs.
        //
        // In the second assertion, we compare 2^53 with (2^53 + 4).  Those are
        // 2 ULPs away, and we don't consider them equal.
        assert!(9_007_199_254_740_992.0.approx_eq_cairo(9_007_199_254_740_994.0));
        assert!(!9_007_199_254_740_992.0.approx_eq_cairo(9_007_199_254_740_996.0));
    }

    #[test]
    fn assert_approx_eq_cairo_should_not_panic() {
        assert_approx_eq_cairo!(42_f64, 42_f64);
    }

    #[test]
    #[should_panic]
    fn assert_approx_eq_cairo_should_panic() {
        assert_approx_eq_cairo!(3_f64, 42_f64);
    }
}
