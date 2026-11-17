//! CSS angle values.

use std::f64::consts::*;

use cssparser::{Parser, Token};
use float_cmp::approx_eq;

use crate::error::*;
use crate::parsers::{Parse, finite_f32};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Angle(f64);

impl Angle {
    pub fn new(rad: f64) -> Angle {
        Angle(Angle::normalize(rad))
    }

    pub fn from_degrees(deg: f64) -> Angle {
        Angle(Angle::normalize(deg.to_radians()))
    }

    pub fn from_vector(vx: f64, vy: f64) -> Angle {
        let rad = vy.atan2(vx);

        if rad.is_nan() {
            Angle(0.0)
        } else {
            Angle(Angle::normalize(rad))
        }
    }

    pub fn radians(self) -> f64 {
        self.0
    }

    pub fn bisect(self, other: Angle) -> Angle {
        let half_delta = (other.0 - self.0) * 0.5;

        if FRAC_PI_2 < half_delta.abs() {
            Angle(Angle::normalize(self.0 + half_delta - PI))
        } else {
            Angle(Angle::normalize(self.0 + half_delta))
        }
    }

    //Flips an angle to be 180deg or PI radians rotated
    pub fn flip(self) -> Angle {
        Angle::new(self.radians() + PI)
    }

    // Normalizes an angle to [0.0, 2*PI)
    fn normalize(rad: f64) -> f64 {
        let res = rad % (PI * 2.0);
        if approx_eq!(f64, res, 0.0) {
            0.0
        } else if res < 0.0 {
            res + PI * 2.0
        } else {
            res
        }
    }
}

// angle:
// https://www.w3.org/TR/SVG/types.html#DataTypeAngle
//
// angle ::= number ("deg" | "grad" | "rad")?
//
impl Parse for Angle {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Angle, ParseError<'i>> {
        let angle = {
            let loc = parser.current_source_location();

            let token = parser.next()?;

            match *token {
                Token::Number { value, .. } => {
                    let degrees = finite_f32(value).map_err(|e| loc.new_custom_error(e))?;
                    Angle::from_degrees(f64::from(degrees))
                }

                Token::Dimension {
                    value, ref unit, ..
                } => {
                    let value = f64::from(finite_f32(value).map_err(|e| loc.new_custom_error(e))?);

                    match unit.as_ref() {
                        "deg" => Angle::from_degrees(value),
                        "grad" => Angle::from_degrees(value * 360.0 / 400.0),
                        "rad" => Angle::new(value),
                        "turn" => Angle::from_degrees(value * 360.0),
                        _ => {
                            return Err(loc.new_unexpected_token_error(token.clone()));
                        }
                    }
                }

                _ => return Err(loc.new_unexpected_token_error(token.clone())),
            }
        };

        Ok(angle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_angle() {
        assert_eq!(Angle::parse_str("0").unwrap(), Angle::new(0.0));
        assert_eq!(Angle::parse_str("15").unwrap(), Angle::from_degrees(15.0));
        assert_eq!(
            Angle::parse_str("180.5deg").unwrap(),
            Angle::from_degrees(180.5)
        );
        assert_eq!(Angle::parse_str("1rad").unwrap(), Angle::new(1.0));
        assert_eq!(
            Angle::parse_str("-400grad").unwrap(),
            Angle::from_degrees(-360.0)
        );
        assert_eq!(
            Angle::parse_str("0.25turn").unwrap(),
            Angle::from_degrees(90.0)
        );

        assert!(Angle::parse_str("").is_err());
        assert!(Angle::parse_str("foo").is_err());
        assert!(Angle::parse_str("300foo").is_err());
    }

    fn test_bisection_angle(
        expected: f64,
        incoming_vx: f64,
        incoming_vy: f64,
        outgoing_vx: f64,
        outgoing_vy: f64,
    ) {
        let i = Angle::from_vector(incoming_vx, incoming_vy);
        let o = Angle::from_vector(outgoing_vx, outgoing_vy);
        let bisected = i.bisect(o);
        assert!(approx_eq!(f64, expected, bisected.radians()));
    }

    #[test]
    fn bisection_angle_is_correct_from_incoming_counterclockwise_to_outgoing() {
        // 1st quadrant
        test_bisection_angle(FRAC_PI_4, 1.0, 0.0, 0.0, 1.0);

        // 2nd quadrant
        test_bisection_angle(FRAC_PI_2 + FRAC_PI_4, 0.0, 1.0, -1.0, 0.0);

        // 3rd quadrant
        test_bisection_angle(PI + FRAC_PI_4, -1.0, 0.0, 0.0, -1.0);

        // 4th quadrant
        test_bisection_angle(PI + FRAC_PI_2 + FRAC_PI_4, 0.0, -1.0, 1.0, 0.0);
    }

    #[test]
    fn bisection_angle_is_correct_from_incoming_clockwise_to_outgoing() {
        // 1st quadrant
        test_bisection_angle(FRAC_PI_4, 0.0, 1.0, 1.0, 0.0);

        // 2nd quadrant
        test_bisection_angle(FRAC_PI_2 + FRAC_PI_4, -1.0, 0.0, 0.0, 1.0);

        // 3rd quadrant
        test_bisection_angle(PI + FRAC_PI_4, 0.0, -1.0, -1.0, 0.0);

        // 4th quadrant
        test_bisection_angle(PI + FRAC_PI_2 + FRAC_PI_4, 1.0, 0.0, 0.0, -1.0);
    }

    #[test]
    fn bisection_angle_is_correct_for_more_than_quarter_turn_angle() {
        test_bisection_angle(0.0, 0.1, -1.0, 0.1, 1.0);

        test_bisection_angle(FRAC_PI_2, 1.0, 0.1, -1.0, 0.1);

        test_bisection_angle(PI, -0.1, 1.0, -0.1, -1.0);

        test_bisection_angle(PI + FRAC_PI_2, -1.0, -0.1, 1.0, -0.1);
    }
}
