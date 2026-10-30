//! A proportional-integral-derivative (PID) controller library.
//!
//! See [Pid] for the adjustable controller itself, as well as [ControlOutput] for the outputs and weights which you can use after setting up your controller. Follow the complete example below to setup your first controller!
//!
//! # Example
//!
//! ```rust
//! use pid::Pid;
//!
//! // Create a new proportional-only PID controller with a setpoint of 15
//! let mut pid = Pid::new(15.0, 100.0);
//! pid.p(10.0, 100.0);
//!
//! // Input a measurement with an error of 5.0 from our setpoint
//! let output = pid.next_control_output(10.0);
//!
//! // Show that the error is correct by multiplying by our kp
//! assert_eq!(output.output, 50.0); // <--
//! assert_eq!(output.p, 50.0);
//!
//! // It won't change on repeat; the controller is proportional-only
//! let output = pid.next_control_output(10.0);
//! assert_eq!(output.output, 50.0); // <--
//! assert_eq!(output.p, 50.0);
//!
//! // Add a new integral term to the controller and input again
//! pid.i(1.0, 100.0);
//! let output = pid.next_control_output(10.0);
//!
//! // Now that the integral makes the controller stateful, it will change
//! assert_eq!(output.output, 55.0); // <--
//! assert_eq!(output.p, 50.0);
//! assert_eq!(output.i, 5.0);
//!
//! // Add our final derivative term and match our setpoint target
//! pid.d(2.0, 100.0);
//! let output = pid.next_control_output(15.0);
//!
//! // The output will now say to go down due to the derivative
//! assert_eq!(output.output, -5.0); // <--
//! assert_eq!(output.p, 0.0);
//! assert_eq!(output.i, 5.0);
//! assert_eq!(output.d, -10.0);
//! ```
#![no_std]

use num_traits::float::FloatCore;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Adjustable proportional-integral-derivative (PID) controller.
///
/// # Examples
///
/// This controller provides a builder pattern interface which allows you to pick-and-choose which PID inputs you'd like to use during operation. Here's what a basic proportional-only controller could look like:
///
/// ```rust
/// use pid::Pid;
///
/// // Create limited controller
/// let mut p_controller = Pid::new(15.0, 100.0);
/// p_controller.p(10.0, 100.0);
///
/// // Get first output
/// let p_output = p_controller.next_control_output(400.0);
/// ```
///
/// This controller would give you set a proportional controller to `10.0` with a target of `15.0` and an output limit of `100.0` per [output](Self::next_control_output) iteration. The same controller with a full PID system built in looks like:
///
/// ```rust
/// use pid::Pid;
///
/// // Create full PID controller
/// let mut full_controller = Pid::new(15.0, 100.0);
/// full_controller.p(10.0, 100.0).i(4.5, 100.0).d(0.25, 100.0);
///
/// // Get first output
/// let full_output = full_controller.next_control_output(400.0);
/// ```
///
/// This [`next_control_output`](Self::next_control_output) method is what's used to input new values into the controller to tell it what the current state of the system is. In the examples above it's only being used once, but realistically this will be a hot method. Please see [ControlOutput] for examples of how to handle these outputs; it's quite straight forward and mirrors the values of this structure in some ways.
///
/// The last item of note is that these [`p`](Self::p()), [`i`](Self::i()), and [`d`](Self::d()) methods can be used *during* operation which lets you add and/or modify these controller values if need be.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct Pid<T: FloatCore> {
    /// Ideal setpoint to strive for.
    pub setpoint: T,
    /// Defines the overall output filter limit.
    pub output_limit: T,
    /// Proportional gain.
    pub kp: T,
    /// Integral gain.
    pub ki: T,
    /// Derivative gain.
    pub kd: T,
    /// Limiter for the proportional term: `-p_limit <= P <= p_limit`.
    pub p_limit: T,
    /// Limiter for the integral term: `-i_limit <= I <= i_limit`.
    pub i_limit: T,
    /// Limiter for the derivative term: `-d_limit <= D <= d_limit`.
    pub d_limit: T,
    /// Last calculated integral value if [Pid::ki] is used.
    integral_term: T,
    /// Previously found measurement whilst using the [Pid::next_control_output] method.
    prev_measurement: Option<T>,
}

/// Output of [controller iterations](Pid::next_control_output) with weights
///
/// # Example
///
/// This structure is simple to use and features three weights: [p](Self::p), [i](Self::i), and [d](Self::d). These can be used to figure out how much each term from [Pid] contributed to the final [output](Self::output) value which should be taken as the final controller output for this iteration:
///
/// ```rust
/// use pid::{Pid, ControlOutput};
///
/// // Setup controller
/// let mut pid = Pid::new(15.0, 100.0);
/// pid.p(10.0, 100.0).i(1.0, 100.0).d(2.0, 100.0);
///
/// // Input an example value and get a report for an output iteration
/// let output = pid.next_control_output(26.2456);
/// println!("P: {}\nI: {}\nD: {}\nFinal Output: {}", output.p, output.i, output.d, output.output);
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ControlOutput<T: FloatCore> {
    /// Contribution of the P term to the output.
    pub p: T,
    /// Contribution of the I term to the output.
    ///
    /// This integral term is equal to `sum[error(t) * ki(t)] (for all t)`
    pub i: T,
    /// Contribution of the D term to the output.
    pub d: T,
    /// Output of the PID controller.
    pub output: T,
}

impl<T> Pid<T>
where
    T: FloatCore,
{
    /// Creates a new controller with the target setpoint and the output limit
    ///
    /// To set your P, I, and D terms into this controller, please use the following builder methods:
    /// - [Self::p()]: Proportional term setting
    /// - [Self::i()]: Integral term setting
    /// - [Self::d()]: Derivative term setting
    pub fn new(setpoint: impl Into<T>, output_limit: impl Into<T>) -> Self {
        Self {
            setpoint: setpoint.into(),
            output_limit: output_limit.into(),
            kp: T::zero(),
            ki: T::zero(),
            kd: T::zero(),
            p_limit: T::zero(),
            i_limit: T::zero(),
            d_limit: T::zero(),
            integral_term: T::zero(),
            prev_measurement: None,
        }
    }

    /// Sets the [Self::p] term for this controller.
    pub fn p(&mut self, gain: impl Into<T>, limit: impl Into<T>) -> &mut Self {
        self.kp = gain.into();
        self.p_limit = limit.into();
        self
    }

    /// Sets the [Self::i] term for this controller.
    pub fn i(&mut self, gain: impl Into<T>, limit: impl Into<T>) -> &mut Self {
        self.ki = gain.into();
        self.i_limit = limit.into();
        self
    }

    /// Sets the [Self::d] term for this controller.
    pub fn d(&mut self, gain: impl Into<T>, limit: impl Into<T>) -> &mut Self {
        self.kd = gain.into();
        self.d_limit = limit.into();
        self
    }

    /// Sets the [Pid::setpoint] to target for this controller.
    pub fn setpoint(&mut self, setpoint: impl Into<T>) -> &mut Self {
        self.setpoint = setpoint.into();
        self
    }

    /// Given a new measurement, calculates the next [control output](ControlOutput).
    ///
    /// # Panics
    ///
    /// - If a setpoint has not been set via `update_setpoint()`.
    pub fn next_control_output(&mut self, measurement: T) -> ControlOutput<T> {
        // Calculate the error between the ideal setpoint and the current
        // measurement to compare against
        let error = self.setpoint - measurement;

        // Calculate the proportional term and limit to it's individual limit
        let p_unbounded = error * self.kp;
        let p = apply_limit(self.p_limit, p_unbounded);

        // Mitigate output jumps when ki(t) != ki(t-1).
        // While it's standard to use an error_integral that's a running sum of
        // just the error (no ki), because we support ki changing dynamically,
        // we store the entire term so that we don't need to remember previous
        // ki values.
        self.integral_term = self.integral_term + error * self.ki;

        // Mitigate integral windup: Don't want to keep building up error
        // beyond what i_limit will allow.
        self.integral_term = apply_limit(self.i_limit, self.integral_term);

        // Mitigate derivative kick: Use the derivative of the measurement
        // rather than the derivative of the error.
        let d_unbounded = -match self.prev_measurement.as_ref() {
            Some(prev_measurement) => measurement - *prev_measurement,
            None => T::zero(),
        } * self.kd;
        self.prev_measurement = Some(measurement);
        let d = apply_limit(self.d_limit, d_unbounded);

        // Calculate the final output by adding together the PID terms, then
        // apply the final defined output limit
        let output = p + self.integral_term + d;
        let output = apply_limit(self.output_limit, output);

        // Return the individual term's contributions and the final output
        ControlOutput {
            p,
            i: self.integral_term,
            d,
            output: output,
        }
    }

    /// Resets the integral term back to zero, this may drastically change the
    /// control output.
    pub fn reset_integral_term(&mut self) {
        self.integral_term = T::zero();
    }
}

/// Saturating the input `value` according the absolute `limit` (`-limit <= output <= limit`).
fn apply_limit<T: FloatCore>(limit: T, value: T) -> T {
    limit.min(value.abs()) * value.signum()
}

#[cfg(test)]
mod tests {
    use super::Pid;
    use crate::ControlOutput;

    /// Proportional-only controller operation and limits
    #[test]
    fn proportional() {
        let mut pid = Pid::new(10.0, 100.0);
        pid.p(2.0, 100.0).i(0.0, 100.0).d(0.0, 100.0);
        assert_eq!(pid.setpoint, 10.0);

        // Test simple proportional
        assert_eq!(pid.next_control_output(0.0).output, 20.0);

        // Test proportional limit
        pid.p_limit = 10.0;
        assert_eq!(pid.next_control_output(0.0).output, 10.0);
    }

    /// Derivative-only controller operation and limits
    #[test]
    fn derivative() {
        let mut pid = Pid::new(10.0, 100.0);
        pid.p(0.0, 100.0).i(0.0, 100.0).d(2.0, 100.0);

        // Test that there's no derivative since it's the first measurement
        assert_eq!(pid.next_control_output(0.0).output, 0.0);

        // Test that there's now a derivative
        assert_eq!(pid.next_control_output(5.0).output, -10.0);

        // Test derivative limit
        pid.d_limit = 5.0;
        assert_eq!(pid.next_control_output(10.0).output, -5.0);
    }

    /// Integral-only controller operation and limits
    #[test]
    fn integral() {
        let mut pid = Pid::new(10.0, 100.0);
        pid.p(0.0, 100.0).i(2.0, 100.0).d(0.0, 100.0);

        // Test basic integration
        assert_eq!(pid.next_control_output(0.0).output, 20.0);
        assert_eq!(pid.next_control_output(0.0).output, 40.0);
        assert_eq!(pid.next_control_output(5.0).output, 50.0);

        // Test limit
        pid.i_limit = 50.0;
        assert_eq!(pid.next_control_output(5.0).output, 50.0);
        // Test that limit doesn't impede reversal of error integral
        assert_eq!(pid.next_control_output(15.0).output, 40.0);

        // Test that error integral accumulates negative values
        let mut pid2 = Pid::new(-10.0, 100.0);
        pid2.p(0.0, 100.0).i(2.0, 100.0).d(0.0, 100.0);
        assert_eq!(pid2.next_control_output(0.0).output, -20.0);
        assert_eq!(pid2.next_control_output(0.0).output, -40.0);

        pid2.i_limit = 50.0;
        assert_eq!(pid2.next_control_output(-5.0).output, -50.0);
        // Test that limit doesn't impede reversal of error integral
        assert_eq!(pid2.next_control_output(-15.0).output, -40.0);
    }

    /// Checks that a full PID controller's limits work properly through multiple output iterations
    #[test]
    fn output_limit() {
        let mut pid = Pid::new(10.0, 1.0);
        pid.p(1.0, 100.0).i(0.0, 100.0).d(0.0, 100.0);

        let out = pid.next_control_output(0.0);
        assert_eq!(out.p, 10.0); // 1.0 * 10.0
        assert_eq!(out.output, 1.0);

        let out = pid.next_control_output(20.0);
        assert_eq!(out.p, -10.0); // 1.0 * (10.0 - 20.0)
        assert_eq!(out.output, -1.0);
    }

    /// Combined PID operation
    #[test]
    fn pid() {
        let mut pid = Pid::new(10.0, 100.0);
        pid.p(1.0, 100.0).i(0.1, 100.0).d(1.0, 100.0);

        let out = pid.next_control_output(0.0);
        assert_eq!(out.p, 10.0); // 1.0 * 10.0
        assert_eq!(out.i, 1.0); // 0.1 * 10.0
        assert_eq!(out.d, 0.0); // -(1.0 * 0.0)
        assert_eq!(out.output, 11.0);

        let out = pid.next_control_output(5.0);
        assert_eq!(out.p, 5.0); // 1.0 * 5.0
        assert_eq!(out.i, 1.5); // 0.1 * (10.0 + 5.0)
        assert_eq!(out.d, -5.0); // -(1.0 * 5.0)
        assert_eq!(out.output, 1.5);

        let out = pid.next_control_output(11.0);
        assert_eq!(out.p, -1.0); // 1.0 * -1.0
        assert_eq!(out.i, 1.4); // 0.1 * (10.0 + 5.0 - 1)
        assert_eq!(out.d, -6.0); // -(1.0 * 6.0)
        assert_eq!(out.output, -5.6);

        let out = pid.next_control_output(10.0);
        assert_eq!(out.p, 0.0); // 1.0 * 0.0
        assert_eq!(out.i, 1.4); // 0.1 * (10.0 + 5.0 - 1.0 + 0.0)
        assert_eq!(out.d, 1.0); // -(1.0 * -1.0)
        assert_eq!(out.output, 2.4);
    }

    /// Full PID operation with mixed f32/f64 checking to make sure they're equal
    #[test]
    fn f32_and_f64() {
        let mut pid32 = Pid::new(10.0f32, 100.0);
        pid32.p(0.0, 100.0).i(0.0, 100.0).d(0.0, 100.0);

        let mut pid64 = Pid::new(10.0, 100.0f64);
        pid64.p(0.0, 100.0).i(0.0, 100.0).d(0.0, 100.0);

        assert_eq!(
            pid32.next_control_output(0.0).output,
            pid64.next_control_output(0.0).output as f32
        );
        assert_eq!(
            pid32.next_control_output(0.0).output as f64,
            pid64.next_control_output(0.0).output
        );
    }

    /// See if the controller can properly target to the setpoint after 2 output iterations
    #[test]
    fn setpoint() {
        let mut pid = Pid::new(10.0, 100.0);
        pid.p(1.0, 100.0).i(0.1, 100.0).d(1.0, 100.0);

        let out = pid.next_control_output(0.0);
        assert_eq!(out.p, 10.0); // 1.0 * 10.0
        assert_eq!(out.i, 1.0); // 0.1 * 10.0
        assert_eq!(out.d, 0.0); // -(1.0 * 0.0)
        assert_eq!(out.output, 11.0);

        pid.setpoint(0.0);

        assert_eq!(
            pid.next_control_output(0.0),
            ControlOutput {
                p: 0.0,
                i: 1.0,
                d: -0.0,
                output: 1.0
            }
        );
    }
}
