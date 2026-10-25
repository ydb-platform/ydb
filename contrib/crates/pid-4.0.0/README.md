# PID Controller for Rust [![Latest Version]][crates.io] [![Documentation]][docs.rs] [![Build Status]][travis] 

[Build Status]: https://api.travis-ci.org/braincore/pid-rs.svg?branch=master
[travis]: https://travis-ci.org/braincore/pid-rs
[Latest Version]: https://img.shields.io/crates/v/pid.svg
[crates.io]: https://crates.io/crates/pid
[Documentation]: https://docs.rs/pid/badge.svg
[docs.rs]: https://docs.rs/pid

A proportional-integral-derivative (PID) controller.

## Features

* Visibility into individual contribution of P, I, and D terms which often
  need to be logged for later analysis and parameter tuning.
* Output limits on a per term basis.
* Three-term control output limit.
* Mitigation of integral windup using integral term limit.
* Mitigation of derivative kick by using the derivative of the measurement
  rather than the derivative of the error.
* On-the-fly changes to `setpoint`/`kp`/`ki`/`kd`.
  * Mitigation of output jumps when changing `ki` by storing the integration of
    `e(t) * ki(t)` rather than only `e(t)`.
* Generic float type parameter to support `f32` or `f64`.
* Support for `no_std` environments, such as embedded systems.
* Optional support for [Serde](https://crates.io/crates/serde). Enable the
  `serde` Cargo feature, if you need `Pid` to implement
  `Serialize`/`Deserialize`.

## Example

```rust
use pid::Pid;

// Create a new proportional-only PID controller with a setpoint of 15
let mut pid = Pid::new(15.0, 100.0);
pid.p(10.0, 100.0);

// Input a measurement with an error of 5.0 from our setpoint
let output = pid.next_control_output(10.0);

// Show that the error is correct by multiplying by our kp
assert_eq!(output.output, 50.0); // <--
assert_eq!(output.p, 50.0);

// It won't change on repeat; the controller is proportional-only
let output = pid.next_control_output(10.0);
assert_eq!(output.output, 50.0); // <--
assert_eq!(output.p, 50.0);

// Add a new integral term to the controller and input again
pid.i(1.0, 100.0);
let output = pid.next_control_output(10.0);

// Now that the integral makes the controller stateful, it will change
assert_eq!(output.output, 55.0); // <--
assert_eq!(output.p, 50.0);
assert_eq!(output.i, 5.0);

// Add our final derivative term and match our setpoint target
pid.d(2.0, 100.0);
let output = pid.next_control_output(15.0);

// The output will now say to go down due to the derivative
assert_eq!(output.output, -5.0); // <--
assert_eq!(output.p, 0.0);
assert_eq!(output.i, 5.0);
assert_eq!(output.d, -10.0);
```

## Assumptions

* Measurements occur at equal spacing. (`t(i) = t(i-1) + C`)
* Output limits per term are symmetric around 0 (`-limit <= term <= limit`).

## Formulation

There are several different formulations of PID controllers. This library
uses the independent form:

![PID independent form](
https://latex.codecogs.com/gif.latex?C(t)&space;=&space;&space;K_p&space;\cdot&space;e(t)&space;&plus;&space;K_i&space;\cdot&space;\int{e(t)dt}&space;-&space;K_d&space;\cdot&space;\frac{dP(t)}{dt})

where:
- C(t) = control output, the output to the actuator.
- P(t) = process variable, the measured value.
- e(t) = error = S(t) - P(t)
- S(t) = set point, the desired target for the process variable.

`kp`/`ki`/`kd` can be changed during operation and can therefore be a function
of time.

If you're interested in the dependent form, add your own logic that computes
`kp`/`ki`/`kd` using dead time, time constant, `kc`, or whatever else.

## Todo

- [ ] Helper for (auto-)tuning by detecting frequency & amplitude of
      oscillations.

## License

Licensed under either at your discretion:

- Apache License, Version 2.0 (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)
