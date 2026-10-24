mod api;
pub use api::*;

mod constant;
pub use constant::ConstantBackoff;
pub use constant::ConstantBuilder;

mod fibonacci;
pub use fibonacci::FibonacciBackoff;
pub use fibonacci::FibonacciBuilder;

mod exponential;
pub use exponential::ExponentialBackoff;
pub use exponential::ExponentialBuilder;

// Random seed value for no_std (the value is "backon" in hex)
#[cfg(not(feature = "std"))]
const RANDOM_SEED: u64 = 0x6261636b6f6e;
