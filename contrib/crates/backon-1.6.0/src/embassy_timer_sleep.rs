use core::time::Duration;

use crate::BlockingSleeper;
use crate::Sleeper;

/// A no_std async sleeper based on the embassy framework (https://embassy.dev)
#[derive(Clone, Copy, Debug, Default)]
pub struct EmbassySleeper;

impl Sleeper for EmbassySleeper {
    type Sleep = embassy_time::Timer;

    fn sleep(&self, dur: Duration) -> Self::Sleep {
        embassy_time::Timer::after_millis(dur.as_millis() as u64)
    }
}

impl BlockingSleeper for EmbassySleeper {
    fn sleep(&self, dur: Duration) {
        embassy_time::block_for(embassy_time::Duration::from_millis(dur.as_millis() as u64));
    }
}
