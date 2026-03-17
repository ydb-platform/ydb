//! Functionality related to patching the current time
use crate::{classes::instant::Instant, common::scalar::*, py::*, pymodule::State};
use pyo3_ffi::*;
use std::time::SystemTime;

pub(crate) fn _patch_time_frozen(state: &mut State, arg: PyObj) -> PyReturn {
    _patch_time(state, arg, true)
}

pub(crate) fn _patch_time_keep_ticking(state: &mut State, arg: PyObj) -> PyReturn {
    _patch_time(state, arg, false)
}

pub(crate) fn _patch_time(state: &mut State, arg: PyObj, freeze: bool) -> PyReturn {
    let Some(inst) = arg.extract(state.instant_type) else {
        return raise_type_err("Expected an Instant")?;
    };

    let pos_epoch = u64::try_from(inst.epoch.get())
        .ok()
        .ok_or_type_err("Can only set time after 1970")?;

    let patch = &mut state.time_patch;

    patch.set_state(if freeze {
        PatchState::Frozen(inst)
    } else {
        PatchState::KeepTicking {
            pin: std::time::Duration::new(pos_epoch, inst.subsec.get() as _),
            at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .ok_or_type_err("System time before 1970")?,
        }
    });
    Ok(none())
}

pub(crate) fn _unpatch_time(state: &mut State) -> PyReturn {
    let patch = &mut state.time_patch;
    patch.set_state(PatchState::Unset);
    Ok(none())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Patch {
    state: PatchState,
    time_machine_installed: bool,
}

impl Patch {
    pub(crate) fn new() -> PyResult<Self> {
        Ok(Self {
            state: PatchState::Unset,
            time_machine_installed: time_machine_installed()?,
        })
    }

    pub(crate) fn set_state(&mut self, state: PatchState) {
        self.state = state;
    }
}

fn time_machine_installed() -> PyResult<bool> {
    // Important: we don't import `time_machine` here,
    // because that would be slower. We only need to check its existence.
    Ok(!import(c"importlib.util")?
        .getattr(c"find_spec")?
        .call1("time_machine".to_py()?.borrow())?
        .is_none())
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PatchState {
    Unset,
    Frozen(Instant),
    KeepTicking {
        pin: std::time::Duration,
        at: std::time::Duration,
    },
}

impl Instant {
    pub(crate) fn from_duration_since_epoch(d: std::time::Duration) -> Option<Self> {
        Some(Instant {
            epoch: EpochSecs::new(d.as_secs() as _)?,
            // Safe: subsec on Duration is always in range
            subsec: SubSecNanos::new_unchecked(d.subsec_nanos() as _),
        })
    }

    fn from_nanos_i64(ns: i64) -> Option<Self> {
        Some(Instant {
            epoch: EpochSecs::new(ns / 1_000_000_000)?,
            subsec: SubSecNanos::from_remainder(ns),
        })
    }
}

impl State {
    pub(crate) fn time_ns(&self) -> PyResult<Instant> {
        let Patch {
            state: status,
            time_machine_installed,
        } = self.time_patch;
        match status {
            PatchState::Unset => {
                if time_machine_installed {
                    self.time_ns_py()
                } else {
                    self.time_ns_rust()
                }
            }
            PatchState::Frozen(e) => Ok(e),
            PatchState::KeepTicking { pin, at } => {
                let dur = pin
                    + SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .ok()
                        .ok_or_raise(unsafe { PyExc_OSError }, "System time out of range")?
                    - at;
                Instant::from_duration_since_epoch(dur)
                    .ok_or_raise(unsafe { PyExc_OSError }, "System time out of range")
            }
        }
    }

    fn time_ns_py(&self) -> PyResult<Instant> {
        let ts = self.time_ns.call0()?;
        let ns = ts
            .cast::<PyInt>()
            .ok_or_raise(
                unsafe { PyExc_RuntimeError },
                "time_ns() returned a non-integer",
            )?
            // FUTURE: this will break in the year 2262. Fix it before then.
            .to_i64()?;
        Instant::from_nanos_i64(ns)
            .ok_or_raise(unsafe { PyExc_OSError }, "System time out of range")
    }

    fn time_ns_rust(&self) -> PyResult<Instant> {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(Instant::from_duration_since_epoch)
            .ok_or_raise(unsafe { PyExc_OSError }, "System time out of range")
    }
}
