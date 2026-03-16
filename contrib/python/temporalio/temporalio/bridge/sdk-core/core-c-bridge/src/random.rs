use crate::ByteArrayRef;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64Mcg;

pub struct Random {
    // We choose this algorithm because it is 64-bit fast and a well supported,
    // deterministic, portable standard. We do not need the security of a CSPRNG
    // like ChaCha20.
    rand: Pcg64Mcg,
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_random_new(seed: u64) -> *mut Random {
    Box::into_raw(Box::new(Random {
        rand: Pcg64Mcg::seed_from_u64(seed),
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_random_free(random: *mut Random) {
    unsafe {
        let _ = Box::from_raw(random);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_random_int32_range(
    random: *mut Random,
    min: i32,
    max: i32,
    max_inclusive: bool,
) -> i32 {
    let random = unsafe { &mut *random };
    if max_inclusive {
        random.rand.gen_range(min..=max)
    } else {
        random.rand.gen_range(min..max)
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_random_double_range(
    random: *mut Random,
    min: f64,
    max: f64,
    max_inclusive: bool,
) -> f64 {
    let random = unsafe { &mut *random };
    if max_inclusive {
        random.rand.gen_range(min..=max)
    } else {
        random.rand.gen_range(min..max)
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_random_fill_bytes(random: *mut Random, mut bytes: ByteArrayRef) {
    let random = unsafe { &mut *random };
    // Confirmed with the algorithm this won't fail, so try_fill not needed
    random.rand.fill(bytes.to_slice_mut());
}
