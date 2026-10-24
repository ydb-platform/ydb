use super::Rb;
use crate::{storage::Array, traits::*, CachingCons, CachingProd, Obs};

#[test]
fn split_and_drop() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (prod, cons) = rb.split_ref();
    let obs = prod.observe();

    assert!(obs.write_is_held() && obs.read_is_held());

    drop(cons);
    assert!(obs.write_is_held() && !obs.read_is_held());

    drop(prod);
    assert!(!obs.write_is_held() && !obs.read_is_held());
}

#[test]
fn manually_hold_and_drop() {
    let rb = Rb::<Array<i32, 2>>::default();
    let obs = Obs::new(&rb);
    assert!(!obs.write_is_held() && !obs.read_is_held());

    let cons = CachingCons::new(&rb);
    assert!(!obs.write_is_held() && obs.read_is_held());

    let prod = CachingProd::new(&rb);
    assert!(obs.write_is_held() && obs.read_is_held());

    drop(cons);
    assert!(obs.write_is_held() && !obs.read_is_held());

    drop(prod);
    assert!(!obs.write_is_held() && !obs.read_is_held());
}

#[test]
#[should_panic]
fn hold_conflict() {
    let rb = Rb::<Array<i32, 2>>::default();
    let _prod = CachingProd::new(&rb);
    CachingProd::new(&rb);
}
