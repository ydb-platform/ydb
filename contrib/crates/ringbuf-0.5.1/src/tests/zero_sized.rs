use crate::{
    producer::Producer,
    traits::{Consumer, Observer, Split},
    HeapRb,
};

#[test]
fn basic() {
    let (mut prod, mut cons) = HeapRb::<()>::new(2).split();
    let obs = prod.observe();
    assert_eq!(obs.capacity().get(), 2);

    assert_eq!(obs.occupied_len(), 0);
    assert_eq!(obs.vacant_len(), 2);
    assert!(obs.is_empty());

    assert!(cons.try_pop().is_none());

    prod.try_push(()).unwrap();
    assert_eq!(obs.occupied_len(), 1);
    assert_eq!(obs.vacant_len(), 1);

    prod.try_push(()).unwrap();
    assert_eq!(obs.occupied_len(), 2);
    assert_eq!(obs.vacant_len(), 0);
    assert!(obs.is_full());

    assert!(prod.try_push(()).is_err());

    cons.try_pop().unwrap();
    assert_eq!(obs.occupied_len(), 1);
    assert_eq!(obs.vacant_len(), 1);

    prod.try_push(()).unwrap();
    assert_eq!(obs.occupied_len(), 2);
    assert_eq!(obs.vacant_len(), 0);
    assert!(obs.is_full());

    cons.try_pop().unwrap();
    assert_eq!(obs.occupied_len(), 1);
    assert_eq!(obs.vacant_len(), 1);

    cons.try_pop().unwrap();
    assert_eq!(obs.occupied_len(), 0);
    assert_eq!(obs.vacant_len(), 2);
    assert!(obs.is_empty());

    assert!(cons.try_pop().is_none());
}
