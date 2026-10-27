use super::Rb;
use crate::{storage::Array, traits::*};

#[test]
fn producer() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (prod, mut cons) = rb.split_ref();
    let mut frozen_prod = prod.freeze();
    frozen_prod.try_push(0).unwrap();
    frozen_prod.sync();
    assert!(cons.iter().cloned().eq(0..1));

    {
        frozen_prod.try_push(1).unwrap();
        assert!(cons.iter().cloned().eq(0..1));
        assert_eq!(cons.occupied_len(), 1);
        assert_eq!(frozen_prod.occupied_len(), 2);

        assert_eq!(cons.try_pop().unwrap(), 0);
        assert!(cons.try_pop().is_none());
        assert_eq!(cons.occupied_len(), 0);
        assert_eq!(frozen_prod.occupied_len(), 2);

        frozen_prod.sync();
        assert!(cons.iter().cloned().eq(1..2));
        assert_eq!(cons.occupied_len(), 1);
        assert_eq!(frozen_prod.occupied_len(), 1);

        frozen_prod.try_push(2).unwrap();
        assert!(cons.iter().cloned().eq(1..2));
        assert_eq!(cons.occupied_len(), 1);
        assert_eq!(frozen_prod.occupied_len(), 2);
    }
    frozen_prod.sync();

    assert!(cons.iter().cloned().eq(1..3));
    assert_eq!(cons.occupied_len(), 2);
    assert_eq!(frozen_prod.occupied_len(), 2);
}

#[test]
fn discard() {
    let mut rb = Rb::<Array<i32, 10>>::default();
    let (prod, cons) = rb.split_ref();
    let mut frozen_prod = prod.freeze();
    frozen_prod.try_push(0).unwrap();
    frozen_prod.sync();
    assert!(cons.iter().cloned().eq(0..1));

    {
        frozen_prod.try_push(1).unwrap();
        assert_eq!(cons.occupied_len(), 1);
        assert_eq!(frozen_prod.occupied_len(), 2);

        frozen_prod.sync();
        assert!(cons.iter().cloned().eq(0..2));
        assert_eq!(cons.occupied_len(), 2);
        assert_eq!(frozen_prod.occupied_len(), 2);

        frozen_prod.try_push(3).unwrap();
        assert_eq!(cons.occupied_len(), 2);
        assert_eq!(frozen_prod.occupied_len(), 3);

        frozen_prod.discard();
        assert_eq!(cons.occupied_len(), 2);
        assert_eq!(frozen_prod.occupied_len(), 2);

        frozen_prod.try_push(2).unwrap();
        assert_eq!(cons.occupied_len(), 2);
        assert_eq!(frozen_prod.occupied_len(), 3);
    }
    frozen_prod.sync();

    assert!(cons.iter().cloned().eq(0..3));
    assert_eq!(cons.occupied_len(), 3);
    assert_eq!(frozen_prod.occupied_len(), 3);
}

#[test]
fn consumer() {
    let mut rb = Rb::<Array<i32, 10>>::default();
    let (mut prod, cons) = rb.split_ref();
    let mut frozen_cons = cons.freeze();
    prod.try_push(0).unwrap();
    prod.try_push(1).unwrap();
    frozen_cons.sync();
    assert!(frozen_cons.iter().cloned().eq(0..2));

    {
        assert_eq!(frozen_cons.try_pop().unwrap(), 0);
        assert!(frozen_cons.iter().cloned().eq(1..2));
        assert_eq!(frozen_cons.occupied_len(), 1);
        assert_eq!(prod.occupied_len(), 2);

        prod.try_push(2).unwrap();
        assert!(frozen_cons.iter().cloned().eq(1..2));
        assert_eq!(frozen_cons.occupied_len(), 1);
        assert_eq!(prod.occupied_len(), 3);

        frozen_cons.sync();
        assert!(frozen_cons.iter().cloned().eq(1..3));
        assert_eq!(frozen_cons.occupied_len(), 2);
        assert_eq!(prod.occupied_len(), 2);

        assert_eq!(frozen_cons.try_pop().unwrap(), 1);
        assert!(frozen_cons.iter().cloned().eq(2..3));
        assert_eq!(frozen_cons.occupied_len(), 1);
        assert_eq!(prod.occupied_len(), 2);
    }
    frozen_cons.sync();

    assert!(frozen_cons.iter().cloned().eq(2..3));
    assert_eq!(frozen_cons.occupied_len(), 1);
    assert_eq!(prod.occupied_len(), 1);
}
