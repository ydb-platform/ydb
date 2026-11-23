use super::Rb;
use crate::{storage::Array, traits::*};
use alloc::collections::BTreeSet;
use core::cell::RefCell;

#[derive(Debug)]
struct Dropper<'a> {
    id: i32,
    set: &'a RefCell<BTreeSet<i32>>,
}

impl<'a> Dropper<'a> {
    fn new(set: &'a RefCell<BTreeSet<i32>>, id: i32) -> Self {
        if !set.borrow_mut().insert(id) {
            panic!("value {} already exists", id);
        }
        Self { set, id }
    }
}

impl Drop for Dropper<'_> {
    fn drop(&mut self) {
        if !self.set.borrow_mut().remove(&self.id) {
            panic!("value {} already removed", self.id);
        }
    }
}

#[test]
fn single() {
    let set = RefCell::new(BTreeSet::new());

    let mut rb = Rb::<Array<Dropper, 3>>::default();

    assert_eq!(set.borrow().len(), 0);

    {
        let (mut prod, mut cons) = rb.split_ref();

        prod.try_push(Dropper::new(&set, 1)).unwrap();
        assert_eq!(set.borrow().len(), 1);
        prod.try_push(Dropper::new(&set, 2)).unwrap();
        assert_eq!(set.borrow().len(), 2);
        prod.try_push(Dropper::new(&set, 3)).unwrap();
        assert_eq!(set.borrow().len(), 3);

        cons.try_pop().unwrap();
        assert_eq!(set.borrow().len(), 2);
        cons.try_pop().unwrap();
        assert_eq!(set.borrow().len(), 1);

        prod.try_push(Dropper::new(&set, 4)).unwrap();
        assert_eq!(set.borrow().len(), 2);
    }

    drop(rb);
    assert_eq!(set.borrow().len(), 0);
}

// TODO: Use transactions.
#[test]
fn transaction() {
    let set = RefCell::new(BTreeSet::new());

    let mut rb = Rb::<Array<Dropper, 5>>::default();

    assert_eq!(set.borrow().len(), 0);
    {
        let (mut prod, mut cons) = rb.split_ref();
        let mut id = 0;
        let mut cnt = 0;
        fn assert_cnt(cnt: usize, n: usize, cons: &impl Consumer, set: &RefCell<BTreeSet<i32>>) {
            assert_eq!(cnt, n);
            assert_eq!(cnt, cons.occupied_len());
            assert_eq!(cnt, set.borrow().len());
        }

        for _ in 0..4 {
            id += 1;
            cnt += 1;
            prod.try_push(Dropper::new(&set, id)).unwrap();
        }
        assert_cnt(cnt, 4, &cons, &set);

        cnt -= cons.skip(2);
        assert_cnt(cnt, 2, &cons, &set);

        while !prod.is_full() {
            id += 1;
            cnt += 1;
            prod.try_push(Dropper::new(&set, id)).unwrap();
        }
        assert_cnt(cnt, 5, &cons, &set);

        cnt -= cons.clear();
        assert_cnt(cnt, 0, &cons, &set);

        while !prod.is_full() {
            id += 1;
            cnt += 1;
            prod.try_push(Dropper::new(&set, id)).unwrap();
        }
        assert_cnt(cnt, 5, &cons, &set);
    }

    drop(rb);
    assert_eq!(set.borrow().len(), 0);
}
