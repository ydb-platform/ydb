// Take a look at the license at the top of the repository in the LICENSE file.

use std::{cell::Cell, fmt, iter::FusedIterator, marker::PhantomData, rc::Rc};

use glib::SignalHandlerId;

use crate::{ListModel, prelude::*};

pub trait ListModelExtManual: IsA<ListModel> + Sized {
    // rustdoc-stripper-ignore-next
    /// Get an immutable snapshot of the container inside the `ListModel`.
    /// Any modification done to the returned container `Vec` will not be
    /// reflected on the `ListModel`.
    fn snapshot(&self) -> Vec<glib::Object> {
        let mut res = Vec::with_capacity(self.n_items() as usize);
        for i in 0..self.n_items() {
            res.push(self.item(i).unwrap())
        }
        res
    }

    // rustdoc-stripper-ignore-next
    /// If `T::static_type().is_a(self.item_type())` then it returns an iterator over the `ListModel` elements,
    /// else the types are not compatible and it panics.
    ///
    /// # Panics
    ///
    /// Panics if `T::static_type().is_a(self.item_type())` is not true.
    fn iter<LT: IsA<glib::Object>>(&self) -> ListModelIter<'_, LT> {
        assert!(self.item_type().is_a(LT::static_type()));

        let len = self.n_items();
        let changed = Rc::new(Cell::new(false));

        let changed_clone = changed.clone();
        let signal_id = Some(self.connect_items_changed(move |_, pos, _, _| {
            if pos < len {
                changed_clone.set(true);
            }
        }));

        ListModelIter {
            ty: Default::default(),
            i: 0,
            reverse_pos: len,
            model: self.upcast_ref(),
            changed,
            signal_id,
        }
    }
}

impl<T: IsA<ListModel>> ListModelExtManual for T {}

#[derive(Debug, PartialEq, Eq)]
pub struct ListModelMutatedDuringIter;

impl std::error::Error for ListModelMutatedDuringIter {}

impl fmt::Display for ListModelMutatedDuringIter {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("the list model was mutated during iteration")
    }
}

// rustdoc-stripper-ignore-next
/// Iterator of `ListModel`'s items.
/// This iterator will always give `n = initial_model.n_items()` items, even if the `ListModel`
/// is mutated during iteration.
/// If the internal `ListModel` gets mutated, the iterator
/// will return `Some(Err(...))` for the remaining items.
/// Mutations to the `ListModel` in position >= `initial_model.n_items()` are allowed.
pub struct ListModelIter<'a, T: IsA<glib::Object>> {
    ty: PhantomData<T>,
    i: u32,
    // it's > i when valid
    reverse_pos: u32,
    model: &'a ListModel,
    changed: Rc<Cell<bool>>,
    signal_id: Option<SignalHandlerId>,
}
impl<T: IsA<glib::Object>> Iterator for ListModelIter<'_, T> {
    type Item = Result<T, ListModelMutatedDuringIter>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.reverse_pos {
            return None;
        }
        let res = match self.changed.get() {
            true => Err(ListModelMutatedDuringIter),
            false => Ok(self.model.item(self.i).unwrap().downcast::<T>().unwrap()),
        };
        self.i += 1;
        Some(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n: usize = (self.reverse_pos - self.i) as _;
        (n, Some(n))
    }

    fn count(self) -> usize {
        (self.reverse_pos - self.i) as usize
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let (end, overflow) = (self.i as usize).overflowing_add(n);
        if end >= self.reverse_pos as usize || overflow {
            self.i = self.reverse_pos;
            None
        } else {
            let end = end as u32;
            self.i = end + 1;

            let res = match self.changed.get() {
                true => Err(ListModelMutatedDuringIter),
                false => Ok(self.model.item(end).unwrap().downcast::<T>().unwrap()),
            };
            Some(res)
        }
    }

    fn last(self) -> Option<Self::Item> {
        if self.i == self.reverse_pos {
            None
        } else {
            let res = match self.changed.get() {
                true => Err(ListModelMutatedDuringIter),
                false => Ok(self
                    .model
                    .item(self.reverse_pos - 1)
                    .unwrap()
                    .downcast::<T>()
                    .unwrap()),
            };
            Some(res)
        }
    }
}

impl<T: IsA<glib::Object>> FusedIterator for ListModelIter<'_, T> {}

impl<T: IsA<glib::Object>> ExactSizeIterator for ListModelIter<'_, T> {}

impl<T: IsA<glib::Object>> DoubleEndedIterator for ListModelIter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.reverse_pos == self.i {
            return None;
        }
        self.reverse_pos -= 1;
        let res = match self.changed.get() {
            true => Err(ListModelMutatedDuringIter),
            false => Ok(self
                .model
                .item(self.reverse_pos)
                .unwrap()
                .downcast::<T>()
                .unwrap()),
        };
        Some(res)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        let (end, overflow) = (self.reverse_pos as usize).overflowing_sub(n);
        if end <= self.i as usize || overflow {
            self.i = self.reverse_pos;
            None
        } else {
            let end = end as u32;
            self.reverse_pos = end - 1;

            let res = match self.changed.get() {
                true => Err(ListModelMutatedDuringIter),
                false => Ok(self.model.item(end - 1).unwrap().downcast::<T>().unwrap()),
            };
            Some(res)
        }
    }
}
impl<T: IsA<glib::Object>> Drop for ListModelIter<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.model.disconnect(self.signal_id.take().unwrap());
    }
}

impl<'a> std::iter::IntoIterator for &'a ListModel {
    type Item = Result<glib::Object, ListModelMutatedDuringIter>;
    type IntoIter = ListModelIter<'a, glib::Object>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[test]
fn list_model_iter_ok() {
    let list = crate::ListStore::new::<crate::Menu>();
    let m1 = crate::Menu::new();
    let m2 = crate::Menu::new();
    let m3 = crate::Menu::new();
    let m4 = crate::Menu::new();

    list.append(&m1);
    list.append(&m2);
    list.append(&m3);

    let mut iter = list.iter::<crate::Menu>();

    assert_eq!(iter.len(), 3);
    assert_eq!(iter.next(), Some(Ok(m1)));
    // Appending items at the end of the `ListModel` can't affect the items
    // we are iterating over.
    list.append(&m4);
    assert_eq!(iter.next_back(), Some(Ok(m3)));
    assert_eq!(iter.len(), 1);
    assert_eq!(iter.next_back(), Some(Ok(m2)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
}

#[test]
fn list_model_iter_err() {
    let list = crate::ListStore::new::<crate::Menu>();
    let m1 = crate::Menu::new();
    let m2 = crate::Menu::new();
    let m3 = crate::Menu::new();
    let m4 = crate::Menu::new();

    list.append(&m1);
    list.append(&m2);
    list.append(&m3);
    list.append(&m4);

    let mut iter = list.iter::<crate::Menu>();

    assert_eq!(iter.next_back(), Some(Ok(m4)));

    // These two don't affect the iter
    list.append(&m2);
    list.append(&m2);

    assert_eq!(iter.next(), Some(Ok(m1)));

    // Does affect the iter
    list.remove(2);
    // Doesn't affect the iter, but the iter should stay tainted.
    list.remove(4);
    assert_eq!(iter.next(), Some(Err(ListModelMutatedDuringIter)));
    assert_eq!(iter.next(), Some(Err(ListModelMutatedDuringIter)));
    // Returned n items
    assert_eq!(iter.next(), None);
}

#[test]
fn list_model_iter_nth() {
    let list = crate::ListStore::new::<crate::Menu>();
    let m1 = crate::Menu::new();
    let m2 = crate::Menu::new();
    let m3 = crate::Menu::new();
    let m4 = crate::Menu::new();
    let m5 = crate::Menu::new();
    let m6 = crate::Menu::new();

    list.append(&m1);
    list.append(&m2);
    list.append(&m3);
    list.append(&m4);
    list.append(&m5);
    list.append(&m6);

    let mut iter = list.iter::<crate::Menu>();

    assert_eq!(iter.len(), 6);
    assert_eq!(iter.nth(1), Some(Ok(m2)));
    assert_eq!(iter.len(), 4);
    assert_eq!(iter.next(), Some(Ok(m3)));
    assert_eq!(iter.nth_back(2), Some(Ok(m4)));
    assert_eq!(iter.len(), 0);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
}

#[test]
fn list_model_iter_last() {
    let list = crate::ListStore::new::<crate::Menu>();
    let m1 = crate::Menu::new();
    let m2 = crate::Menu::new();
    let m3 = crate::Menu::new();

    list.append(&m1);
    list.append(&m2);
    list.append(&m3);

    let iter = list.iter::<crate::Menu>();

    assert_eq!(iter.len(), 3);
    assert_eq!(iter.last(), Some(Ok(m3)));
}

#[test]
fn list_model_iter_count() {
    let list = crate::ListStore::new::<crate::Menu>();
    let m1 = crate::Menu::new();
    let m2 = crate::Menu::new();
    let m3 = crate::Menu::new();

    list.append(&m1);
    list.append(&m2);
    list.append(&m3);

    let iter = list.iter::<crate::Menu>();

    assert_eq!(iter.len(), 3);
    assert_eq!(iter.count(), 3);
}
