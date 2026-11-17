// Take a look at the license at the top of the repository in the LICENSE file.

use std::{cell::Cell, cmp::Ordering, rc::Rc};

use glib::{Object, prelude::*, translate::*};

use crate::{ListModel, ListStore, ffi, prelude::*};

impl ListStore {
    #[doc(alias = "g_list_store_new")]
    pub fn new<T: IsA<Object>>() -> Self {
        Self::with_type(T::static_type())
    }

    #[doc(alias = "g_list_store_new")]
    pub fn with_type(type_: glib::types::Type) -> Self {
        unsafe { from_glib_full(ffi::g_list_store_new(type_.into_glib())) }
    }

    #[doc(alias = "g_list_store_insert_sorted")]
    pub fn insert_sorted<P: IsA<glib::Object>, F: FnMut(&Object, &Object) -> Ordering>(
        &self,
        item: &P,
        compare_func: F,
    ) -> u32 {
        unsafe {
            let mut func = compare_func;
            let func_obj: &mut dyn FnMut(&Object, &Object) -> Ordering = &mut func;
            let func_ptr = &func_obj as *const &mut dyn FnMut(&Object, &Object) -> Ordering
                as glib::ffi::gpointer;

            ffi::g_list_store_insert_sorted(
                self.to_glib_none().0,
                item.as_ref().to_glib_none().0,
                Some(compare_func_trampoline),
                func_ptr,
            )
        }
    }

    #[doc(alias = "g_list_store_sort")]
    pub fn sort<F: FnMut(&Object, &Object) -> Ordering>(&self, compare_func: F) {
        unsafe {
            let mut func = compare_func;
            let func_obj: &mut dyn FnMut(&Object, &Object) -> Ordering = &mut func;
            let func_ptr = &func_obj as *const &mut dyn FnMut(&Object, &Object) -> Ordering
                as glib::ffi::gpointer;

            ffi::g_list_store_sort(
                self.to_glib_none().0,
                Some(compare_func_trampoline),
                func_ptr,
            )
        }
    }

    #[doc(alias = "g_list_store_splice")]
    pub fn splice(&self, position: u32, n_removals: u32, additions: &[impl IsA<glib::Object>]) {
        let n_additions = additions.len() as u32;
        unsafe {
            let additions = additions.as_ptr() as *mut *mut glib::gobject_ffi::GObject;

            ffi::g_list_store_splice(
                self.to_glib_none().0,
                position,
                n_removals,
                additions,
                n_additions,
            );
        }
    }

    // rustdoc-stripper-ignore-next
    /// Appends all elements in a slice to the `ListStore`.
    pub fn extend_from_slice(&self, additions: &[impl IsA<glib::Object>]) {
        self.splice(self.n_items(), 0, additions)
    }

    // rustdoc-stripper-ignore-next
    /// Retains only the elements specified by the predicate.
    /// This method operates in place, visiting each element exactly once in the original order,
    /// and preserves the order of the retained elements.
    /// Because the elements are visited exactly once in the original order,
    /// external state may be used to decide which elements to keep.
    ///
    /// # Panics
    /// Panics if the predicate closure mutates the list by removing or adding items.
    pub fn retain(&self, mut f: impl FnMut(&glib::Object) -> bool) {
        let mut consec_removed = 0;
        let mut i = 0;
        const ADDITIONS: &[glib::Object] = &[]; // To satisfy the type checker

        let changed = Rc::new(Cell::new(false));
        let changed_clone = changed.clone();
        let signal_id = self.connect_items_changed(move |_list, _, _, _| changed_clone.set(true));

        let _signal_guard = {
            struct Guard<'a> {
                list_store: &'a ListStore,
                signal_id: Option<glib::SignalHandlerId>,
            }
            impl Drop for Guard<'_> {
                fn drop(&mut self) {
                    self.list_store.disconnect(self.signal_id.take().unwrap());
                }
            }
            Guard {
                list_store: self,
                signal_id: Some(signal_id),
            }
        };

        while i < self.n_items() {
            let keep = f(self.item(i).unwrap().as_ref());
            if changed.get() {
                panic!("The closure passed to ListStore::retain() must not mutate the list store");
            }
            if !keep {
                consec_removed += 1;
            } else if consec_removed > 0 {
                self.splice(i - consec_removed, consec_removed, ADDITIONS);
                changed.set(false);
                i -= consec_removed;
                consec_removed = 0;
            }
            i += 1;
        }
        if consec_removed > 0 {
            self.splice(i - consec_removed, consec_removed, ADDITIONS);
        }
    }

    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    #[doc(alias = "g_list_store_find_with_equal_func_full")]
    #[doc(alias = "g_list_store_find_with_equal_func")]
    pub fn find_with_equal_func<F: FnMut(&glib::Object) -> bool>(
        &self,
        equal_func: F,
    ) -> Option<u32> {
        unsafe extern "C" fn equal_func_trampoline(
            a: glib::ffi::gconstpointer,
            _b: glib::ffi::gconstpointer,
            func: glib::ffi::gpointer,
        ) -> glib::ffi::gboolean {
            unsafe {
                let func = func as *mut &mut dyn FnMut(&Object) -> bool;

                let a = from_glib_borrow(a as *mut glib::gobject_ffi::GObject);

                (*func)(&a).into_glib()
            }
        }

        let mut func = equal_func;
        let func_obj: &mut dyn FnMut(&Object) -> bool = &mut func;
        let func_ptr = &func_obj as *const &mut dyn FnMut(&Object) -> bool as glib::ffi::gpointer;
        let mut position = std::mem::MaybeUninit::uninit();

        // GIO prior to 2.76 requires a non-NULL item to be passed in so we're constructing a fake item here.
        // See https://gitlab.gnome.org/GNOME/glib/-/merge_requests/3284
        #[cfg(not(feature = "v2_76"))]
        let result = unsafe {
            let g_class: *mut glib::gobject_ffi::GTypeClass =
                glib::gobject_ffi::g_type_class_peek(self.item_type().into_glib()) as *mut _;

            // g_class will be `NULL` when no instance of the `item-type` has been created yet.
            // See https://github.com/gtk-rs/gtk-rs-core/issues/1767
            if g_class.is_null() {
                return None;
            }

            let item = glib::gobject_ffi::GObject {
                g_type_instance: glib::gobject_ffi::GTypeInstance { g_class },
                ref_count: 1,
                qdata: std::ptr::null_mut(),
            };

            bool::from_glib(ffi::g_list_store_find_with_equal_func_full(
                self.to_glib_none().0,
                mut_override(&item as *const _),
                Some(equal_func_trampoline),
                func_ptr,
                position.as_mut_ptr(),
            ))
            .then(|| position.assume_init())
        };

        #[cfg(feature = "v2_76")]
        let result = unsafe {
            bool::from_glib(ffi::g_list_store_find_with_equal_func_full(
                self.to_glib_none().0,
                std::ptr::null_mut(),
                Some(equal_func_trampoline),
                func_ptr,
                position.as_mut_ptr(),
            ))
            .then(|| position.assume_init())
        };

        result
    }
}

impl<P: IsA<glib::Object>> std::iter::FromIterator<P> for ListStore {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        let store = Self::new::<P>();
        for item in iter.into_iter() {
            store.append(&item)
        }
        store
    }
}

impl<'a> std::iter::IntoIterator for &'a ListStore {
    type Item = <&'a ListModel as IntoIterator>::Item;
    type IntoIter = <&'a ListModel as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.upcast_ref::<ListModel>().into_iter()
    }
}

unsafe extern "C" fn compare_func_trampoline(
    a: glib::ffi::gconstpointer,
    b: glib::ffi::gconstpointer,
    func: glib::ffi::gpointer,
) -> i32 {
    unsafe {
        let func = func as *mut &mut dyn FnMut(&Object, &Object) -> Ordering;

        let a = from_glib_borrow(a as *mut glib::gobject_ffi::GObject);
        let b = from_glib_borrow(b as *mut glib::gobject_ffi::GObject);

        (*func)(&a, &b).into_glib()
    }
}

impl<A: AsRef<glib::Object>> std::iter::Extend<A> for ListStore {
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        let additions = iter
            .into_iter()
            .map(|o| o.as_ref().clone())
            .collect::<Vec<_>>();
        self.splice(self.n_items(), 0, &additions)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ListStore, prelude::*};

    #[test]
    fn splice() {
        let item0 = ListStore::new::<ListStore>();
        let item1 = ListStore::new::<ListStore>();
        let list = ListStore::new::<ListStore>();
        list.splice(0, 0, &[item0.clone(), item1.clone()]);
        assert_eq!(list.item(0), Some(item0.upcast()));
        assert_eq!(list.item(1), Some(item1.upcast()));
    }

    #[test]
    fn extend() {
        let item0 = ListStore::new::<ListStore>();
        let item1 = ListStore::new::<ListStore>();
        let mut list = ListStore::new::<ListStore>();
        list.extend([&item0, &item1]);
        assert_eq!(list.item(0).as_ref(), Some(item0.upcast_ref()));
        assert_eq!(list.item(1).as_ref(), Some(item1.upcast_ref()));
        list.extend([item0.clone(), item1.clone()]);
        assert_eq!(list.item(2).as_ref(), Some(item0.upcast_ref()));
        assert_eq!(list.item(3).as_ref(), Some(item1.upcast_ref()));

        let list_from_slice = ListStore::new::<ListStore>();
        list_from_slice.extend_from_slice(&[item0, item1.clone()]);
        assert_eq!(list_from_slice.item(1).as_ref(), Some(item1.upcast_ref()));
    }

    #[test]
    fn from_iterator() {
        let item0 = ListStore::new::<ListStore>();
        let item1 = ListStore::new::<ListStore>();
        let v = vec![item0.clone(), item1.clone()];
        let list = ListStore::from_iter(v);
        assert_eq!(list.item(0).as_ref(), Some(item0.upcast_ref()));
        assert_eq!(list.item(1).as_ref(), Some(item1.upcast_ref()));
        assert_eq!(list.item(2).as_ref(), None);
    }

    #[cfg(feature = "v2_74")]
    #[test]
    fn find() {
        let item0 = ListStore::new::<ListStore>();
        let item1 = ListStore::new::<ListStore>();
        let list = ListStore::new::<ListStore>();
        list.append(&item0);
        list.append(&item1);

        let res = list.find_with_equal_func(|item| item == &item1);
        assert_eq!(res, Some(1));
    }

    #[test]
    fn retain() {
        let list = {
            let list = ListStore::new::<ListStore>();
            for _ in 0..10 {
                list.append(&ListStore::new::<ListStore>());
            }
            list
        };

        use std::cell::Cell;
        use std::rc::Rc;

        let signal_count = Rc::new(Cell::new(0));
        let signal_count_clone = signal_count.clone();
        list.connect_items_changed(move |_, _, _, _| {
            signal_count_clone.set(signal_count_clone.get() + 1);
        });

        let to_keep = [
            // list.item(0).unwrap(),
            list.item(1).unwrap(),
            // list.item(2).unwrap(),
            list.item(3).unwrap(),
            // list.item(4).unwrap(),
            // list.item(5).unwrap(),
            // list.item(6).unwrap(),
            list.item(7).unwrap(),
            // list.item(8).unwrap(),
            // list.item(9).unwrap(),
        ];
        list.retain(|item| to_keep.contains(item));

        // Check that we removed the correct items
        assert_eq!(list.n_items(), 3);
        assert_eq!(list.item(0).as_ref(), Some(&to_keep[0]));
        assert_eq!(list.item(1).as_ref(), Some(&to_keep[1]));
        assert_eq!(list.item(2).as_ref(), Some(&to_keep[2]));

        assert_eq!(signal_count.get(), 4);
    }
}
