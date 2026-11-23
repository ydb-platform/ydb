// Take a look at the license at the top of the repository in the LICENSE file.

use std::ptr;

use glib::{Error, prelude::*, subclass::prelude::*, translate::*};

use crate::{Cancellable, Initable, ffi};

pub trait InitableImpl: ObjectImpl + ObjectSubclass<Type: IsA<Initable>> {
    fn init(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_init(cancellable)
    }
}

pub trait InitableImplExt: InitableImpl {
    fn parent_init(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<Initable>() as *const ffi::GInitableIface;

            let func = (*parent_iface)
                .init
                .expect("no parent \"init\" implementation");

            let mut err = ptr::null_mut();
            func(
                self.obj().unsafe_cast_ref::<Initable>().to_glib_none().0,
                cancellable.to_glib_none().0,
                &mut err,
            );

            if err.is_null() {
                Ok(())
            } else {
                Err(from_glib_full(err))
            }
        }
    }
}

impl<T: InitableImpl> InitableImplExt for T {}

unsafe impl<T: InitableImpl> IsImplementable<T> for Initable {
    fn interface_init(iface: &mut glib::Interface<Self>) {
        let iface = iface.as_mut();
        iface.init = Some(initable_init::<T>);
    }
}

unsafe extern "C" fn initable_init<T: InitableImpl>(
    initable: *mut ffi::GInitable,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(initable as *mut T::Instance);
        let imp = instance.imp();

        match imp.init(
            Option::<Cancellable>::from_glib_borrow(cancellable)
                .as_ref()
                .as_ref(),
        ) {
            Ok(()) => glib::ffi::GTRUE,
            Err(e) => {
                if !error.is_null() {
                    *error = e.into_glib_ptr();
                }
                glib::ffi::GFALSE
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cancellable, Initable, prelude::*};

    pub mod imp {
        use std::cell::Cell;

        use super::*;

        pub struct InitableTestType(pub Cell<u64>);

        #[glib::object_subclass]
        impl ObjectSubclass for InitableTestType {
            const NAME: &'static str = "InitableTestType";
            type Type = super::InitableTestType;
            type Interfaces = (Initable,);

            fn new() -> Self {
                Self(Cell::new(0))
            }
        }

        impl InitableImpl for InitableTestType {
            fn init(&self, _cancellable: Option<&Cancellable>) -> Result<(), glib::Error> {
                self.0.set(0x123456789abcdef);
                Ok(())
            }
        }

        impl ObjectImpl for InitableTestType {}
    }

    pub mod ffi {
        use super::*;
        pub type InitableTestType = <imp::InitableTestType as ObjectSubclass>::Instance;

        pub unsafe extern "C" fn initable_test_type_get_value(this: *mut InitableTestType) -> u64 {
            unsafe {
                let this = super::InitableTestType::from_glib_borrow(this);
                this.imp().0.get()
            }
        }
    }

    glib::wrapper! {
        pub struct InitableTestType(ObjectSubclass<imp::InitableTestType>)
            @implements Initable;
    }

    #[allow(clippy::new_without_default)]
    impl InitableTestType {
        pub fn new() -> Self {
            Initable::new(Option::<&Cancellable>::None)
                .expect("Failed creation/initialization of InitableTestType object")
        }

        pub unsafe fn new_uninit() -> Self {
            unsafe {
                // This creates an uninitialized InitableTestType object, for testing
                // purposes. In real code, using Initable::new (like the new() method
                // does) is recommended.
                glib::Object::new_internal(Self::static_type(), &mut [])
                    .downcast()
                    .unwrap()
            }
        }

        pub fn value(&self) -> u64 {
            self.imp().0.get()
        }
    }

    #[test]
    fn test_initable_with_init() {
        let res = unsafe {
            let test = InitableTestType::new_uninit();

            assert_ne!(0x123456789abcdef, test.value());

            test.init(Option::<&Cancellable>::None).map(|_| test)
        };
        assert!(res.is_ok());
        let test = res.unwrap();

        assert_eq!(0x123456789abcdef, test.value());
    }

    #[test]
    fn test_initable_with_initable_new() {
        let test = InitableTestType::new();
        assert_eq!(0x123456789abcdef, test.value());
    }

    #[test]
    #[should_panic = ""]
    fn test_initable_new_failure() {
        let value: u32 = 2;
        let _ = Initable::builder::<InitableTestType>()
            .property("invalid-property", value)
            .build(Option::<&Cancellable>::None);
        unreachable!();
    }

    #[test]
    fn test_initable_with_initable_with_type() {
        let test = Initable::with_type(
            InitableTestType::static_type(),
            Option::<&Cancellable>::None,
        )
        .expect("Failed creation/initialization of InitableTestType object from type")
        .downcast::<InitableTestType>()
        .expect("Failed downcast of InitableTestType object");
        assert_eq!(0x123456789abcdef, test.value());
    }

    #[test]
    fn test_initable_with_initable_with_values() {
        let test = Initable::with_type(
            InitableTestType::static_type(),
            Option::<&Cancellable>::None,
        )
        .expect("Failed creation/initialization of InitableTestType object from values")
        .downcast::<InitableTestType>()
        .expect("Failed downcast of InitableTestType object");
        assert_eq!(0x123456789abcdef, test.value());
    }

    #[test]
    fn test_initable_through_ffi() {
        unsafe {
            let test = InitableTestType::new_uninit();
            let test: *mut ffi::InitableTestType = test.as_ptr();
            let mut error: *mut glib::ffi::GError = std::ptr::null_mut();

            assert_ne!(0x123456789abcdef, ffi::initable_test_type_get_value(test));

            let result = crate::ffi::g_initable_init(
                test as *mut crate::ffi::GInitable,
                std::ptr::null_mut(),
                &mut error,
            );

            assert_eq!(glib::ffi::GTRUE, result);
            assert_eq!(error, ptr::null_mut());
            assert_eq!(0x123456789abcdef, ffi::initable_test_type_get_value(test));
        }
    }
}
