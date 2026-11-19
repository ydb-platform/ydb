// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{prelude::*, subclass::prelude::*, translate::*};

use crate::{File, FileMonitor, FileMonitorEvent, ffi};

// Support custom implementation of virtual functions defined in `gio::ffi::GFileMonitorClass`.
pub trait FileMonitorImpl: ObjectImpl + ObjectSubclass<Type: IsA<FileMonitor>> {
    fn changed(&self, file: &File, other_file: Option<&File>, event_type: FileMonitorEvent) {
        self.parent_changed(file, other_file, event_type)
    }

    fn cancel(&self) {
        self.parent_cancel()
    }
}

// Support parent implementation of virtual functions defined in `gio::ffi::GFileMonitorClass`.
pub trait FileMonitorImplExt: FileMonitorImpl {
    fn parent_changed(&self, file: &File, other_file: Option<&File>, event_type: FileMonitorEvent) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GFileMonitorClass;

            if let Some(f) = (*parent_class).changed {
                f(
                    self.obj().unsafe_cast_ref::<FileMonitor>().to_glib_none().0,
                    file.to_glib_none().0,
                    other_file.to_glib_none().0,
                    event_type.into_glib(),
                );
            }
        }
    }

    fn parent_cancel(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GFileMonitorClass;

            let f = (*parent_class)
                .cancel
                .expect("No parent class implementation for \"cancel\"");

            let _ = f(self.obj().unsafe_cast_ref::<FileMonitor>().to_glib_none().0);
        }
    }
}

impl<T: FileMonitorImpl> FileMonitorImplExt for T {}

// Implement virtual functions defined in `gio::ffi::GFileMonitorClass`.
unsafe impl<T: FileMonitorImpl> IsSubclassable<T> for FileMonitor {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.changed = Some(changed::<T>);
        klass.cancel = Some(cancel::<T>);
    }
}

unsafe extern "C" fn changed<T: FileMonitorImpl>(
    monitor: *mut ffi::GFileMonitor,
    file: *mut ffi::GFile,
    other_file: *mut ffi::GFile,
    event_type: ffi::GFileMonitorEvent,
) {
    unsafe {
        let instance = &*(monitor as *mut T::Instance);
        let imp = instance.imp();
        let other_file = Option::<File>::from_glib_none(other_file);

        imp.changed(
            &from_glib_borrow(file),
            other_file.as_ref(),
            from_glib(event_type),
        );
    }
}

unsafe extern "C" fn cancel<T: FileMonitorImpl>(
    monitor: *mut ffi::GFileMonitor,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(monitor as *mut T::Instance);
        let imp = instance.imp();

        imp.cancel();

        // vfunc must return true as specified in documentation.
        // https://docs.gtk.org/gio/vfunc.FileMonitor.cancel.html
        true.into_glib()
    }
}

#[cfg(test)]
mod tests {
    // The following tests rely on a custom type `MyCustomFileMonitor` that extends another custom type `MyFileMonitor`.
    // For each virtual method defined in class `gio::ffi::GFileMonitorClass`, a test checks that `MyCustomFileMonitor` and `MyFileMonitor` return the same results.

    use super::*;
    use crate::prelude::*;

    // Define `MyCustomFileMonitor` as a subclass of `MyFileMonitor`.
    mod imp {
        use super::*;

        #[derive(Default)]
        pub struct MyFileMonitor;

        #[glib::object_subclass]
        impl ObjectSubclass for MyFileMonitor {
            const NAME: &'static str = "MyFileMonitor";
            type Type = super::MyFileMonitor;
            type ParentType = FileMonitor;
        }

        impl ObjectImpl for MyFileMonitor {}

        // Implements `FileMonitorImpl` with custom implementation.
        impl FileMonitorImpl for MyFileMonitor {
            fn cancel(&self) {}
        }

        #[derive(Default)]
        pub struct MyCustomFileMonitor;

        #[glib::object_subclass]
        impl ObjectSubclass for MyCustomFileMonitor {
            const NAME: &'static str = "MyCustomFileMonitor";
            type Type = super::MyCustomFileMonitor;
            type ParentType = super::MyFileMonitor;
        }

        impl ObjectImpl for MyCustomFileMonitor {}

        // Implements `FileMonitorImpl` with default implementation, which calls the parent's implementation.
        impl FileMonitorImpl for MyCustomFileMonitor {}

        impl MyFileMonitorImpl for MyCustomFileMonitor {}
    }

    glib::wrapper! {
        pub struct MyFileMonitor(ObjectSubclass<imp::MyFileMonitor>) @extends FileMonitor;
    }

    pub trait MyFileMonitorImpl:
        ObjectImpl + ObjectSubclass<Type: IsA<MyFileMonitor> + IsA<FileMonitor>>
    {
    }

    // To make this class subclassable we need to implement IsSubclassable
    unsafe impl<T: MyFileMonitorImpl + FileMonitorImpl> IsSubclassable<T> for MyFileMonitor {}

    glib::wrapper! {
        pub struct MyCustomFileMonitor(ObjectSubclass<imp::MyCustomFileMonitor>) @extends MyFileMonitor, FileMonitor;
    }

    #[test]
    fn file_monitor_changed() {
        // run test in a main context dedicated and configured as the thread default one
        let _ = glib::MainContext::new().with_thread_default(|| {
            // invoke `MyCustomFileMonitor` implementation of `gio::ffi::GFileMonitorClass::cancel`
            let my_custom_file_monitor = glib::Object::new::<MyCustomFileMonitor>();
            let rx = {
                let (tx, rx) = async_channel::bounded(1);
                my_custom_file_monitor.connect_changed(move |_, file, other_file, event_type| {
                    let res = glib::MainContext::ref_thread_default().block_on(tx.send((
                        file.uri(),
                        other_file.map(File::uri),
                        event_type,
                    )));
                    assert!(res.is_ok(), "{}", res.err().unwrap());
                });
                rx
            };
            // emit an event
            my_custom_file_monitor.emit_event(
                &File::for_uri("child"),
                None::<&File>,
                FileMonitorEvent::Created,
            );
            let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
            assert!(res.is_ok(), "{}", res.err().unwrap());
            let event = res.unwrap();

            // invoke `MyFileMonitor` implementation of `gio::ffi::GFileMonitorClass::cancel`
            let my_file_monitor = glib::Object::new::<MyFileMonitor>();
            let expected_rx = {
                let (tx, rx) = async_channel::bounded(1);
                my_file_monitor.connect_changed(move |_, file, other_file, event_type| {
                    let res = glib::MainContext::ref_thread_default().block_on(tx.send((
                        file.uri(),
                        other_file.map(File::uri),
                        event_type,
                    )));
                    assert!(res.is_ok(), "{}", res.err().unwrap());
                });
                rx
            };
            // emit an event
            my_file_monitor.emit_event(
                &File::for_uri("child"),
                None::<&File>,
                FileMonitorEvent::Created,
            );
            let res = glib::MainContext::ref_thread_default().block_on(expected_rx.recv());
            assert!(res.is_ok(), "{}", res.err().unwrap());
            let expected_event = res.unwrap();

            // both results should equal
            assert_eq!(event, expected_event);
        });
    }

    #[test]
    fn file_monitor_cancel() {
        // run test in a main context dedicated and configured as the thread default one
        let _ = glib::MainContext::new().with_thread_default(|| {
            // invoke `MyCustomFileMonitor` implementation of `gio::ffi::GFileMonitorClass::cancel`
            let my_custom_file_monitor = glib::Object::new::<MyCustomFileMonitor>();
            let rx = {
                let (tx, rx) = async_channel::bounded(1);
                my_custom_file_monitor.connect_cancelled_notify(move |_| {
                    let res = glib::MainContext::ref_thread_default().block_on(tx.send(true));
                    assert!(res.is_ok(), "{}", res.err().unwrap());
                });
                rx
            };
            let cancelled = my_custom_file_monitor.cancel();
            let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
            assert!(res.is_ok(), "{}", res.err().unwrap());
            let notified = res.unwrap();
            assert_eq!(cancelled, notified);

            // invoke `MyFileMonitor` implementation of `gio::ffi::GFileMonitorClass::cancel`
            let my_file_monitor = glib::Object::new::<MyFileMonitor>();
            let expected_rx = {
                let (tx, rx) = async_channel::bounded(1);
                my_file_monitor.connect_cancelled_notify(move |_| {
                    let res = glib::MainContext::ref_thread_default().block_on(tx.send(true));
                    assert!(res.is_ok(), "{}", res.err().unwrap());
                });
                rx
            };
            let expected_cancelled = my_file_monitor.cancel();
            let res = glib::MainContext::ref_thread_default().block_on(expected_rx.recv());
            assert!(res.is_ok(), "{}", res.err().unwrap());
            let expected_notified = res.unwrap();
            assert_eq!(expected_cancelled, expected_notified);

            // both results should equal
            assert_eq!(cancelled, expected_cancelled);
            assert_eq!(notified, expected_notified);
        });
    }
}
