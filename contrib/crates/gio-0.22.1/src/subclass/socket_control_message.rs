// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{prelude::*, subclass::prelude::*, translate::*};

use crate::{SocketControlMessage, ffi};

pub trait SocketControlMessageImpl:
    ObjectImpl + ObjectSubclass<Type: IsA<SocketControlMessage>>
{
    fn level(&self) -> i32 {
        self.parent_level()
    }

    fn msg_type(&self) -> i32 {
        self.parent_msg_type()
    }

    fn size(&self) -> usize {
        self.parent_size()
    }

    fn serialize(&self, data: &mut [u8]) {
        self.parent_serialize(data);
    }

    fn deserialize(level: i32, type_: i32, data: &[u8]) -> Option<SocketControlMessage> {
        Self::parent_deserialize(level, type_, data)
    }
}

pub trait SocketControlMessageImplExt: SocketControlMessageImpl {
    fn parent_level(&self) -> i32 {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GSocketControlMessageClass;
            let f = (*parent_class)
                .get_level
                .expect("No parent class implementation for \"level\"");

            f(self
                .obj()
                .unsafe_cast_ref::<SocketControlMessage>()
                .to_glib_none()
                .0)
        }
    }

    fn parent_msg_type(&self) -> i32 {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GSocketControlMessageClass;
            let f = (*parent_class)
                .get_type
                .expect("No parent class implementation for \"msg_type\"");

            f(self
                .obj()
                .unsafe_cast_ref::<SocketControlMessage>()
                .to_glib_none()
                .0)
        }
    }

    fn parent_size(&self) -> usize {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GSocketControlMessageClass;
            let f = (*parent_class)
                .get_size
                .expect("No parent class implementation for \"size\"");

            f(self
                .obj()
                .unsafe_cast_ref::<SocketControlMessage>()
                .to_glib_none()
                .0)
        }
    }

    fn parent_serialize(&self, data: &mut [u8]) {
        unsafe {
            let type_data = Self::type_data();
            let parent_class =
                type_data.as_ref().parent_class() as *mut ffi::GSocketControlMessageClass;
            let f = (*parent_class)
                .serialize
                .expect("No parent class implementation for \"serialize\"");

            f(
                self.obj()
                    .unsafe_cast_ref::<SocketControlMessage>()
                    .to_glib_none()
                    .0,
                data.as_mut_ptr() as _,
            )
        }
    }

    fn parent_deserialize(level: i32, type_: i32, data: &[u8]) -> Option<SocketControlMessage> {
        unsafe {
            let type_data = Self::type_data();
            let parent_class =
                type_data.as_ref().parent_class() as *mut ffi::GSocketControlMessageClass;

            (*parent_class).deserialize.map(|f| {
                let message_ptr = f(level, type_, data.len(), data.as_ptr() as _);
                from_glib_full(message_ptr)
            })
        }
    }
}

impl<T: SocketControlMessageImpl> SocketControlMessageImplExt for T {}

unsafe impl<T: SocketControlMessageImpl> IsSubclassable<T> for SocketControlMessage {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.get_level = Some(socket_control_message_get_level::<T>);
        klass.get_type = Some(socket_control_message_get_type::<T>);
        klass.get_size = Some(socket_control_message_get_size::<T>);
        klass.serialize = Some(socket_control_message_serialize::<T>);
        klass.deserialize = Some(socket_control_message_deserialize::<T>);
    }
}

unsafe extern "C" fn socket_control_message_get_level<T: SocketControlMessageImpl>(
    ptr: *mut ffi::GSocketControlMessage,
) -> i32 {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.level()
    }
}

unsafe extern "C" fn socket_control_message_get_type<T: SocketControlMessageImpl>(
    ptr: *mut ffi::GSocketControlMessage,
) -> i32 {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.msg_type()
    }
}

unsafe extern "C" fn socket_control_message_get_size<T: SocketControlMessageImpl>(
    ptr: *mut ffi::GSocketControlMessage,
) -> usize {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.size()
    }
}

unsafe extern "C" fn socket_control_message_serialize<T: SocketControlMessageImpl>(
    ptr: *mut ffi::GSocketControlMessage,
    data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let data = std::slice::from_raw_parts_mut(data as *mut u8, imp.size());

        imp.serialize(data);
    }
}

unsafe extern "C" fn socket_control_message_deserialize<T: SocketControlMessageImpl>(
    level: i32,
    type_: i32,
    size: usize,
    data: glib::ffi::gpointer,
) -> *mut ffi::GSocketControlMessage {
    unsafe {
        let data = std::slice::from_raw_parts(data as *mut u8, size);

        T::deserialize(level, type_, data).into_glib_ptr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::cell::Cell;
    use std::mem::size_of;

    mod imp {
        use super::*;

        #[derive(Default)]
        pub struct TestSocketControlMessage(pub Cell<u64>);

        #[glib::object_subclass]
        impl ObjectSubclass for TestSocketControlMessage {
            const NAME: &'static str = "TestSocketControlMessage";
            type Type = super::TestSocketControlMessage;
            type ParentType = SocketControlMessage;
        }

        impl ObjectImpl for TestSocketControlMessage {}

        impl SocketControlMessageImpl for TestSocketControlMessage {
            fn level(&self) -> i32 {
                i32::MAX
            }

            fn msg_type(&self) -> i32 {
                i32::MAX
            }

            fn size(&self) -> usize {
                size_of::<u64>()
            }

            fn serialize(&self, data: &mut [u8]) {
                data.copy_from_slice(&self.0.get().to_ne_bytes());
            }

            fn deserialize(level: i32, type_: i32, data: &[u8]) -> Option<SocketControlMessage> {
                if level == i32::MAX && type_ == i32::MAX {
                    let obj = glib::Object::new::<super::TestSocketControlMessage>();
                    obj.imp().0.set(u64::from_ne_bytes(data.try_into().ok()?));
                    Some(obj.into())
                } else {
                    None
                }
            }
        }
    }

    glib::wrapper! {
        pub struct TestSocketControlMessage(ObjectSubclass<imp::TestSocketControlMessage>)
            @extends SocketControlMessage;
    }

    #[test]
    fn test_socket_control_message_subclassing() {
        let obj = glib::Object::new::<TestSocketControlMessage>();

        assert_eq!(obj.level(), i32::MAX);
        assert_eq!(obj.msg_type(), i32::MAX);
        assert_eq!(obj.size(), size_of::<u64>());

        obj.imp().0.set(0x12345678abcdefu64);

        let mut data = [0; size_of::<u64>()];
        obj.serialize(&mut data);

        let de = SocketControlMessage::deserialize(i32::MAX, i32::MAX, &data)
            .expect("deserialize failed");
        let de = de
            .downcast::<TestSocketControlMessage>()
            .expect("downcast failed");
        assert_eq!(de.imp().0.get(), 0x12345678abcdefu64);
    }
}
