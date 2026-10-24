// Take a look at the license at the top of the repository in the LICENSE file.

use std::{boxed::Box as Box_, marker::PhantomData, pin::Pin};

use futures_util::TryFutureExt;
use glib::{Object, Type, object::IsClass, prelude::*};

use crate::{AsyncInitable, Cancellable, prelude::*};

impl AsyncInitable {
    // rustdoc-stripper-ignore-next
    /// Create a new instance of an async initable object with the default property values.
    ///
    /// Similar to [`Object::new`] but can fail because the object initialization in
    /// `AsyncInitable::init` failed.
    #[doc(alias = "g_async_initable_new_async")]
    #[track_caller]
    #[allow(clippy::new_ret_no_self)]
    pub fn new<
        O: IsClass + IsA<Object> + IsA<AsyncInitable>,
        Q: FnOnce(Result<O, glib::Error>) + 'static,
    >(
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: Q,
    ) {
        Self::with_type(O::static_type(), io_priority, cancellable, move |res| {
            callback(res.map(|o| unsafe { o.unsafe_cast() }))
        })
    }

    // rustdoc-stripper-ignore-next
    /// Create a new instance of an async initable object with the default property values as future.
    ///
    /// Similar to [`Object::new`] but can fail because the object initialization in
    /// `AsyncInitable::init` failed.
    #[doc(alias = "g_async_initable_new_async")]
    #[track_caller]
    pub fn new_future<O: IsClass + IsA<Object> + IsA<AsyncInitable>>(
        io_priority: glib::Priority,
    ) -> Pin<Box_<dyn std::future::Future<Output = Result<O, glib::Error>> + 'static>> {
        Box::pin(
            Self::with_type_future(O::static_type(), io_priority)
                .map_ok(|o| unsafe { o.unsafe_cast() }),
        )
    }

    // rustdoc-stripper-ignore-next
    /// Create a new instance of an async initable object of the given type with the default
    /// property values.
    ///
    /// Similar to [`Object::with_type`] but can fail because the object initialization in
    /// `AsyncInitable::init` failed.
    #[doc(alias = "g_async_initable_new_async")]
    #[track_caller]
    pub fn with_type<Q: FnOnce(Result<Object, glib::Error>) + 'static>(
        type_: Type,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: Q,
    ) {
        if !type_.is_a(AsyncInitable::static_type()) {
            panic!("Type '{type_}' is not async initable");
        }

        unsafe {
            let obj = Object::new_internal(type_, &mut []);
            obj.unsafe_cast_ref::<Self>().init_async(
                io_priority,
                cancellable,
                glib::clone!(
                    #[strong]
                    obj,
                    move |res| {
                        callback(res.map(|_| obj));
                    }
                ),
            )
        };
    }

    // rustdoc-stripper-ignore-next
    /// Create a new instance of an async initable object of the given type with the default property values as future.
    ///
    /// Similar to [`Object::with_type`] but can fail because the object initialization in
    /// `AsyncInitable::init` failed.
    #[doc(alias = "g_async_initable_new_async")]
    #[track_caller]
    pub fn with_type_future(
        type_: Type,
        io_priority: glib::Priority,
    ) -> Pin<Box_<dyn std::future::Future<Output = Result<Object, glib::Error>> + 'static>> {
        if !type_.is_a(AsyncInitable::static_type()) {
            panic!("Type '{type_}' is not async initable");
        }

        unsafe {
            Box_::pin(crate::GioFuture::new(
                &(),
                move |_obj, cancellable, send| {
                    let obj = Object::new_internal(type_, &mut []);
                    obj.unsafe_cast_ref::<Self>().init_async(
                        io_priority,
                        Some(cancellable),
                        glib::clone!(
                            #[strong]
                            obj,
                            move |res| {
                                send.resolve(res.map(|_| obj));
                            }
                        ),
                    );
                },
            ))
        }
    }

    // rustdoc-stripper-ignore-next
    /// Create a new instance of an async initable object of the given type with the given properties as mutable values.
    ///
    /// Similar to [`Object::with_mut_values`] but can fail because the object initialization in
    /// `AsyncInitable::init` failed.
    #[doc(alias = "g_async_initable_new_async")]
    #[track_caller]
    pub fn with_mut_values<Q: FnOnce(Result<Object, glib::Error>) + 'static>(
        type_: Type,
        properties: &mut [(&str, glib::Value)],
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: Q,
    ) {
        if !type_.is_a(AsyncInitable::static_type()) {
            panic!("Type '{type_}' is not async initable");
        }

        unsafe {
            let obj = Object::new_internal(type_, properties);
            obj.unsafe_cast_ref::<Self>().init_async(
                io_priority,
                cancellable,
                glib::clone!(
                    #[strong]
                    obj,
                    move |res| {
                        callback(res.map(|_| obj));
                    }
                ),
            )
        };
    }

    // rustdoc-stripper-ignore-next
    /// Create a new instance of an async initable object of the given type with the given properties as mutable values as a future.
    ///
    /// Similar to [`Object::with_mut_values`] but can fail because the object initialization in
    /// `AsyncInitable::init` failed.
    #[doc(alias = "g_async_initable_new_async")]
    #[track_caller]
    pub fn with_mut_values_future(
        type_: Type,
        properties: &mut [(&str, glib::Value)],
        io_priority: glib::Priority,
    ) -> Pin<Box_<dyn std::future::Future<Output = Result<Object, glib::Error>> + 'static>> {
        if !type_.is_a(AsyncInitable::static_type()) {
            panic!("Type '{type_}' is not async initable");
        }

        unsafe {
            // FIXME: object construction should ideally happen as part of the future
            let obj = Object::new_internal(type_, properties);
            Box_::pin(crate::GioFuture::new(
                &obj,
                move |obj, cancellable, send| {
                    obj.unsafe_cast_ref::<Self>().init_async(
                        io_priority,
                        Some(cancellable),
                        glib::clone!(
                            #[strong]
                            obj,
                            move |res| {
                                send.resolve(res.map(|_| obj));
                            }
                        ),
                    );
                },
            ))
        }
    }

    // rustdoc-stripper-ignore-next
    /// Create a new object builder for a specific type.
    pub fn builder<'a, O: IsA<Object> + IsClass + IsA<AsyncInitable>>()
    -> AsyncInitableBuilder<'a, O> {
        AsyncInitableBuilder::new(O::static_type())
    }

    // rustdoc-stripper-ignore-next
    /// Create a new object builder for a specific type.
    pub fn builder_with_type<'a>(type_: Type) -> AsyncInitableBuilder<'a, Object> {
        if !type_.is_a(AsyncInitable::static_type()) {
            panic!("Type '{type_}' is not async initable");
        }

        AsyncInitableBuilder::new(type_)
    }
}

#[must_use = "builder doesn't do anything unless built"]
pub struct AsyncInitableBuilder<'a, O> {
    type_: Type,
    properties: smallvec::SmallVec<[(&'a str, glib::Value); 16]>,
    phantom: PhantomData<O>,
}

impl<'a, O: IsA<Object> + IsClass> AsyncInitableBuilder<'a, O> {
    #[inline]
    fn new(type_: Type) -> Self {
        AsyncInitableBuilder {
            type_,
            properties: smallvec::SmallVec::new(),
            phantom: PhantomData,
        }
    }

    // rustdoc-stripper-ignore-next
    /// Gets the type of this builder.
    #[inline]
    pub fn type_(&self) -> Type {
        self.type_
    }

    // rustdoc-stripper-ignore-next
    /// Set property `name` to the given value `value`.
    #[inline]
    pub fn property(self, name: &'a str, value: impl Into<glib::Value>) -> Self {
        let AsyncInitableBuilder {
            type_,
            mut properties,
            ..
        } = self;
        properties.push((name, value.into()));

        AsyncInitableBuilder {
            type_,
            properties,
            phantom: PhantomData,
        }
    }

    // rustdoc-stripper-ignore-next
    /// Build the object with the provided properties.
    ///
    /// # Panics
    ///
    /// This panics if the object is not instantiable, doesn't have all the given properties or
    /// property values of the wrong type are provided.
    #[track_caller]
    #[inline]
    pub fn build<Q: FnOnce(Result<O, glib::Error>) + 'static>(
        mut self,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: Q,
    ) {
        AsyncInitable::with_mut_values(
            self.type_,
            &mut self.properties,
            io_priority,
            cancellable,
            move |res| callback(res.map(|o| unsafe { o.unsafe_cast() })),
        );
    }

    // rustdoc-stripper-ignore-next
    /// Build the object with the provided properties.
    ///
    /// # Panics
    ///
    /// This panics if the object is not instantiable, doesn't have all the given properties or
    /// property values of the wrong type are provided.
    #[track_caller]
    #[inline]
    pub fn build_future(
        mut self,
        io_priority: glib::Priority,
    ) -> Pin<Box_<dyn std::future::Future<Output = Result<O, glib::Error>> + 'static>> {
        Box::pin(
            AsyncInitable::with_mut_values_future(self.type_, &mut self.properties, io_priority)
                .map_ok(|o| unsafe { o.unsafe_cast() }),
        )
    }
}
