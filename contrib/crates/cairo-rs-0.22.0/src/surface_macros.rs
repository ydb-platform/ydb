// Take a look at the license at the top of the repository in the LICENSE file.

// e.g. declare_surface(ImageSurface, SurfaceType::Image)
macro_rules! declare_surface {
    ($surf_name:ident, $surf_type:expr) => {
        #[derive(Debug)]
        #[repr(transparent)]
        pub struct $surf_name(Surface);

        impl TryFrom<Surface> for $surf_name {
            type Error = Surface;

            #[inline]
            fn try_from(surface: Surface) -> Result<$surf_name, Surface> {
                if surface.type_() == $surf_type {
                    Ok($surf_name(surface))
                } else {
                    Err(surface)
                }
            }
        }

        impl $surf_name {
            #[inline]
            pub unsafe fn from_raw_full(
                ptr: *mut crate::ffi::cairo_surface_t,
            ) -> Result<$surf_name, crate::error::Error> {
                unsafe {
                    let surface = Surface::from_raw_full(ptr)?;
                    Self::try_from(surface).map_err(|_| crate::error::Error::SurfaceTypeMismatch)
                }
            }

            #[inline]
            pub unsafe fn from_raw_none(
                ptr: *mut crate::ffi::cairo_surface_t,
            ) -> Result<$surf_name, crate::error::Error> {
                unsafe {
                    let surface = Surface::from_raw_none(ptr);
                    Self::try_from(surface).map_err(|_| crate::error::Error::SurfaceTypeMismatch)
                }
            }
        }

        #[cfg(feature = "use_glib")]
        impl IntoGlibPtr<*mut crate::ffi::cairo_surface_t> for $surf_name {
            #[inline]
            fn into_glib_ptr(self) -> *mut crate::ffi::cairo_surface_t {
                std::mem::ManuallyDrop::new(self).to_glib_none().0
            }
        }

        #[cfg(feature = "use_glib")]
        impl<'a> ToGlibPtr<'a, *mut crate::ffi::cairo_surface_t> for $surf_name {
            type Storage = std::marker::PhantomData<&'a Surface>;

            #[inline]
            fn to_glib_none(&'a self) -> Stash<'a, *mut crate::ffi::cairo_surface_t, Self> {
                let stash = self.0.to_glib_none();
                Stash(stash.0, stash.1)
            }

            #[inline]
            fn to_glib_full(&self) -> *mut crate::ffi::cairo_surface_t {
                unsafe { crate::ffi::cairo_surface_reference(self.to_glib_none().0) }
            }
        }

        #[cfg(feature = "use_glib")]
        impl FromGlibPtrNone<*mut crate::ffi::cairo_surface_t> for $surf_name {
            #[inline]
            unsafe fn from_glib_none(ptr: *mut crate::ffi::cairo_surface_t) -> $surf_name {
                unsafe { Self::try_from(from_glib_none::<_, Surface>(ptr)).unwrap() }
            }
        }

        #[cfg(feature = "use_glib")]
        impl FromGlibPtrBorrow<*mut crate::ffi::cairo_surface_t> for $surf_name {
            #[inline]
            unsafe fn from_glib_borrow(
                ptr: *mut crate::ffi::cairo_surface_t,
            ) -> crate::Borrowed<$surf_name> {
                unsafe {
                    let surface = from_glib_borrow::<_, Surface>(ptr);
                    let surface = Self::try_from(surface.into_inner())
                        .map_err(std::mem::forget)
                        .unwrap();
                    crate::Borrowed::new(surface)
                }
            }
        }

        #[cfg(feature = "use_glib")]
        impl FromGlibPtrFull<*mut crate::ffi::cairo_surface_t> for $surf_name {
            #[inline]
            unsafe fn from_glib_full(ptr: *mut crate::ffi::cairo_surface_t) -> $surf_name {
                unsafe { Self::from_raw_full(ptr).unwrap() }
            }
        }

        #[cfg(feature = "use_glib")]
        gvalue_impl!(
            $surf_name,
            crate::ffi::cairo_surface_t,
            crate::ffi::gobject::cairo_gobject_surface_get_type
        );

        impl Deref for $surf_name {
            type Target = Surface;

            #[inline]
            fn deref(&self) -> &Surface {
                &self.0
            }
        }

        impl AsRef<Surface> for $surf_name {
            #[inline]
            fn as_ref(&self) -> &Surface {
                &self.0
            }
        }

        impl Clone for $surf_name {
            #[inline]
            fn clone(&self) -> $surf_name {
                $surf_name(self.0.clone())
            }
        }
    };
}
