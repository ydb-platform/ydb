// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;

use glib::translate::*;

use crate::{Color, ffi};

impl Color {
    #[doc(alias = "pango_color_parse")]
    pub fn parse(spec: &str) -> Result<Self, glib::BoolError> {
        unsafe {
            let mut color = Self::uninitialized();
            let is_success =
                ffi::pango_color_parse(color.to_glib_none_mut().0, spec.to_glib_none().0);
            if from_glib(is_success) {
                Ok(color)
            } else {
                Err(glib::bool_error!("Failed to parse the color"))
            }
        }
    }

    #[cfg(feature = "v1_46")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v1_46")))]
    #[doc(alias = "pango_color_parse_with_alpha")]
    pub fn parse_with_alpha(spec: &str) -> Result<(Self, u16), glib::BoolError> {
        unsafe {
            let mut color = Self::uninitialized();
            let mut alpha = std::mem::MaybeUninit::uninit();
            let is_success = ffi::pango_color_parse_with_alpha(
                color.to_glib_none_mut().0,
                alpha.as_mut_ptr(),
                spec.to_glib_none().0,
            );
            if from_glib(is_success) {
                Ok((color, alpha.assume_init()))
            } else {
                Err(glib::bool_error!("Failed to parse the color with alpha"))
            }
        }
    }

    pub fn red(&self) -> u16 {
        unsafe { *self.to_glib_none().0 }.red
    }

    pub fn green(&self) -> u16 {
        unsafe { *self.to_glib_none().0 }.green
    }

    pub fn blue(&self) -> u16 {
        unsafe { *self.to_glib_none().0 }.blue
    }
}

impl fmt::Debug for Color {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Color")
            .field("red", &self.red())
            .field("green", &self.green())
            .field("blue", &self.blue())
            .finish()
    }
}

impl std::str::FromStr for Color {
    type Err = glib::BoolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Color::parse(s)
    }
}
