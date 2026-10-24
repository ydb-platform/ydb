// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;
#[cfg(feature = "use_glib")]
use std::marker::PhantomData;

use crate::{Error, ffi, utils::status_to_result};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq)]
#[doc(alias = "cairo_matrix_t")]
pub struct Matrix(ffi::cairo_matrix_t);

impl Default for Matrix {
    fn default() -> Self {
        Self::identity()
    }
}

impl Matrix {
    #[inline]
    pub(crate) fn ptr(&self) -> *const ffi::cairo_matrix_t {
        self as *const Matrix as _
    }

    #[inline]
    pub(crate) fn mut_ptr(&mut self) -> *mut ffi::cairo_matrix_t {
        self as *mut Matrix as _
    }

    #[inline]
    pub(crate) fn null() -> Self {
        Self(ffi::cairo_matrix_t {
            xx: 0.0,
            yx: 0.0,
            xy: 0.0,
            yy: 0.0,
            x0: 0.0,
            y0: 0.0,
        })
    }

    #[inline]
    pub fn identity() -> Self {
        Self(ffi::cairo_matrix_t {
            xx: 1.0,
            yx: 0.0,
            xy: 0.0,
            yy: 1.0,
            x0: 0.0,
            y0: 0.0,
        })
    }

    #[inline]
    pub fn new(xx: f64, yx: f64, xy: f64, yy: f64, x0: f64, y0: f64) -> Self {
        Self(ffi::cairo_matrix_t {
            xx,
            yx,
            xy,
            yy,
            x0,
            y0,
        })
    }

    #[inline]
    pub fn xx(&self) -> f64 {
        self.0.xx
    }
    #[inline]
    pub fn set_xx(&mut self, xx: f64) {
        self.0.xx = xx;
    }
    #[inline]
    pub fn yx(&self) -> f64 {
        self.0.yx
    }
    #[inline]
    pub fn set_yx(&mut self, yx: f64) {
        self.0.yx = yx;
    }
    #[inline]
    pub fn xy(&self) -> f64 {
        self.0.xy
    }
    #[inline]
    pub fn set_xy(&mut self, xy: f64) {
        self.0.xy = xy;
    }
    #[inline]
    pub fn yy(&self) -> f64 {
        self.0.yy
    }
    #[inline]
    pub fn set_yy(&mut self, yy: f64) {
        self.0.yy = yy;
    }
    #[inline]
    pub fn x0(&self) -> f64 {
        self.0.x0
    }
    #[inline]
    pub fn set_x0(&mut self, x0: f64) {
        self.0.x0 = x0;
    }
    #[inline]
    pub fn y0(&self) -> f64 {
        self.0.y0
    }
    #[inline]
    pub fn set_y0(&mut self, y0: f64) {
        self.0.y0 = y0;
    }

    #[doc(alias = "cairo_matrix_multiply")]
    #[inline]
    pub fn multiply(left: &Matrix, right: &Matrix) -> Matrix {
        let mut matrix = Self::null();
        unsafe {
            ffi::cairo_matrix_multiply(matrix.mut_ptr(), left.ptr(), right.ptr());
        }
        matrix
    }

    #[doc(alias = "cairo_matrix_translate")]
    #[inline]
    pub fn translate(&mut self, tx: f64, ty: f64) {
        unsafe { ffi::cairo_matrix_translate(self.mut_ptr(), tx, ty) }
    }

    #[doc(alias = "cairo_matrix_scale")]
    #[inline]
    pub fn scale(&mut self, sx: f64, sy: f64) {
        unsafe { ffi::cairo_matrix_scale(self.mut_ptr(), sx, sy) }
    }

    #[doc(alias = "cairo_matrix_rotate")]
    #[inline]
    pub fn rotate(&mut self, angle: f64) {
        unsafe { ffi::cairo_matrix_rotate(self.mut_ptr(), angle) }
    }

    #[doc(alias = "cairo_matrix_invert")]
    #[inline]
    pub fn invert(&mut self) {
        let status = unsafe { ffi::cairo_matrix_invert(self.mut_ptr()) };
        status_to_result(status).expect("Failed to invert the matrix")
    }

    #[doc(alias = "cairo_matrix_invert")]
    pub fn try_invert(&self) -> Result<Matrix, Error> {
        let mut matrix = *self;

        let status = unsafe { ffi::cairo_matrix_invert(matrix.mut_ptr()) };
        status_to_result(status)?;
        Ok(matrix)
    }

    #[doc(alias = "cairo_matrix_transform_distance")]
    #[inline]
    pub fn transform_distance(&self, _dx: f64, _dy: f64) -> (f64, f64) {
        let mut dx = _dx;
        let mut dy = _dy;

        unsafe {
            ffi::cairo_matrix_transform_distance(self.ptr(), &mut dx, &mut dy);
        }
        (dx, dy)
    }

    #[doc(alias = "cairo_matrix_transform_point")]
    #[inline]
    pub fn transform_point(&self, _x: f64, _y: f64) -> (f64, f64) {
        let mut x = _x;
        let mut y = _y;

        unsafe {
            ffi::cairo_matrix_transform_point(self.ptr(), &mut x, &mut y);
        }
        (x, y)
    }
}

impl fmt::Debug for Matrix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Matrix")
            .field("xx", &self.xx())
            .field("yx", &self.yx())
            .field("xy", &self.xy())
            .field("yy", &self.yy())
            .field("x0", &self.x0())
            .field("y0", &self.y0())
            .finish()
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl Uninitialized for Matrix {
    #[inline]
    unsafe fn uninitialized() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl<'a> ToGlibPtr<'a, *const ffi::cairo_matrix_t> for Matrix {
    type Storage = PhantomData<&'a Self>;

    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *const ffi::cairo_matrix_t, Self> {
        Stash(
            self as *const Matrix as *const ffi::cairo_matrix_t,
            PhantomData,
        )
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl<'a> ToGlibPtrMut<'a, *mut ffi::cairo_matrix_t> for Matrix {
    type Storage = PhantomData<&'a mut Self>;

    #[inline]
    fn to_glib_none_mut(&'a mut self) -> StashMut<'a, *mut ffi::cairo_matrix_t, Self> {
        StashMut(self as *mut Matrix as *mut ffi::cairo_matrix_t, PhantomData)
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl FromGlibPtrNone<*const ffi::cairo_matrix_t> for Matrix {
    #[inline]
    unsafe fn from_glib_none(ptr: *const ffi::cairo_matrix_t) -> Self {
        unsafe { *(ptr as *const Matrix) }
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl FromGlibPtrBorrow<*mut ffi::cairo_matrix_t> for Matrix {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::cairo_matrix_t) -> crate::Borrowed<Self> {
        unsafe { crate::Borrowed::new(*(ptr as *mut Matrix)) }
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl FromGlibPtrNone<*mut ffi::cairo_matrix_t> for Matrix {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::cairo_matrix_t) -> Self {
        unsafe { *(ptr as *mut Matrix) }
    }
}

#[cfg(feature = "use_glib")]
gvalue_impl_inline!(
    Matrix,
    ffi::cairo_matrix_t,
    ffi::gobject::cairo_gobject_matrix_get_type
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_matrix_does_not_invert() {
        let matrix = Matrix::null();
        assert!(matrix.try_invert().is_err());
    }

    #[test]
    #[should_panic]
    fn inverting_invalid_matrix_panics() {
        let mut matrix = Matrix::null();
        matrix.invert();
    }

    #[test]
    fn valid_matrix_try_invert() {
        let matrix = Matrix::identity();
        assert_eq!(matrix.try_invert().unwrap(), Matrix::identity());
    }

    #[test]
    fn valid_matrix_invert() {
        let mut matrix = Matrix::identity();
        matrix.invert();
        assert_eq!(matrix, Matrix::identity());
    }
}
