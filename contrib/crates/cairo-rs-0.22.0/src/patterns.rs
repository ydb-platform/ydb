// Take a look at the license at the top of the repository in the LICENSE file.

use std::{ops::Deref, ptr};

use libc::{c_double, c_int, c_uint};

use crate::{
    Error, Extend, Filter, Matrix, MeshCorner, Path, PatternType, Surface, ffi,
    utils::status_to_result,
};

// See https://cairographics.org/manual/bindings-patterns.html for more info
#[derive(Debug)]
pub struct Pattern {
    pointer: *mut ffi::cairo_pattern_t,
}

impl Pattern {
    user_data_methods! {
        ffi::cairo_pattern_get_user_data,
        ffi::cairo_pattern_set_user_data,
    }

    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::cairo_pattern_t {
        self.pointer
    }

    #[inline]
    pub unsafe fn from_raw_none(pointer: *mut ffi::cairo_pattern_t) -> Pattern {
        unsafe {
            ffi::cairo_pattern_reference(pointer);
            Self::from_raw_full(pointer)
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(pointer: *mut ffi::cairo_pattern_t) -> Pattern {
        Self { pointer }
    }

    #[doc(alias = "cairo_pattern_get_type")]
    #[doc(alias = "get_type")]
    pub fn type_(&self) -> PatternType {
        unsafe { ffi::cairo_pattern_get_type(self.pointer).into() }
    }

    #[doc(alias = "cairo_pattern_get_reference_count")]
    #[doc(alias = "get_reference_count")]
    pub fn reference_count(&self) -> isize {
        unsafe { ffi::cairo_pattern_get_reference_count(self.pointer) as isize }
    }

    #[doc(alias = "cairo_pattern_set_extend")]
    pub fn set_extend(&self, extend: Extend) {
        unsafe { ffi::cairo_pattern_set_extend(self.pointer, extend.into()) }
    }

    #[doc(alias = "cairo_pattern_get_extend")]
    #[doc(alias = "get_extend")]
    pub fn extend(&self) -> Extend {
        unsafe { Extend::from(ffi::cairo_pattern_get_extend(self.pointer)) }
    }

    #[doc(alias = "cairo_pattern_set_filter")]
    pub fn set_filter(&self, filter: Filter) {
        unsafe { ffi::cairo_pattern_set_filter(self.pointer, filter.into()) }
    }

    #[doc(alias = "cairo_pattern_get_filter")]
    #[doc(alias = "get_filter")]
    pub fn filter(&self) -> Filter {
        unsafe { Filter::from(ffi::cairo_pattern_get_filter(self.pointer)) }
    }

    #[doc(alias = "cairo_pattern_set_matrix")]
    pub fn set_matrix(&self, matrix: Matrix) {
        unsafe { ffi::cairo_pattern_set_matrix(self.pointer, matrix.ptr()) }
    }

    #[doc(alias = "cairo_pattern_get_matrix")]
    #[doc(alias = "get_matrix")]
    pub fn matrix(&self) -> Matrix {
        let mut matrix = Matrix::null();
        unsafe {
            ffi::cairo_pattern_get_matrix(self.pointer, matrix.mut_ptr());
        }
        matrix
    }

    #[doc(alias = "cairo_pattern_status")]
    pub fn status(&self) -> Result<(), Error> {
        let status = unsafe { ffi::cairo_pattern_status(self.pointer) };
        status_to_result(status)
    }
}

impl Clone for Pattern {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            pointer: unsafe { ffi::cairo_pattern_reference(self.pointer) },
        }
    }
}

impl Drop for Pattern {
    #[inline]
    fn drop(&mut self) {
        unsafe { ffi::cairo_pattern_destroy(self.pointer) }
    }
}

impl AsRef<Pattern> for Pattern {
    #[inline]
    fn as_ref(&self) -> &Pattern {
        self
    }
}

macro_rules! convert {
    ($source: ident => $dest: ident = $( $variant: ident )|+ $( ($intermediate: ident) )*) => {
        impl TryFrom<$source> for $dest {
            type Error = $source;

            fn try_from(pattern: $source) -> Result<Self, $source> {
                if $( pattern.type_() == PatternType::$variant )||+ {
                    $(
                        let pattern = $intermediate(pattern);
                    )*
                    Ok($dest(pattern))
                }
                else {
                    Err(pattern)
                }
            }
        }
    };
}

macro_rules! pattern_type(
    //Signals without arguments
    ($pattern_type:ident $( = $variant: ident)*) => (

        #[derive(Debug, Clone)]
        pub struct $pattern_type(Pattern);

        impl Deref for $pattern_type {
            type Target = Pattern;

            #[inline]
            fn deref(&self) -> &Pattern {
                &self.0
            }
        }

        impl AsRef<Pattern> for $pattern_type {
            #[inline]
            fn as_ref(&self) -> &Pattern {
                &self.0
            }
        }

        $(
            convert!(Pattern => $pattern_type = $variant);
        )*
    );
);

pattern_type!(SolidPattern = Solid);

impl SolidPattern {
    #[doc(alias = "cairo_pattern_create_rgb")]
    pub fn from_rgb(red: f64, green: f64, blue: f64) -> Self {
        unsafe {
            Self(Pattern::from_raw_full(ffi::cairo_pattern_create_rgb(
                red, green, blue,
            )))
        }
    }

    #[doc(alias = "cairo_pattern_create_rgba")]
    pub fn from_rgba(red: f64, green: f64, blue: f64, alpha: f64) -> Self {
        unsafe {
            Self(Pattern::from_raw_full(ffi::cairo_pattern_create_rgba(
                red, green, blue, alpha,
            )))
        }
    }

    #[doc(alias = "cairo_pattern_get_rgba")]
    #[doc(alias = "get_rgba")]
    pub fn rgba(&self) -> Result<(f64, f64, f64, f64), Error> {
        unsafe {
            let mut red = 0.0;
            let mut green = 0.0;
            let mut blue = 0.0;
            let mut alpha = 0.0;

            let status = ffi::cairo_pattern_get_rgba(
                self.pointer,
                &mut red,
                &mut green,
                &mut blue,
                &mut alpha,
            );
            status_to_result(status)?;

            Ok((red, green, blue, alpha))
        }
    }
}

pattern_type!(Gradient);
convert!(Pattern => Gradient = LinearGradient | RadialGradient);

impl Gradient {
    #[doc(alias = "cairo_pattern_add_color_stop_rgb")]
    pub fn add_color_stop_rgb(&self, offset: f64, red: f64, green: f64, blue: f64) {
        unsafe { ffi::cairo_pattern_add_color_stop_rgb(self.pointer, offset, red, green, blue) }
    }

    #[doc(alias = "cairo_pattern_add_color_stop_rgba")]
    pub fn add_color_stop_rgba(&self, offset: f64, red: f64, green: f64, blue: f64, alpha: f64) {
        unsafe {
            ffi::cairo_pattern_add_color_stop_rgba(self.pointer, offset, red, green, blue, alpha)
        }
    }

    #[doc(alias = "cairo_pattern_get_color_stop_count")]
    #[doc(alias = "get_color_stop_count")]
    pub fn color_stop_count(&self) -> Result<isize, Error> {
        unsafe {
            let mut count = 0;
            let status = ffi::cairo_pattern_get_color_stop_count(self.pointer, &mut count);

            status_to_result(status)?;
            Ok(count as isize)
        }
    }

    #[doc(alias = "cairo_pattern_get_color_stop_rgba")]
    #[doc(alias = "get_color_stop_rgba")]
    pub fn color_stop_rgba(&self, index: isize) -> Result<(f64, f64, f64, f64, f64), Error> {
        unsafe {
            let mut offset = 0.0;
            let mut red = 0.0;
            let mut green = 0.0;
            let mut blue = 0.0;
            let mut alpha = 0.0;

            let status = ffi::cairo_pattern_get_color_stop_rgba(
                self.pointer,
                index as c_int,
                &mut offset,
                &mut red,
                &mut green,
                &mut blue,
                &mut alpha,
            );
            status_to_result(status)?;
            Ok((offset, red, green, blue, alpha))
        }
    }
}

macro_rules! gradient_type {
    ($gradient_type: ident) => {
        #[derive(Debug, Clone)]
        pub struct $gradient_type(Gradient);

        impl Deref for $gradient_type {
            type Target = Gradient;

            #[inline]
            fn deref(&self) -> &Gradient {
                &self.0
            }
        }

        impl AsRef<Gradient> for $gradient_type {
            #[inline]
            fn as_ref(&self) -> &Gradient {
                &self.0
            }
        }

        impl AsRef<Pattern> for $gradient_type {
            #[inline]
            fn as_ref(&self) -> &Pattern {
                &self.0
            }
        }

        convert!(Pattern => $gradient_type = $gradient_type (Gradient));
        convert!(Gradient => $gradient_type = $gradient_type);
    }
}

gradient_type!(LinearGradient);

impl LinearGradient {
    #[doc(alias = "cairo_pattern_create_linear")]
    pub fn new(x0: f64, y0: f64, x1: f64, y1: f64) -> Self {
        unsafe {
            Self(Gradient(Pattern::from_raw_full(
                ffi::cairo_pattern_create_linear(x0, y0, x1, y1),
            )))
        }
    }

    #[doc(alias = "cairo_pattern_get_linear_points")]
    #[doc(alias = "get_linear_points")]
    pub fn linear_points(&self) -> Result<(f64, f64, f64, f64), Error> {
        unsafe {
            let mut x0 = 0.0;
            let mut y0 = 0.0;
            let mut x1 = 0.0;
            let mut y1 = 0.0;

            let status = ffi::cairo_pattern_get_linear_points(
                self.pointer,
                &mut x0,
                &mut y0,
                &mut x1,
                &mut y1,
            );
            status_to_result(status)?;
            Ok((x0, y0, x1, y1))
        }
    }
}

gradient_type!(RadialGradient);

impl RadialGradient {
    #[doc(alias = "cairo_pattern_create_radial")]
    pub fn new(x0: f64, y0: f64, r0: f64, x1: f64, y1: f64, r1: f64) -> Self {
        unsafe {
            Self(Gradient(Pattern::from_raw_full(
                ffi::cairo_pattern_create_radial(x0, y0, r0, x1, y1, r1),
            )))
        }
    }

    #[doc(alias = "cairo_pattern_get_radial_circles")]
    #[doc(alias = "get_radial_circles")]
    pub fn radial_circles(&self) -> Result<(f64, f64, f64, f64, f64, f64), Error> {
        unsafe {
            let mut x0 = 0.0;
            let mut y0 = 0.0;
            let mut r0 = 0.0;
            let mut x1 = 0.0;
            let mut y1 = 0.0;
            let mut r1 = 0.0;

            let status = ffi::cairo_pattern_get_radial_circles(
                self.pointer,
                &mut x0,
                &mut y0,
                &mut r0,
                &mut x1,
                &mut y1,
                &mut r1,
            );
            status_to_result(status)?;
            Ok((x0, y0, r0, x1, y1, r1))
        }
    }
}

pattern_type!(SurfacePattern = Surface);

impl SurfacePattern {
    #[doc(alias = "cairo_pattern_create_for_surface")]
    pub fn create(surface: impl AsRef<Surface>) -> Self {
        unsafe {
            Self(Pattern::from_raw_full(
                ffi::cairo_pattern_create_for_surface(surface.as_ref().to_raw_none()),
            ))
        }
    }

    #[doc(alias = "cairo_pattern_get_surface")]
    #[doc(alias = "get_surface")]
    pub fn surface(&self) -> Result<Surface, Error> {
        unsafe {
            let mut surface_ptr: *mut ffi::cairo_surface_t = ptr::null_mut();
            let status = ffi::cairo_pattern_get_surface(self.pointer, &mut surface_ptr);
            status_to_result(status)?;
            Ok(Surface::from_raw_none(surface_ptr))
        }
    }
}

pattern_type!(Mesh = Mesh);

impl Mesh {
    #[doc(alias = "cairo_pattern_create_mesh")]
    pub fn new() -> Self {
        unsafe { Self(Pattern::from_raw_full(ffi::cairo_pattern_create_mesh())) }
    }

    #[doc(alias = "cairo_mesh_pattern_begin_patch")]
    pub fn begin_patch(&self) {
        unsafe { ffi::cairo_mesh_pattern_begin_patch(self.pointer) }
    }

    #[doc(alias = "cairo_mesh_pattern_end_patch")]
    pub fn end_patch(&self) {
        unsafe { ffi::cairo_mesh_pattern_end_patch(self.pointer) }
    }

    #[doc(alias = "cairo_mesh_pattern_move_to")]
    pub fn move_to(&self, x: f64, y: f64) {
        unsafe { ffi::cairo_mesh_pattern_move_to(self.pointer, x, y) }
    }

    #[doc(alias = "cairo_mesh_pattern_line_to")]
    pub fn line_to(&self, x: f64, y: f64) {
        unsafe { ffi::cairo_mesh_pattern_line_to(self.pointer, x, y) }
    }

    #[doc(alias = "cairo_mesh_pattern_curve_to")]
    pub fn curve_to(&self, x1: f64, y1: f64, x2: f64, y2: f64, x3: f64, y3: f64) {
        unsafe { ffi::cairo_mesh_pattern_curve_to(self.pointer, x1, y1, x2, y2, x3, y3) }
    }

    #[doc(alias = "cairo_mesh_pattern_set_control_point")]
    pub fn set_control_point(&self, corner: MeshCorner, x: f64, y: f64) {
        unsafe { ffi::cairo_mesh_pattern_set_control_point(self.pointer, corner.into(), x, y) }
    }

    #[doc(alias = "cairo_mesh_pattern_get_control_point")]
    #[doc(alias = "get_control_point")]
    pub fn control_point(&self, patch_num: usize, corner: MeshCorner) -> Result<(f64, f64), Error> {
        let mut x: c_double = 0.0;
        let mut y: c_double = 0.0;

        let status = unsafe {
            ffi::cairo_mesh_pattern_get_control_point(
                self.pointer,
                patch_num as c_uint,
                corner.into(),
                &mut x,
                &mut y,
            )
        };
        status_to_result(status)?;
        Ok((x, y))
    }

    #[doc(alias = "cairo_mesh_pattern_set_corner_color_rgb")]
    pub fn set_corner_color_rgb(&self, corner: MeshCorner, red: f64, green: f64, blue: f64) {
        unsafe {
            ffi::cairo_mesh_pattern_set_corner_color_rgb(
                self.pointer,
                corner.into(),
                red,
                green,
                blue,
            )
        }
    }

    #[doc(alias = "cairo_mesh_pattern_set_corner_color_rgba")]
    pub fn set_corner_color_rgba(
        &self,
        corner: MeshCorner,
        red: f64,
        green: f64,
        blue: f64,
        alpha: f64,
    ) {
        unsafe {
            ffi::cairo_mesh_pattern_set_corner_color_rgba(
                self.pointer,
                corner.into(),
                red,
                green,
                blue,
                alpha,
            )
        }
    }

    #[doc(alias = "cairo_mesh_pattern_get_corner_color_rgba")]
    #[doc(alias = "get_corner_color_rgba")]
    pub fn corner_color_rgba(
        &self,
        patch_num: usize,
        corner: MeshCorner,
    ) -> Result<(f64, f64, f64, f64), Error> {
        let mut red: c_double = 0.0;
        let mut green: c_double = 0.0;
        let mut blue: c_double = 0.0;
        let mut alpha: c_double = 0.0;

        let status = unsafe {
            ffi::cairo_mesh_pattern_get_corner_color_rgba(
                self.pointer,
                patch_num as c_uint,
                corner.into(),
                &mut red,
                &mut green,
                &mut blue,
                &mut alpha,
            )
        };
        status_to_result(status)?;
        Ok((red, green, blue, alpha))
    }

    #[doc(alias = "cairo_mesh_pattern_get_patch_count")]
    #[doc(alias = "get_patch_count")]
    pub fn patch_count(&self) -> Result<usize, Error> {
        let mut count: c_uint = 0;
        unsafe {
            let status = ffi::cairo_mesh_pattern_get_patch_count(self.pointer, &mut count);
            status_to_result(status)?;
        }
        Ok(count as usize)
    }

    #[doc(alias = "cairo_mesh_pattern_get_path")]
    #[doc(alias = "get_path")]
    pub fn path(&self, patch_num: usize) -> Result<Path, Error> {
        let path: Path = unsafe {
            Path::from_raw_full(ffi::cairo_mesh_pattern_get_path(
                self.pointer,
                patch_num as c_uint,
            ))
        };
        let status = unsafe {
            let ptr: *mut ffi::cairo_path_t = path.as_ptr();
            (*ptr).status
        };
        status_to_result(status)?;
        Ok(path)
    }
}

impl Default for Mesh {
    fn default() -> Self {
        Self::new()
    }
}

#[test]
fn try_from() {
    let linear = LinearGradient::new(0., 0., 1., 1.);
    let gradient = Gradient::clone(&linear);
    let pattern = Pattern::clone(&linear);
    assert!(Gradient::try_from(pattern.clone()).is_ok());
    assert!(LinearGradient::try_from(gradient).is_ok());
    assert!(LinearGradient::try_from(pattern).is_ok());
}
