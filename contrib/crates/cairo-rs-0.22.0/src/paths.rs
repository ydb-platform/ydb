// Take a look at the license at the top of the repository in the LICENSE file.

use std::{iter::FusedIterator, ptr};

use crate::{PathDataType, ffi};

#[derive(Debug)]
#[doc(alias = "cairo_path_t")]
pub struct Path(ptr::NonNull<ffi::cairo_path_t>);

impl Path {
    #[inline]
    pub fn as_ptr(&self) -> *mut ffi::cairo_path_t {
        self.0.as_ptr()
    }

    #[inline]
    pub unsafe fn from_raw_full(pointer: *mut ffi::cairo_path_t) -> Path {
        unsafe {
            debug_assert!(!pointer.is_null());
            Path(ptr::NonNull::new_unchecked(pointer))
        }
    }

    pub fn iter(&self) -> PathSegments<'_> {
        use std::slice;

        unsafe {
            let ptr: *mut ffi::cairo_path_t = self.as_ptr();
            let length = (*ptr).num_data as usize;
            let data_ptr = (*ptr).data;
            let data_vec = if length != 0 && !data_ptr.is_null() {
                slice::from_raw_parts(data_ptr, length)
            } else {
                &[]
            };

            PathSegments {
                data: data_vec,
                i: 0,
                num_data: length,
            }
        }
    }
}

impl Drop for Path {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::cairo_path_destroy(self.as_ptr());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PathSegment {
    MoveTo((f64, f64)),
    LineTo((f64, f64)),
    CurveTo((f64, f64), (f64, f64), (f64, f64)),
    ClosePath,
}

pub struct PathSegments<'a> {
    data: &'a [ffi::cairo_path_data],
    i: usize,
    num_data: usize,
}

impl Iterator for PathSegments<'_> {
    type Item = PathSegment;

    fn next(&mut self) -> Option<PathSegment> {
        if self.i >= self.num_data {
            return None;
        }

        unsafe {
            let res = match PathDataType::from(self.data[self.i].header.data_type) {
                PathDataType::MoveTo => PathSegment::MoveTo(to_tuple(&self.data[self.i + 1].point)),
                PathDataType::LineTo => PathSegment::LineTo(to_tuple(&self.data[self.i + 1].point)),
                PathDataType::CurveTo => PathSegment::CurveTo(
                    to_tuple(&self.data[self.i + 1].point),
                    to_tuple(&self.data[self.i + 2].point),
                    to_tuple(&self.data[self.i + 3].point),
                ),
                PathDataType::ClosePath => PathSegment::ClosePath,
                PathDataType::__Unknown(x) => panic!("Unknown value: {x}"),
            };

            self.i += self.data[self.i].header.length as usize;

            Some(res)
        }
    }
}

impl FusedIterator for PathSegments<'_> {}

fn to_tuple(pair: &[f64; 2]) -> (f64, f64) {
    (pair[0], pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{context::*, enums::Format, image_surface::*};

    fn make_cr() -> Context {
        let surface = ImageSurface::create(Format::Rgb24, 1, 1).unwrap();

        Context::new(&surface).expect("Can't create a Cairo context")
    }

    fn assert_path_equals_segments(expected: &Path, actual: &[PathSegment]) {
        // First ensure the lengths are equal

        let expected_iter = expected.iter();
        let actual_iter = actual.iter();

        assert_eq!(expected_iter.count(), actual_iter.count());

        // Then actually compare the contents

        let expected_iter = expected.iter();
        let actual_iter = actual.iter();

        let iter = expected_iter.zip(actual_iter);
        for (e, a) in iter {
            assert_eq!(e, *a);
        }
    }

    #[test]
    fn empty_path_doesnt_iter() {
        let cr = make_cr();

        let path = cr.copy_path().expect("Invalid context");

        assert!(path.iter().next().is_none());
    }

    #[test]
    fn moveto() {
        let cr = make_cr();

        cr.move_to(1.0, 2.0);

        let path = cr.copy_path().expect("Invalid path");

        assert_path_equals_segments(&path, &[PathSegment::MoveTo((1.0, 2.0))]);
    }

    #[test]
    fn moveto_lineto_moveto() {
        let cr = make_cr();

        cr.move_to(1.0, 2.0);
        cr.line_to(3.0, 4.0);
        cr.move_to(5.0, 6.0);

        let path = cr.copy_path().expect("Invalid path");

        assert_path_equals_segments(
            &path,
            &[
                PathSegment::MoveTo((1.0, 2.0)),
                PathSegment::LineTo((3.0, 4.0)),
                PathSegment::MoveTo((5.0, 6.0)),
            ],
        );
    }

    #[test]
    fn moveto_closepath() {
        let cr = make_cr();

        cr.move_to(1.0, 2.0);
        cr.close_path();

        let path = cr.copy_path().expect("Invalid path");

        // Note that Cairo represents a close_path as closepath+moveto,
        // so that the next subpath will have a starting point,
        // from the extra moveto.
        assert_path_equals_segments(
            &path,
            &[
                PathSegment::MoveTo((1.0, 2.0)),
                PathSegment::ClosePath,
                PathSegment::MoveTo((1.0, 2.0)),
            ],
        );
    }
    #[test]
    fn curveto_closed_subpath_lineto() {
        let cr = make_cr();

        cr.move_to(1.0, 2.0);
        cr.curve_to(3.0, 4.0, 5.0, 6.0, 7.0, 8.0);
        cr.close_path();
        cr.line_to(9.0, 10.0);

        let path = cr.copy_path().expect("Invalid path");

        assert_path_equals_segments(
            &path,
            &[
                PathSegment::MoveTo((1.0, 2.0)),
                PathSegment::CurveTo((3.0, 4.0), (5.0, 6.0), (7.0, 8.0)),
                PathSegment::ClosePath,
                PathSegment::MoveTo((1.0, 2.0)),
                PathSegment::LineTo((9.0, 10.0)),
            ],
        );
    }
}
