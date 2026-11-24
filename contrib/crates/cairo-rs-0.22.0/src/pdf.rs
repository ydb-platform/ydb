// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    ffi::{CStr, CString},
    io, mem,
    ops::Deref,
    path::Path,
    ptr,
};

#[cfg(feature = "use_glib")]
use glib::translate::*;

use crate::{Error, PdfVersion, Surface, SurfaceType, ffi};
#[cfg(all(feature = "pdf", feature = "v1_16"))]
use crate::{PdfMetadata, PdfOutline};

impl PdfVersion {
    pub fn as_str(self) -> Option<&'static str> {
        unsafe {
            let res = ffi::cairo_pdf_version_to_string(self.into());
            res.as_ref()
                .and_then(|cstr| CStr::from_ptr(cstr as _).to_str().ok())
        }
    }
}

declare_surface!(PdfSurface, SurfaceType::Pdf);

impl PdfSurface {
    #[doc(alias = "cairo_pdf_surface_create")]
    pub fn new<P: AsRef<Path>>(width: f64, height: f64, path: P) -> Result<Self, Error> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let path = CString::new(path).unwrap();

        unsafe { Self::from_raw_full(ffi::cairo_pdf_surface_create(path.as_ptr(), width, height)) }
    }

    for_stream_constructors!(cairo_pdf_surface_create_for_stream);

    #[doc(alias = "cairo_pdf_get_versions")]
    #[doc(alias = "get_versions")]
    pub fn versions() -> impl Iterator<Item = PdfVersion> {
        let vers_slice = unsafe {
            let mut vers_ptr = ptr::null_mut();
            let mut num_vers = mem::MaybeUninit::uninit();
            ffi::cairo_pdf_get_versions(&mut vers_ptr, num_vers.as_mut_ptr());

            let num_vers = num_vers.assume_init();
            if num_vers == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(vers_ptr, num_vers as _)
            }
        };
        vers_slice.iter().map(|v| PdfVersion::from(*v))
    }

    #[doc(alias = "cairo_pdf_surface_restrict_to_version")]
    pub fn restrict(&self, version: PdfVersion) -> Result<(), Error> {
        unsafe {
            ffi::cairo_pdf_surface_restrict_to_version(self.0.to_raw_none(), version.into());
        }
        self.status()
    }

    #[doc(alias = "cairo_pdf_surface_set_size")]
    pub fn set_size(&self, width: f64, height: f64) -> Result<(), Error> {
        unsafe {
            ffi::cairo_pdf_surface_set_size(self.0.to_raw_none(), width, height);
        }
        self.status()
    }

    #[cfg(all(feature = "pdf", feature = "v1_16"))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "pdf", feature = "v1_16"))))]
    #[doc(alias = "cairo_pdf_surface_set_metadata")]
    pub fn set_metadata(&self, metadata: PdfMetadata, value: &str) -> Result<(), Error> {
        let value = CString::new(value).unwrap();
        unsafe {
            ffi::cairo_pdf_surface_set_metadata(
                self.0.to_raw_none(),
                metadata.into(),
                value.as_ptr(),
            );
        }
        self.status()
    }

    #[cfg(all(feature = "pdf", feature = "v1_18"))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "pdf", feature = "v1_18"))))]
    #[doc(alias = "cairo_pdf_surface_set_custom_metadata")]
    pub fn set_custom_metadata(&self, name: &str, value: &str) -> Result<(), Error> {
        let name = CString::new(name).unwrap();
        let value = CString::new(value).unwrap();
        unsafe {
            ffi::cairo_pdf_surface_set_custom_metadata(
                self.0.to_raw_none(),
                name.as_ptr(),
                value.as_ptr(),
            );
        }
        self.status()
    }

    #[cfg(all(feature = "pdf", feature = "v1_16"))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "pdf", feature = "v1_16"))))]
    #[doc(alias = "cairo_pdf_surface_set_page_label")]
    pub fn set_page_label(&self, label: &str) -> Result<(), Error> {
        let label = CString::new(label).unwrap();
        unsafe {
            ffi::cairo_pdf_surface_set_page_label(self.0.to_raw_none(), label.as_ptr());
        }
        self.status()
    }

    #[cfg(all(feature = "pdf", feature = "v1_16"))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "pdf", feature = "v1_16"))))]
    #[doc(alias = "cairo_pdf_surface_set_thumbnail_size")]
    pub fn set_thumbnail_size(&self, width: i32, height: i32) -> Result<(), Error> {
        unsafe {
            ffi::cairo_pdf_surface_set_thumbnail_size(
                self.0.to_raw_none(),
                width as _,
                height as _,
            );
        }
        self.status()
    }

    #[cfg(all(feature = "pdf", feature = "v1_16"))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "pdf", feature = "v1_16"))))]
    #[doc(alias = "cairo_pdf_surface_add_outline")]
    pub fn add_outline(
        &self,
        parent_id: i32,
        name: &str,
        link_attribs: &str,
        flags: PdfOutline,
    ) -> Result<i32, Error> {
        let name = CString::new(name).unwrap();
        let link_attribs = CString::new(link_attribs).unwrap();

        let res = unsafe {
            ffi::cairo_pdf_surface_add_outline(
                self.0.to_raw_none(),
                parent_id,
                name.as_ptr(),
                link_attribs.as_ptr(),
                flags.bits() as _,
            ) as _
        };

        self.status()?;
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempfile;

    use super::*;
    use crate::context::*;

    fn draw(surface: &Surface) {
        let cr = Context::new(surface).expect("Can't create a Cairo context");

        cr.set_line_width(25.0);

        cr.set_source_rgba(1.0, 0.0, 0.0, 0.5);
        cr.line_to(0., 0.);
        cr.line_to(100., 100.);
        cr.stroke().expect("Surface on an invalid state");

        cr.set_source_rgba(0.0, 0.0, 1.0, 0.5);
        cr.line_to(0., 100.);
        cr.line_to(100., 0.);
        cr.stroke().expect("Surface on an invalid state");
    }

    fn draw_in_buffer() -> Vec<u8> {
        let buffer: Vec<u8> = vec![];

        let surface = PdfSurface::for_stream(100., 100., buffer).unwrap();
        surface.restrict(PdfVersion::_1_5).unwrap();
        draw(&surface);
        *surface.finish_output_stream().unwrap().downcast().unwrap()
    }

    #[test]
    fn versions() {
        assert!(PdfSurface::versions().any(|v| v == PdfVersion::_1_4));
    }

    #[test]
    fn version_string() {
        let ver_str = PdfVersion::_1_4.as_str().unwrap();
        assert_eq!(ver_str, "PDF 1.4");
    }

    #[test]
    #[cfg(unix)]
    fn file() {
        let surface = PdfSurface::new(100., 100., "/dev/null").unwrap();
        draw(&surface);
        surface.finish();
    }

    #[test]
    fn writer() {
        let file = tempfile().expect("tempfile failed");
        let surface = PdfSurface::for_stream(100., 100., file).unwrap();

        draw(&surface);
        let stream = surface.finish_output_stream().unwrap();
        let file = stream.downcast::<std::fs::File>().unwrap();

        let buffer = draw_in_buffer();
        let file_size = file.metadata().unwrap().len();
        assert_eq!(file_size, buffer.len() as u64);
    }

    #[test]
    fn ref_writer() {
        let mut file = tempfile().expect("tempfile failed");
        let surface = unsafe { PdfSurface::for_raw_stream(100., 100., &mut file).unwrap() };

        draw(&surface);
        surface.finish_output_stream().unwrap();
        drop(file);
    }

    #[test]
    fn buffer() {
        let buffer = draw_in_buffer();

        let header = b"%PDF-1.5";
        assert_eq!(&buffer[..header.len()], header);
    }

    #[test]
    fn custom_writer() {
        struct CustomWriter(usize);

        impl io::Write for CustomWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0 += buf.len();
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let custom_writer = CustomWriter(0);

        let surface = PdfSurface::for_stream(20., 20., custom_writer).unwrap();
        surface.set_size(100., 100.).unwrap();
        draw(&surface);
        let stream = surface.finish_output_stream().unwrap();
        let custom_writer = stream.downcast::<CustomWriter>().unwrap();

        let buffer = draw_in_buffer();

        assert_eq!(custom_writer.0, buffer.len());
    }

    fn with_panicky_stream() -> PdfSurface {
        struct PanicWriter;

        impl io::Write for PanicWriter {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                panic!("panic in writer");
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let surface = PdfSurface::for_stream(20., 20., PanicWriter).unwrap();
        surface.finish();
        surface
    }

    #[test]
    #[should_panic]
    fn finish_stream_propagates_panic() {
        let _ = with_panicky_stream().finish_output_stream();
    }
}
