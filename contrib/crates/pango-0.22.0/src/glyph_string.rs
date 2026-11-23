// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{Slice, translate::*};

use crate::{GlyphInfo, GlyphString, ffi};

impl GlyphString {
    #[inline]
    pub fn num_glyphs(&self) -> i32 {
        unsafe { (*self.as_ptr()).num_glyphs }
    }

    #[inline]
    pub fn glyph_info(&self) -> &[GlyphInfo] {
        unsafe {
            let ptr = (*self.as_ptr()).glyphs;
            Slice::from_glib_borrow_num(ptr, self.num_glyphs() as usize)
        }
    }

    #[inline]
    pub fn glyph_info_mut(&mut self) -> &mut [GlyphInfo] {
        unsafe {
            let ptr = (*self.as_ptr()).glyphs;
            Slice::from_glib_borrow_num_mut(ptr, self.num_glyphs() as usize)
        }
    }

    #[inline]
    pub fn log_clusters(&self) -> &[i32] {
        unsafe {
            let ptr = (*self.as_ptr()).log_clusters as *const i32;
            Slice::from_glib_borrow_num(ptr, self.num_glyphs() as usize)
        }
    }

    #[inline]
    pub fn log_clusters_mut(&mut self) -> &mut [i32] {
        unsafe {
            let ptr = (*self.as_ptr()).log_clusters;
            Slice::from_glib_borrow_num_mut(ptr, self.num_glyphs() as usize)
        }
    }

    #[doc(alias = "pango_glyph_string_get_logical_widths")]
    #[doc(alias = "get_logical_widths")]
    pub fn logical_widths(&self, text: &str, rtl: bool) -> Vec<i32> {
        let count = text.chars().count();
        unsafe {
            let mut logical_widths = Vec::with_capacity(count);
            ffi::pango_glyph_string_get_logical_widths(
                mut_override(self.to_glib_none().0),
                text.as_ptr() as *const _,
                text.len().try_into().unwrap(),
                rtl as i32,
                logical_widths.as_mut_ptr(),
            );
            logical_widths.set_len(count);
            logical_widths
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn glyph_string_logical_widths() {
        const TXT: &str = "abcdefghijklmnopqrstuv";
        let mut s = super::GlyphString::new();
        s.set_size(TXT.len() as i32);
        for i in 0..TXT.len() {
            s.glyph_info_mut()[i].set_glyph(TXT.as_bytes()[i] as u32);
            s.glyph_info_mut()[i].geometry_mut().set_width(12);
            s.log_clusters_mut()[i] = i as i32;
        }
        let widths = s.logical_widths(TXT, false);
        println!("{widths:?}");
    }
}
