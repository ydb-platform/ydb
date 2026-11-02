// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use std::fmt;

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc(alias = "cairo_text_cluster_t")]
pub struct TextCluster(ffi::cairo_text_cluster_t);

impl TextCluster {
    pub fn new(num_bytes: i32, num_glyphs: i32) -> Self {
        Self(ffi::cairo_text_cluster_t {
            num_bytes,
            num_glyphs,
        })
    }

    pub fn num_bytes(&self) -> i32 {
        self.0.num_bytes
    }

    pub fn num_glyphs(&self) -> i32 {
        self.0.num_glyphs
    }

    pub fn set_num_bytes(&mut self, num_bytes: i32) {
        self.0.num_bytes = num_bytes;
    }

    pub fn set_num_glyphs(&mut self, num_glyphs: i32) {
        self.0.num_glyphs = num_glyphs;
    }
}

impl fmt::Debug for TextCluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextCluster")
            .field("num_bytes", &self.num_bytes())
            .field("num_glyphs", &self.num_glyphs())
            .finish()
    }
}
