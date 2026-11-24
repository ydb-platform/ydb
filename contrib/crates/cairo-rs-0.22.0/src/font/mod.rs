// Take a look at the license at the top of the repository in the LICENSE file.

mod font_extents;
mod font_face;
mod font_options;
mod glyph;
mod scaled_font;
mod text_cluster;
mod text_extents;
mod user_fonts;

/* TODO
 Allocates an array of cairo_glyph_t's. This function is only useful in
 implementations of cairo_user_scaled_font_text_to_glyphs_func_t where the user
 needs to allocate an array of glyphs that cairo will free. For all other uses,
 user can use their own allocation method for glyphs.


impl Glyph {

    //pub fn cairo_glyph_allocate(num_glyphs: c_int) -> *Glyph;

    //pub fn cairo_glyph_free(glyphs: *Glyph);
}

 Allocates an array of cairo_glyph_t's. This function is only useful in
 implementations of cairo_user_scaled_font_text_to_glyphs_func_t where the user
 needs to allocate an array of glyphs that cairo will free. For all other uses,
 user can use their own allocation method for glyphs.

impl TextCluster {
    //pub fn cairo_text_cluster_allocate(num_clusters: c_int) -> *TextCluster;

    //pub fn cairo_text_cluster_free(clusters: *TextCluster);
}
*/
pub use self::{
    font_extents::FontExtents, font_face::FontFace, font_options::FontOptions, glyph::Glyph,
    scaled_font::ScaledFont, text_cluster::TextCluster, text_extents::TextExtents,
    user_fonts::UserFontFace,
};
pub use crate::enums::{
    Antialias, FontSlant, FontType, FontWeight, HintMetrics, HintStyle, SubpixelOrder,
    TextClusterFlags,
};
