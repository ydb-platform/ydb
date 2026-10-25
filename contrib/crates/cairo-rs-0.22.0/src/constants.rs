// Take a look at the license at the top of the repository in the LICENSE file.

pub const MIME_TYPE_JPEG: &str = "image/jpeg";
pub const MIME_TYPE_PNG: &str = "image/png";
pub const MIME_TYPE_JP2: &str = "image/jp2";
pub const MIME_TYPE_URI: &str = "text/x-uri";
pub const MIME_TYPE_UNIQUE_ID: &str = "application/x-cairo.uuid";
pub const MIME_TYPE_JBIG2: &str = "application/x-cairo.jbig2";
pub const MIME_TYPE_JBIG2_GLOBAL: &str = "application/x-cairo.jbig2-global";
pub const MIME_TYPE_JBIG2_GLOBAL_ID: &str = "application/x-cairo.jbig2-global-id";
#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const MIME_TYPE_CCITT_FAX: &str = "image/g3fax";
#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const MIME_TYPE_CCITT_FAX_PARAMS: &str = "application/x-cairo.ccitt.params";
#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const MIME_TYPE_EPS: &str = "application/postscript";
#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const MIME_TYPE_EPS_PARAMS: &str = "application/x-cairo.eps.params";

#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const PDF_OUTLINE_ROOT: i32 = 0;

#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const CAIRO_TAG_DEST: &str = "cairo.dest";
#[cfg(feature = "v1_16")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
pub const CAIRO_TAG_LINK: &str = "Link";
