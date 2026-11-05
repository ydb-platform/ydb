//! Main SVG document structure.

use data_url::mime::Mime;
use glib::prelude::*;
use markup5ever::QualName;
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::include_str;
use std::io::Cursor;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::{cell::RefCell, sync::OnceLock};

use crate::accept_language::UserLanguage;
use crate::bbox::BoundingBox;
use crate::borrow_element_as;
use crate::css::{self, Origin, Stylesheet};
use crate::dpi::Dpi;
use crate::drawing_ctx::{
    DrawingMode, RenderingConfiguration, SvgNesting, draw_tree, with_saved_cr,
};
use crate::error::{AcquireError, InternalRenderingError, LoadingError, NodeIdError};
use crate::io::{self, BinaryData};
use crate::is_element_of_type;
use crate::limits;
use crate::node::{CascadedValues, Node, NodeBorrow, NodeData};
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::structure::IntrinsicDimensions;
use crate::surface_utils::shared_surface::SharedImageSurface;
use crate::url_resolver::{AllowedUrl, UrlResolver};
use crate::xml::{Attributes, xml_load_from_possibly_compressed_stream};

/// Identifier of a node
#[derive(Debug, PartialEq, Clone)]
pub enum NodeId {
    /// element id
    Internal(String),
    /// url, element id
    External(String, String),
}

impl NodeId {
    pub fn parse(href: &str) -> Result<NodeId, NodeIdError> {
        let (url, id) = match href.rfind('#') {
            None => (Some(href), None),
            Some(0) => (None, Some(&href[1..])),
            Some(p) => (Some(&href[..p]), Some(&href[(p + 1)..])),
        };

        match (url, id) {
            (None, Some(id)) if !id.is_empty() => Ok(NodeId::Internal(String::from(id))),
            (Some(url), Some(id)) if !id.is_empty() => {
                Ok(NodeId::External(String::from(url), String::from(id)))
            }
            _ => Err(NodeIdError::NodeIdRequired),
        }
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeId::Internal(id) => write!(f, "#{id}"),
            NodeId::External(url, id) => write!(f, "{url}#{id}"),
        }
    }
}

/// Loading options for SVG documents.
pub struct LoadOptions {
    /// Load url resolver; all references will be resolved with respect to this.
    pub url_resolver: UrlResolver,

    /// Whether to turn off size limits in libxml2.
    pub unlimited_size: bool,

    /// Whether to keep original (undecoded) image data to embed in Cairo PDF surfaces.
    pub keep_image_data: bool,
}

impl LoadOptions {
    /// Creates a `LoadOptions` with defaults, and sets the `url resolver`.
    pub fn new(url_resolver: UrlResolver) -> Self {
        LoadOptions {
            url_resolver,
            unlimited_size: false,
            keep_image_data: false,
        }
    }

    /// Sets whether libxml2's limits on memory usage should be turned off.
    ///
    /// This should only be done for trusted data.
    pub fn with_unlimited_size(mut self, unlimited: bool) -> Self {
        self.unlimited_size = unlimited;
        self
    }

    /// Sets whether to keep the original compressed image data from referenced JPEG/PNG images.
    ///
    /// This is only useful for rendering to Cairo PDF
    /// surfaces, which can embed the original, compressed image data instead of uncompressed
    /// RGB buffers.
    pub fn keep_image_data(mut self, keep: bool) -> Self {
        self.keep_image_data = keep;
        self
    }

    /// Creates a new `LoadOptions` with a different `url resolver`.
    ///
    /// This is used when loading a referenced file that may in turn cause other files
    /// to be loaded, for example `<image xlink:href="subimage.svg"/>`
    pub fn copy_with_base_url(&self, base_url: &AllowedUrl) -> Self {
        let mut url_resolver = self.url_resolver.clone();
        url_resolver.base_url = Some((**base_url).clone());

        LoadOptions {
            url_resolver,
            unlimited_size: self.unlimited_size,
            keep_image_data: self.keep_image_data,
        }
    }
}

/// Document-level rendering options.
///
/// This gets then converted to a [`RenderingConfiguration`] when all the parameters are
/// known.
pub struct RenderingOptions {
    pub dpi: Dpi,
    pub cancellable: Option<gio::Cancellable>,
    pub user_language: UserLanguage,
    pub svg_nesting: SvgNesting,
    pub testing: bool,
}

impl RenderingOptions {
    /// Copies the options to a [`RenderingConfiguration`], and adds the `measuring` flag.
    fn to_rendering_configuration(&self, measuring: bool) -> RenderingConfiguration {
        RenderingConfiguration {
            dpi: self.dpi,
            cancellable: self.cancellable.clone(),
            user_language: self.user_language.clone(),
            svg_nesting: self.svg_nesting,
            testing: self.testing,
            measuring,
        }
    }
}

/// A loaded SVG file and its derived data.
pub struct Document {
    /// Tree of nodes; the root is guaranteed to be an `<svg>` element.
    ///
    /// This is inside a [`RefCell`] because when cascading lazily, we
    /// need a mutable tree.
    tree: RefCell<Node>,

    /// Metadata about the SVG handle.
    session: Session,

    /// Mapping from `id` attributes to nodes.
    ids: HashMap<String, Node>,

    /// Othewr SVG documents and images referenced from this document.
    ///
    /// This requires requires interior mutability because we load resources all over the
    /// place.  Eventually we'll be able to do this once, at loading time, and keep this
    /// immutable.
    resources: RefCell<Resources>,

    /// Used to load referenced resources.
    load_options: Arc<LoadOptions>,

    /// Stylesheets defined in the document.
    stylesheets: Vec<Stylesheet>,

    /// Whether there's a pending cascade operation.
    ///
    /// The document starts un-cascaded and with this flag turned on,
    /// to avoid a double cascade if
    /// [`crate::SvgHandle::set_stylesheet`] is called after loading
    /// the document.
    needs_cascade: Cell<bool>,
}

impl Document {
    /// Constructs a `Document` by loading it from a stream.
    ///
    /// Note that the document is **not** cascaded just after loading.  Cascading is done lazily;
    /// call [`Document::ensure_is_cascaded`] if you need a cascaded tree of elements.
    pub fn load_from_stream(
        session: Session,
        load_options: Arc<LoadOptions>,
        stream: &gio::InputStream,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<Document, LoadingError> {
        xml_load_from_possibly_compressed_stream(
            session.clone(),
            DocumentBuilder::new(session, load_options.clone()),
            load_options,
            stream,
            cancellable,
        )
    }

    /// Utility function to load a document from a static string in tests.
    #[cfg(test)]
    pub fn load_from_bytes(input: &'static [u8]) -> Document {
        let bytes = glib::Bytes::from_static(input);
        let stream = gio::MemoryInputStream::from_bytes(&bytes);

        let session = Session::new_for_test_suite();

        let document = Document::load_from_stream(
            session.clone(),
            Arc::new(LoadOptions::new(UrlResolver::new(None))),
            &stream.upcast(),
            None::<&gio::Cancellable>,
        )
        .unwrap();

        document.ensure_is_cascaded();

        document
    }

    /// Gets the root node.  This is guaranteed to be an `<svg>` element.
    pub fn root(&self) -> Node {
        self.tree.borrow().clone()
    }

    /// Looks up a node in this document or one of its resources by its `id` attribute.
    fn lookup_node(
        &self,
        node_id: &NodeId,
        cancellable: Option<&gio::Cancellable>,
    ) -> Option<Node> {
        match node_id {
            NodeId::Internal(id) => self.lookup_internal_node(id),
            NodeId::External(url, id) => self
                .resources
                .borrow_mut()
                .lookup_node(&self.session, &self.load_options, url, id, cancellable)
                .ok(),
        }
    }

    /// Looks up a node in this document by its `id` attribute.
    pub fn lookup_internal_node(&self, id: &str) -> Option<Node> {
        self.ids.get(id).map(|n| (*n).clone())
    }

    /// Loads a resource by URL, or returns a pre-loaded one.
    fn lookup_resource(
        &self,
        url: &str,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<Resource, LoadingError> {
        let aurl = self
            .load_options
            .url_resolver
            .resolve_href(url)
            .map_err(|_| LoadingError::BadUrl)?;

        self.resources.borrow_mut().lookup_resource(
            &self.session,
            &self.load_options,
            &aurl,
            cancellable,
        )
    }

    /// Runs the CSS cascade on the document tree
    ///
    /// This uses the default UserAgent stylesheet, the document's internal stylesheets,
    /// plus an extra set of stylesheets supplied by the caller.
    pub fn cascade(&self, extra: &[Stylesheet]) {
        self.needs_cascade.set(false);

        let stylesheets = {
            static UA_STYLESHEETS: OnceLock<Vec<Stylesheet>> = OnceLock::new();
            UA_STYLESHEETS.get_or_init(|| {
                vec![
                    Stylesheet::from_data(
                        include_str!("ua.css"),
                        &UrlResolver::new(None),
                        Origin::UserAgent,
                        Session::default(),
                    )
                    .expect("could not parse user agent stylesheet for librsvg, there's a bug!"),
                ]
            })
        };
        css::cascade(
            &mut self.tree.borrow_mut(),
            stylesheets,
            &self.stylesheets,
            extra,
            &self.session,
        );
    }

    pub fn get_intrinsic_dimensions(&self) -> IntrinsicDimensions {
        self.ensure_is_cascaded();

        let root = self.root();
        let cascaded = CascadedValues::new_from_node(&root);
        let values = cascaded.get();
        borrow_element_as!(self.root(), Svg).get_intrinsic_dimensions(values)
    }

    pub fn render_document(
        &self,
        cr: &cairo::Context,
        viewport: &cairo::Rectangle,
        options: &RenderingOptions,
    ) -> Result<(), InternalRenderingError> {
        let root = self.root();
        self.render_layer(cr, root, viewport, options)
    }

    pub fn render_layer(
        &self,
        cr: &cairo::Context,
        node: Node,
        viewport: &cairo::Rectangle,
        options: &RenderingOptions,
    ) -> Result<(), InternalRenderingError> {
        cr.status()?;

        let root = self.root();

        let viewport = Rect::from(*viewport);

        let config = options.to_rendering_configuration(false);

        with_saved_cr(cr, || {
            self.draw_tree(
                DrawingMode::LimitToStack { node, root },
                cr,
                viewport,
                config,
            )
        })
        .map(|_bbox| ())
        .map_err(|err| *err)
    }

    fn geometry_for_layer(
        &self,
        node: Node,
        viewport: Rect,
        options: &RenderingOptions,
    ) -> Result<(Rect, Rect), Box<InternalRenderingError>> {
        let root = self.root();

        let target = cairo::ImageSurface::create(cairo::Format::Rgb24, 1, 1)?;
        let cr = cairo::Context::new(&target)?;

        let config = options.to_rendering_configuration(true);

        let bbox = self.draw_tree(
            DrawingMode::LimitToStack { node, root },
            &cr,
            viewport,
            config,
        )?;

        let ink_rect = bbox.ink_rect.unwrap_or_default();
        let logical_rect = bbox.rect.unwrap_or_default();

        Ok((ink_rect, logical_rect))
    }

    pub fn get_geometry_for_layer(
        &self,
        node: Node,
        viewport: &cairo::Rectangle,
        options: &RenderingOptions,
    ) -> Result<(cairo::Rectangle, cairo::Rectangle), InternalRenderingError> {
        let viewport = Rect::from(*viewport);

        let (ink_rect, logical_rect) = self
            .geometry_for_layer(node, viewport, options)
            .map_err(|err| *err)?;

        Ok((
            cairo::Rectangle::from(ink_rect),
            cairo::Rectangle::from(logical_rect),
        ))
    }

    fn get_bbox_for_element(
        &self,
        node: &Node,
        options: &RenderingOptions,
    ) -> Result<BoundingBox, InternalRenderingError> {
        let target = cairo::ImageSurface::create(cairo::Format::Rgb24, 1, 1)?;
        let cr = cairo::Context::new(&target)?;

        let node = node.clone();

        let config = options.to_rendering_configuration(true);

        self.draw_tree(DrawingMode::OnlyNode(node), &cr, unit_rectangle(), config)
            .map_err(|err| *err)
    }

    /// Returns (ink_rect, logical_rect)
    pub fn get_geometry_for_element(
        &self,
        node: Node,
        options: &RenderingOptions,
    ) -> Result<(cairo::Rectangle, cairo::Rectangle), InternalRenderingError> {
        let bbox = self.get_bbox_for_element(&node, options)?;

        let ink_rect = bbox.ink_rect.unwrap_or_default();
        let logical_rect = bbox.rect.unwrap_or_default();

        // Translate so ink_rect is always at offset (0, 0)
        let ofs = (-ink_rect.x0, -ink_rect.y0);

        Ok((
            cairo::Rectangle::from(ink_rect.translate(ofs)),
            cairo::Rectangle::from(logical_rect.translate(ofs)),
        ))
    }

    pub fn render_element(
        &self,
        cr: &cairo::Context,
        node: Node,
        element_viewport: &cairo::Rectangle,
        options: &RenderingOptions,
    ) -> Result<(), InternalRenderingError> {
        cr.status()?;

        let bbox = self.get_bbox_for_element(&node, options)?;

        if bbox.ink_rect.is_none() || bbox.rect.is_none() {
            // Nothing to draw
            return Ok(());
        }

        let ink_r = bbox.ink_rect.unwrap_or_default();

        if ink_r.is_empty() {
            return Ok(());
        }

        // Render, transforming so element is at the new viewport's origin

        with_saved_cr(cr, || {
            let factor = (element_viewport.width() / ink_r.width())
                .min(element_viewport.height() / ink_r.height());

            cr.translate(element_viewport.x(), element_viewport.y());
            cr.scale(factor, factor);
            cr.translate(-ink_r.x0, -ink_r.y0);

            let config = options.to_rendering_configuration(false);

            self.draw_tree(DrawingMode::OnlyNode(node), cr, unit_rectangle(), config)
        })
        .map(|_bbox| ())
        .map_err(|err| *err)
    }

    /// Wrapper for [`draw_tree`].  This just ensures that the document
    /// is cascaded before rendering.
    fn draw_tree(
        &self,
        drawing_mode: DrawingMode,
        cr: &cairo::Context,
        viewport_rect: Rect,
        config: RenderingConfiguration,
    ) -> Result<BoundingBox, Box<InternalRenderingError>> {
        self.ensure_is_cascaded();

        let cancellable = config.cancellable.clone();

        draw_tree(
            self.session.clone(),
            drawing_mode,
            cr,
            viewport_rect,
            config,
            &mut AcquiredNodes::new(self, cancellable),
        )
        .map(|boxed_bbox| *boxed_bbox)
    }

    fn ensure_is_cascaded(&self) {
        if self.needs_cascade.get() {
            self.cascade(&[]);
        }
    }
}

fn unit_rectangle() -> Rect {
    Rect::from_size(1.0, 1.0)
}

/// Any kind of resource loaded while processing an SVG document: images, or SVGs themselves.
#[derive(Clone)]
pub enum Resource {
    Document(Rc<Document>),
    Image(SharedImageSurface),
}

/// Set of external resources (other SVG documents, or raster images) referenced by an SVG.
///
/// For example, a PNG image in `<image href="foo.png"/>` gets decoded
/// and stored here, referenced by its URL.
struct Resources {
    resources: HashMap<AllowedUrl, Result<Resource, LoadingError>>,
}

impl Resources {
    fn new() -> Resources {
        Resources {
            resources: Default::default(),
        }
    }

    /// Looks up a specific node by its id in another SVG document.
    ///
    /// For example, in `<use href="foo.svg#some_node"/>`, or in `filter="url(filters.svg#foo)"`.
    ///
    /// The URL is not validated yet; this function will take care of that and return a
    /// suitable error.
    fn lookup_node(
        &mut self,
        session: &Session,
        load_options: &LoadOptions,
        url: &str,
        id: &str,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<Node, LoadingError> {
        self.get_extern_document(session, load_options, url, cancellable)
            .and_then(|resource| match resource {
                Resource::Document(doc) => doc.lookup_internal_node(id).ok_or(LoadingError::BadUrl),
                _ => unreachable!("get_extern_document() should already have ensured the document"),
            })
    }

    /// Validates the URL and loads an SVG document as a [`Resource`].
    ///
    /// The document can then be used whole (`<image href="foo.svg"/>`, or individual
    /// elements from it can be looked up (`<use href="foo.svg#some_node"/>`).
    fn get_extern_document(
        &mut self,
        session: &Session,
        load_options: &LoadOptions,
        href: &str,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<Resource, LoadingError> {
        let aurl = load_options
            .url_resolver
            .resolve_href(href)
            .map_err(|_| LoadingError::BadUrl)?;

        let resource = self.lookup_resource(session, load_options, &aurl, cancellable)?;

        match resource {
            Resource::Document(_) => Ok(resource),
            _ => Err(LoadingError::Other(format!(
                "{href} is not an SVG document"
            ))),
        }
    }

    /// Loads a resource (an SVG document or a raster image), or returns an already-loaded one.
    fn lookup_resource(
        &mut self,
        session: &Session,
        load_options: &LoadOptions,
        aurl: &AllowedUrl,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<Resource, LoadingError> {
        match self.resources.entry(aurl.clone()) {
            Entry::Occupied(e) => e.get().clone(),

            Entry::Vacant(e) => {
                let resource_result = load_resource(session, load_options, aurl, cancellable);
                e.insert(resource_result.clone());
                resource_result
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ResourceType {
    Unknown,
    Svg,
    Png,
    Jpeg,
    Gif,
    WebP,
    Avif,
}

// Compares the fields of `Mime`, but ignores its `parameters`
fn is_mime_type(mime: &Mime, type_: &str, subtype: &str) -> bool {
    mime.type_ == type_ && mime.subtype == subtype
}

impl ResourceType {
    fn from(mime_type: &Option<Mime>) -> ResourceType {
        match mime_type {
            None => ResourceType::Unknown,

            // See issue #548 - data: URLs without a MIME-type automatically
            // fall back to "text/plain;charset=US-ASCII".  Some (old?) versions of
            // Adobe Illustrator generate data: URLs without MIME-type for image
            // data.  We'll catch this and fall back to sniffing by unsetting the
            // content_type.
            Some(x) if *x == Mime::from_str("text/plain;charset=US-ASCII").unwrap() => {
                ResourceType::Unknown
            }

            Some(x) if is_mime_type(x, "image", "svg+xml") => ResourceType::Svg,
            Some(x) if is_mime_type(x, "image", "png") => ResourceType::Png,
            Some(x) if is_mime_type(x, "image", "jpeg") => ResourceType::Jpeg,
            Some(x) if is_mime_type(x, "image", "gif") => ResourceType::Gif,
            Some(x) if is_mime_type(x, "image", "webp") => ResourceType::WebP,
            Some(x) if is_mime_type(x, "image", "avif") => ResourceType::Avif,

            _ => ResourceType::Unknown,
        }
    }

    fn is_known(&self) -> bool {
        !matches!(*self, ResourceType::Unknown)
    }

    fn to_image_format(self) -> image::ImageFormat {
        use ResourceType::*;

        match self {
            Svg => unreachable!(),

            Png => image::ImageFormat::Png,
            Jpeg => image::ImageFormat::Jpeg,
            Gif => image::ImageFormat::Gif,
            WebP => image::ImageFormat::WebP,
            Avif => image::ImageFormat::Avif,

            _ => unreachable!(),
        }
    }
}

/// Loads the entire contents of a URL, sniffs them, and decodes them as a [`Resource`]
/// for an SVG or raster image.
///
/// Assumes that `gio`'s content-sniffing machinery is working correctly.  Anything that
/// doesn't sniff like an SVG document will be decoded as a raster image.
///
/// This handles `data:` URLs correctly, by decoding them into binary data, and then
/// sniffing it or using the declared MIME type in the `data:` URL itself.
fn load_resource(
    session: &Session,
    load_options: &LoadOptions,
    aurl: &AllowedUrl,
    cancellable: Option<&gio::Cancellable>,
) -> Result<Resource, LoadingError> {
    let data = io::acquire_data(aurl, cancellable)?;

    let BinaryData {
        data: bytes,
        mime_type,
    } = data;

    let resource_type = ResourceType::from(&mime_type);

    if resource_type.is_known() {
        use ResourceType::*;

        match resource_type {
            Png | Jpeg | Gif | WebP | Avif => {
                let format = resource_type.to_image_format();
                load_image_resource_from_bytes(load_options, aurl, bytes, format)
            }
            Svg => load_svg_resource_from_bytes(session, load_options, aurl, bytes, cancellable),
            _ => unreachable!(),
        }
    } else {
        // We don't know the MIME type of the data.  Sniff it, hand off raster images to the image crate,
        // or else fall back to trying to load as an SVG.

        if let Ok(format) = image::guess_format(&bytes) {
            load_image_resource_from_bytes(load_options, aurl, bytes, format)
        } else {
            load_svg_resource_from_bytes(session, load_options, aurl, bytes, cancellable)
        }
    }
}

/// Parses [`BinaryData`] that is known to be an SVG document, using librsvg itself.
fn load_svg_resource_from_bytes(
    session: &Session,
    load_options: &LoadOptions,
    aurl: &AllowedUrl,
    input_bytes: Vec<u8>,
    cancellable: Option<&gio::Cancellable>,
) -> Result<Resource, LoadingError> {
    let bytes = glib::Bytes::from_owned(input_bytes);
    let stream = gio::MemoryInputStream::from_bytes(&bytes);

    let document = Document::load_from_stream(
        session.clone(),
        Arc::new(load_options.copy_with_base_url(aurl)),
        &stream.upcast(),
        cancellable,
    )?;

    document.ensure_is_cascaded();

    Ok(Resource::Document(Rc::new(document)))
}

/// Decodes [`BinaryData`] that is presumed to be a raster image.
///
/// To know which decoder to use (or to even decide if this is a supported image format),
/// this function uses the `mime_type` field in the [`BinaryData`].
///
/// The [`AllowedUrl`] is not used for decoding; it is just to construct an error message
/// for the return value.
fn load_image_resource_from_bytes(
    load_options: &LoadOptions,
    aurl: &AllowedUrl,
    bytes: Vec<u8>,
    format: image::ImageFormat,
) -> Result<Resource, LoadingError> {
    if bytes.is_empty() {
        return Err(LoadingError::Other(String::from("no image data")));
    }

    load_image_with_image_rs(aurl, bytes, format, load_options)
}

/// Decides whether the specified MIME type is supported as a raster image format.
///
/// Librsvg explicitly only supports PNG/JPEG/GIF/WEBP, and AVIF optionally.  See the
/// documentation on [supported raster image formats][formats] for details.
///
/// [formats]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/features.html#supported-raster-image-formats
fn is_supported_image_format(format: image::ImageFormat) -> bool {
    use image::ImageFormat::*;

    match format {
        Png => true,
        Jpeg => true,
        Gif => true,
        WebP => true,

        #[cfg(feature = "avif")]
        Avif => true,

        _ => false,
    }
}

fn image_format_to_content_type(format: image::ImageFormat) -> String {
    use image::ImageFormat::*;

    let mime_type = match format {
        Png => "image/png",
        Jpeg => "image/jpeg",
        Gif => "image/gif",
        WebP => "image/webp",
        Avif => "image/avif",
        _ => unreachable!("we should have already filtered supported image types"),
    };

    mime_type.to_string()
}

fn load_image_with_image_rs(
    aurl: &AllowedUrl,
    bytes: Vec<u8>,
    format: image::ImageFormat,
    load_options: &LoadOptions,
) -> Result<Resource, LoadingError> {
    if is_supported_image_format(format) {
        let cursor = Cursor::new(&bytes);

        let reader = image::ImageReader::with_format(cursor, format);
        let image = reader
            .decode()
            .map_err(|e| LoadingError::Other(format!("error decoding image: {e}")))?;

        let bytes = if load_options.keep_image_data {
            Some(bytes)
        } else {
            None
        };

        let content_type = image_format_to_content_type(format);

        let surface = SharedImageSurface::from_image(&image, Some(&content_type), bytes)
            .map_err(|e| image_loading_error_from_cairo(e, aurl))?;

        Ok(Resource::Image(surface))
    } else {
        Err(LoadingError::Other(String::from(
            "unsupported image format {format:?}",
        )))
    }
}

/// Formats a URL for human consumption, as in error messages.  This is to
/// reduce very long `data:` URLs to an abbreviated version.
fn human_readable_url(aurl: &AllowedUrl) -> &str {
    if aurl.scheme() == "data" {
        // avoid printing a huge data: URL for image data
        "data URL"
    } else {
        aurl.as_ref()
    }
}

/// Converts a `cairo::Error` that happened while wrapping a decoded raster image
/// into a `LoadingError` augmented with the image's URL.
fn image_loading_error_from_cairo(status: cairo::Error, aurl: &AllowedUrl) -> LoadingError {
    let url = human_readable_url(aurl);

    match status {
        cairo::Error::NoMemory => LoadingError::OutOfMemory(format!("loading image: {url}")),
        cairo::Error::InvalidSize => LoadingError::Other(format!("image too big: {url}")),
        _ => LoadingError::Other(format!("cairo error: {status}")),
    }
}

pub struct AcquiredNode {
    stack: Option<Rc<RefCell<NodeStack>>>,
    node: Node,
}

impl Drop for AcquiredNode {
    fn drop(&mut self) {
        if let Some(ref stack) = self.stack {
            let mut stack = stack.borrow_mut();
            let last = stack.pop().unwrap();
            assert!(last == self.node);
        }
    }
}

impl AcquiredNode {
    pub fn get(&self) -> &Node {
        &self.node
    }
}

/// Detects circular references between nodes, and enforces referencing limits.
///
/// Consider this fragment of SVG:
///
/// ```xml
/// <pattern id="foo">
///   <rect width="1" height="1" fill="url(#foo)"/>
/// </pattern>
/// ```
///
/// The pattern has a child element that references the pattern itself.  This kind of circular
/// reference is invalid.  The `AcquiredNodes` struct is passed around
/// wherever it may be necessary to resolve references to nodes, or to access nodes
/// "elsewhere" in the DOM that is not the current subtree.
///
/// Also, such constructs that reference other elements can be maliciously arranged like
/// in the [billion laughs attack][lol], to cause huge amounts of CPU to be consumed through
/// creating an exponential number of references.  `AcquiredNodes` imposes a hard limit on
/// the number of references that can be resolved for typical, well-behaved SVG documents.
///
/// The [`Self::acquire()`] and [`Self::acquire_ref()`] methods return an [`AcquiredNode`], which
/// acts like a smart pointer for a [`Node`].  Once a node has been acquired, it cannot be
/// acquired again until its [`AcquiredNode`] is dropped.  In the example above, a graphic element
/// would acquire the `pattern`, which would then acquire its `rect` child, which then would fail
/// to re-acquired the `pattern` — thus signaling a circular reference.
///
/// Those methods return an [`AcquireError`] to signal circular references.  Also, they
/// can return [`AcquireError::MaxReferencesExceeded`] if the aforementioned referencing limit
/// is exceeded.
///
/// [lol]: https://bitbucket.org/tiran/defusedxml
pub struct AcquiredNodes<'i> {
    document: &'i Document,
    num_elements_acquired: usize,
    node_stack: Rc<RefCell<NodeStack>>,
    nodes_with_cycles: Vec<Node>,
    cancellable: Option<gio::Cancellable>,
}

impl<'i> AcquiredNodes<'i> {
    pub fn new(document: &Document, cancellable: Option<gio::Cancellable>) -> AcquiredNodes<'_> {
        AcquiredNodes {
            document,
            num_elements_acquired: 0,
            node_stack: Rc::new(RefCell::new(NodeStack::new())),
            nodes_with_cycles: Vec::new(),
            cancellable,
        }
    }

    pub fn lookup_resource(&self, url: &str) -> Result<Resource, LoadingError> {
        self.document
            .lookup_resource(url, self.cancellable.as_ref())
    }

    /// Acquires a node by its id.
    ///
    /// This is typically used during an "early resolution" stage, when XML `id`s are being
    /// resolved to node references.
    pub fn acquire(&mut self, node_id: &NodeId) -> Result<AcquiredNode, AcquireError> {
        self.num_elements_acquired += 1;

        // This is a mitigation for SVG files that try to instance a huge number of
        // elements via <use>, recursive patterns, etc.  See limits.rs for details.
        if self.num_elements_acquired > limits::MAX_REFERENCED_ELEMENTS {
            return Err(AcquireError::MaxReferencesExceeded);
        }

        // FIXME: callers shouldn't have to know that get_node() can initiate a file load.
        // Maybe we should have the following stages:
        //   - load main SVG XML
        //
        //   - load secondary resources: SVG XML and other files like images
        //
        //   - Now that all files are loaded, resolve URL references
        let node = self
            .document
            .lookup_node(node_id, self.cancellable.as_ref())
            .ok_or_else(|| AcquireError::LinkNotFound(node_id.clone()))?;

        if self.nodes_with_cycles.contains(&node) {
            return Err(AcquireError::CircularReference(node.clone()));
        }

        if node.borrow_element().is_accessed_by_reference() {
            self.acquire_ref(&node)
        } else {
            Ok(AcquiredNode { stack: None, node })
        }
    }

    /// Acquires a node whose reference is already known.
    ///
    /// This is useful for cases where a node is initially referenced by its id with
    /// [`Self::acquire`] and kept around for later use.  During the later use, the node
    /// needs to be re-acquired with this method.  For example:
    ///
    /// * At an "early resolution" stage, `acquire()` a pattern by its id, and keep around its
    ///   [`Node`] reference.
    ///
    /// * At the drawing stage, `acquire_ref()` the pattern node that we already had, so that
    ///   its child elements that reference other paint servers will be able to detect circular
    ///   references to the pattern.
    pub fn acquire_ref(&mut self, node: &Node) -> Result<AcquiredNode, AcquireError> {
        if self.nodes_with_cycles.contains(node) {
            Err(AcquireError::CircularReference(node.clone()))
        } else if self.node_stack.borrow().contains(node) {
            self.nodes_with_cycles.push(node.clone());
            Err(AcquireError::CircularReference(node.clone()))
        } else {
            self.node_stack.borrow_mut().push(node);
            Ok(AcquiredNode {
                stack: Some(self.node_stack.clone()),
                node: node.clone(),
            })
        }
    }
}

/// Keeps a stack of nodes and can check if a certain node is contained in the stack
///
/// Sometimes parts of the code cannot plainly use the implicit stack of acquired
/// nodes as maintained by DrawingCtx::acquire_node(), and they must keep their
/// own stack of nodes to test for reference cycles.  NodeStack can be used to do that.
pub struct NodeStack(Vec<Node>);

impl NodeStack {
    pub fn new() -> NodeStack {
        NodeStack(Vec::new())
    }

    pub fn push(&mut self, node: &Node) {
        self.0.push(node.clone());
    }

    pub fn pop(&mut self) -> Option<Node> {
        self.0.pop()
    }

    pub fn contains(&self, node: &Node) -> bool {
        self.0.contains(node)
    }
}

/// Used to build a tree of SVG nodes while an XML document is being read.
///
/// This struct holds the document-related state while loading an SVG document from XML:
/// the loading options, the partially-built tree of nodes, the CSS stylesheets that
/// appear while loading the document.
///
/// The XML loader asks a `DocumentBuilder` to
/// [`append_element`][DocumentBuilder::append_element],
/// [`append_characters`][DocumentBuilder::append_characters], etc.  When all the XML has
/// been consumed, the caller can use [`build`][DocumentBuilder::build] to get a
/// fully-loaded [`Document`].
pub struct DocumentBuilder {
    /// Metadata for the document's lifetime.
    session: Session,

    /// Loading options; mainly the URL resolver.
    load_options: Arc<LoadOptions>,

    /// Root node of the tree.
    tree: Option<Node>,

    /// Mapping from `id` attributes to nodes.
    ids: HashMap<String, Node>,

    /// Stylesheets defined in the document.
    stylesheets: Vec<Stylesheet>,
}

impl DocumentBuilder {
    pub fn new(session: Session, load_options: Arc<LoadOptions>) -> DocumentBuilder {
        DocumentBuilder {
            session,
            load_options,
            tree: None,
            ids: HashMap::new(),
            stylesheets: Vec::new(),
        }
    }

    /// Adds a stylesheet in order to the document.
    ///
    /// Stylesheets will later be matched in the order in which they were added.
    pub fn append_stylesheet(&mut self, stylesheet: Stylesheet) {
        self.stylesheets.push(stylesheet);
    }

    /// Creates an element of the specified `name` as a child of `parent`.
    ///
    /// This is the main function to create new SVG elements while parsing XML.
    ///
    /// `name` is the XML element's name, for example `rect`.
    ///
    /// `attrs` has the XML element's attributes, e.g. cx/cy/r for `<circle cx="0" cy="0"
    /// r="5">`.
    ///
    /// If `parent` is `None` it means that we are creating the root node in the tree of
    /// elements.  The code will later validate that this is indeed an `<svg>` element.
    pub fn append_element(
        &mut self,
        name: &QualName,
        attrs: Attributes,
        parent: Option<Node>,
    ) -> Node {
        let node = Node::new(NodeData::new_element(&self.session, name, attrs));

        if let Some(id) = node.borrow_element().get_id() {
            match self.ids.entry(id.to_string()) {
                Entry::Occupied(_) => {
                    rsvg_log!(self.session, "ignoring duplicate id {id} for {node}");
                }

                Entry::Vacant(e) => {
                    e.insert(node.clone());
                }
            }
        }

        if let Some(parent) = parent {
            parent.append(node.clone());
        } else if self.tree.is_none() {
            self.tree = Some(node.clone());
        } else {
            panic!("The tree root has already been set");
        }

        node
    }

    /// Creates a node for an XML text element as a child of `parent`.
    pub fn append_characters(&mut self, text: &str, parent: &mut Node) {
        if !text.is_empty() {
            // When the last child is a Chars node we can coalesce
            // the text and avoid screwing up the Pango layouts
            if let Some(child) = parent.last_child().filter(|c| c.is_chars()) {
                child.borrow_chars().append(text);
            } else {
                parent.append(Node::new(NodeData::new_chars(text)));
            };
        }
    }

    /// Does the final validation on the `Document` being read, and returns it.
    pub fn build(self) -> Result<Document, LoadingError> {
        let DocumentBuilder {
            load_options,
            session,
            tree,
            ids,
            stylesheets,
            ..
        } = self;

        match tree {
            Some(root) if root.is_element() => {
                if is_element_of_type!(root, Svg) {
                    let document = Document {
                        tree: RefCell::new(root),
                        session: session.clone(),
                        ids,
                        resources: RefCell::new(Resources::new()),
                        load_options,
                        stylesheets,
                        needs_cascade: Cell::new(true),
                    };

                    Ok(document)
                } else {
                    Err(LoadingError::NoSvgRoot)
                }
            }
            _ => Err(LoadingError::NoSvgRoot),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_node_id() {
        assert_eq!(
            NodeId::parse("#foo").unwrap(),
            NodeId::Internal("foo".to_string())
        );

        assert_eq!(
            NodeId::parse("uri#foo").unwrap(),
            NodeId::External("uri".to_string(), "foo".to_string())
        );

        assert!(matches!(
            NodeId::parse("uri"),
            Err(NodeIdError::NodeIdRequired)
        ));
    }

    #[test]
    fn ignores_stylesheet_with_invalid_utf8() {
        let handle = crate::api::Loader::new()
            .read_path("tests/fixtures/loading/non-utf8-stylesheet.svg")
            .unwrap();
        assert!(handle.document.stylesheets.is_empty());
    }
}
