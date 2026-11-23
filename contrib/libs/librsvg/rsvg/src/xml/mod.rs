//! The main XML parser.

use encoding_rs::Encoding;
use gio::{
    BufferedInputStream, Cancellable, ConverterInputStream, InputStream, ZlibCompressorFormat,
    ZlibDecompressor, prelude::BufferedInputStreamExt,
};
use glib::object::Cast;
use markup5ever::{ExpandedName, LocalName, Namespace, QualName, expanded_name, local_name, ns};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::str;
use std::string::ToString;
use std::sync::Arc;
use xml5ever::{
    TokenizerResult,
    buffer_queue::BufferQueue,
    tendril::format_tendril,
    tokenizer::{ProcessResult, TagKind, Token, TokenSink, XmlTokenizer, XmlTokenizerOpts},
};

use crate::borrow_element_as;
use crate::css::{Origin, Stylesheet};
use crate::document::{Document, DocumentBuilder, LoadOptions};
use crate::error::{ImplementationLimit, LoadingError};
use crate::io::{self, IoError};
use crate::limits::{MAX_LOADED_ELEMENTS, MAX_XINCLUDE_DEPTH};
use crate::node::{Node, NodeBorrow};
use crate::rsvg_log;
use crate::session::Session;
use crate::style::StyleType;
use crate::url_resolver::AllowedUrl;

use xml2_load::Xml2Parser;

mod attributes;
mod xml2;
mod xml2_load;

use xml2::xmlEntityPtr;

pub use attributes::Attributes;

#[derive(Clone)]
enum Context {
    // Starting state
    Start,

    // Creating nodes for elements under the current node
    ElementCreation,

    // Inside <style>; accumulate text to include in a stylesheet
    Style,

    // An unsupported element inside a `<style>` element, to be ignored
    UnsupportedStyleChild,

    // Inside <xi:include>
    XInclude(XIncludeContext),

    // An unsupported element inside a <xi:include> context, to be ignored
    UnsupportedXIncludeChild,

    // Insie <xi::fallback>
    XIncludeFallback(XIncludeContext),

    // An XML parsing error was found.  We will no-op upon any further XML events.
    FatalError(LoadingError),
}

#[derive(Clone)]
struct XIncludeContext {
    need_fallback: bool,
}

unsafe extern "C" {
    // The original function takes an xmlNodePtr, but that is compatible
    // with xmlEntityPtr for the purposes of this function.
    fn xmlFreeNode(node: xmlEntityPtr);
}

/// This is to hold an xmlEntityPtr from libxml2; we just hold an opaque pointer
/// that is freed in impl Drop.
struct XmlEntity(xmlEntityPtr);

impl Drop for XmlEntity {
    fn drop(&mut self) {
        unsafe {
            // Even though we are freeing an xmlEntityPtr, historically the code has always
            // used xmlFreeNode() because that function actually does allow freeing entities.
            //
            // See https://gitlab.gnome.org/GNOME/libxml2/-/issues/731
            // for a possible memory leak on older versions of libxml2 when using
            // xmlFreeNode() instead of xmlFreeEntity() - the latter just became public
            // in librsvg-2.12.0.
            xmlFreeNode(self.0);
        }
    }
}

// Creates an ExpandedName from the XInclude namespace and a local_name
//
// The markup5ever crate doesn't have built-in namespaces for XInclude,
// so we make our own.
macro_rules! xinclude_name {
    ($local_name:expr) => {
        ExpandedName {
            ns: &Namespace::from("http://www.w3.org/2001/XInclude"),
            local: &LocalName::from($local_name),
        }
    };
}

/// Holds the state used for XML processing
///
/// These methods are called when an XML event is parsed out of the XML stream: `start_element`,
/// `end_element`, `characters`.
///
/// When an element starts, we push a corresponding `Context` into the `context_stack`.  Within
/// that context, all XML events will be forwarded to it, and processed in one of the `XmlHandler`
/// trait objects. Normally the context refers to a `NodeCreationContext` implementation which is
/// what creates normal graphical elements.
struct XmlStateInner {
    document_builder: DocumentBuilder,
    num_loaded_elements: usize,
    xinclude_depth: usize,
    context_stack: Vec<Context>,
    current_node: Option<Node>,

    // Note that neither XmlStateInner nor Xmlstate implement Drop.
    //
    // An XmlState is finally consumed in XmlState::build_document(), and that
    // function is responsible for freeing all the XmlEntityPtr from this field.
    //
    // (The structs cannot impl Drop because build_document()
    // destructures and consumes them at the same time.)
    entities: HashMap<String, XmlEntity>,
}

pub struct XmlState {
    inner: RefCell<XmlStateInner>,

    session: Session,
    load_options: Arc<LoadOptions>,
}

/// Errors returned from XmlState::acquire()
///
/// These follow the terminology from <https://www.w3.org/TR/xinclude/#terminology>
enum AcquireError {
    /// Resource could not be acquired (file not found), or I/O error.
    /// In this case, the `xi:fallback` can be used if present.
    ResourceError,

    /// Resource could not be parsed/decoded
    FatalError(String),
}

impl XmlStateInner {
    fn context(&self) -> Context {
        // We can unwrap since the stack is never empty
        self.context_stack.last().unwrap().clone()
    }
}

impl XmlState {
    fn new(
        session: Session,
        document_builder: DocumentBuilder,
        load_options: Arc<LoadOptions>,
    ) -> XmlState {
        XmlState {
            inner: RefCell::new(XmlStateInner {
                document_builder,
                num_loaded_elements: 0,
                xinclude_depth: 0,
                context_stack: vec![Context::Start],
                current_node: None,
                entities: HashMap::new(),
            }),

            session,
            load_options,
        }
    }

    fn check_last_error(&self) -> Result<(), LoadingError> {
        let inner = self.inner.borrow();

        match inner.context() {
            Context::FatalError(e) => Err(e),
            _ => Ok(()),
        }
    }

    fn check_limits(&self) -> Result<(), ()> {
        if self.inner.borrow().num_loaded_elements > MAX_LOADED_ELEMENTS {
            self.error(LoadingError::LimitExceeded(
                ImplementationLimit::TooManyLoadedElements,
            ));
            Err(())
        } else {
            Ok(())
        }
    }

    pub fn start_element(&self, name: QualName, attrs: Attributes) -> Result<(), ()> {
        self.check_limits()?;

        let context = self.inner.borrow().context();

        if let Context::FatalError(_) = context {
            return Err(());
        }

        self.inner.borrow_mut().num_loaded_elements += 1;

        let new_context = match context {
            Context::Start => self.element_creation_start_element(&name, attrs),
            Context::ElementCreation => self.element_creation_start_element(&name, attrs),

            Context::Style => self.inside_style_start_element(&name),
            Context::UnsupportedStyleChild => self.unsupported_style_start_element(&name),

            Context::XInclude(ref ctx) => self.inside_xinclude_start_element(ctx, &name),
            Context::UnsupportedXIncludeChild => self.unsupported_xinclude_start_element(&name),
            Context::XIncludeFallback(ref ctx) => {
                self.xinclude_fallback_start_element(ctx, &name, attrs)
            }

            Context::FatalError(_) => unreachable!(),
        };

        self.inner.borrow_mut().context_stack.push(new_context);

        Ok(())
    }

    pub fn end_element(&self, _name: QualName) {
        let context = self.inner.borrow().context();

        match context {
            Context::Start => panic!("end_element: XML handler stack is empty!?"),
            Context::ElementCreation => self.element_creation_end_element(),

            Context::Style => self.style_end_element(),
            Context::UnsupportedStyleChild => (),

            Context::XInclude(_) => (),
            Context::UnsupportedXIncludeChild => (),
            Context::XIncludeFallback(_) => (),

            Context::FatalError(_) => return,
        }

        // We can unwrap since start_element() always adds a context to the stack
        self.inner.borrow_mut().context_stack.pop().unwrap();
    }

    pub fn characters(&self, text: &str) {
        let context = self.inner.borrow().context();

        match context {
            Context::Start => {
                // This is character data before the first element, i.e. something like
                //  <?xml version="1.0" encoding="UTF-8"?><svg xmlns="http://www.w3.org/2000/svg"/>
                // ^ note the space here
                // libxml2 is not finished reading the file yet; it will emit an error
                // on its own when it finishes.  So, ignore this condition.
            }

            Context::ElementCreation => self.element_creation_characters(text),

            Context::Style => self.element_creation_characters(text),
            Context::UnsupportedStyleChild => (),

            Context::XInclude(_) => (),
            Context::UnsupportedXIncludeChild => (),
            Context::XIncludeFallback(ref ctx) => self.xinclude_fallback_characters(ctx, text),
            Context::FatalError(_) => (),
        }
    }

    pub fn processing_instruction(&self, target: &str, data: &str) {
        if target != "xml-stylesheet" {
            return;
        }

        if let Ok(pairs) = parse_xml_stylesheet_processing_instruction(data) {
            let mut alternate = None;
            let mut type_ = None;
            let mut href = None;

            for (att, value) in pairs {
                match att.as_str() {
                    "alternate" => alternate = Some(value),
                    "type" => type_ = Some(value),
                    "href" => href = Some(value),
                    _ => (),
                }
            }

            let mut inner = self.inner.borrow_mut();

            if type_.as_deref() != Some("text/css")
                || (alternate.is_some() && alternate.as_deref() != Some("no"))
            {
                rsvg_log!(
                    self.session,
                    "invalid parameters in XML processing instruction for stylesheet",
                );
                return;
            }

            if let Some(href) = href {
                if let Ok(aurl) = self.load_options.url_resolver.resolve_href(&href) {
                    if let Ok(stylesheet) =
                        Stylesheet::from_href(&aurl, Origin::Author, self.session.clone())
                    {
                        inner.document_builder.append_stylesheet(stylesheet);
                    } else {
                        // FIXME: https://www.w3.org/TR/xml-stylesheet/ does not seem to specify
                        // what to do if the stylesheet cannot be loaded, so here we ignore the error.
                        rsvg_log!(
                            self.session,
                            "could not create stylesheet from {} in XML processing instruction",
                            href
                        );
                    }
                } else {
                    rsvg_log!(
                        self.session,
                        "{} not allowed for xml-stylesheet in XML processing instruction",
                        href
                    );
                }
            } else {
                rsvg_log!(
                    self.session,
                    "xml-stylesheet processing instruction does not have href; ignoring"
                );
            }
        } else {
            self.error(LoadingError::XmlParseError(String::from(
                "invalid processing instruction data in xml-stylesheet",
            )));
        }
    }

    pub fn error(&self, e: LoadingError) {
        self.inner
            .borrow_mut()
            .context_stack
            .push(Context::FatalError(e));
    }

    pub fn entity_lookup(&self, entity_name: &str) -> Option<xmlEntityPtr> {
        self.inner
            .borrow()
            .entities
            .get(entity_name)
            .map(|entity| entity.0)
    }

    pub fn entity_insert(&self, entity_name: &str, entity: xmlEntityPtr) {
        let mut inner = self.inner.borrow_mut();

        inner
            .entities
            .insert(entity_name.to_string(), XmlEntity(entity));
    }

    fn element_creation_start_element(&self, name: &QualName, attrs: Attributes) -> Context {
        if name.expanded() == xinclude_name!("include") {
            self.xinclude_start_element(name, attrs)
        } else {
            let mut inner = self.inner.borrow_mut();

            let parent = inner.current_node.clone();
            let node = inner.document_builder.append_element(name, attrs, parent);
            inner.current_node = Some(node);

            if name.expanded() == expanded_name!(svg "style") {
                Context::Style
            } else {
                Context::ElementCreation
            }
        }
    }

    fn element_creation_end_element(&self) {
        let mut inner = self.inner.borrow_mut();
        let node = inner.current_node.take().unwrap();
        inner.current_node = node.parent();
    }

    fn element_creation_characters(&self, text: &str) {
        let mut inner = self.inner.borrow_mut();

        let mut parent = inner.current_node.clone().unwrap();
        inner.document_builder.append_characters(text, &mut parent);
    }

    fn style_end_element(&self) {
        self.add_inline_stylesheet();
        self.element_creation_end_element()
    }

    fn add_inline_stylesheet(&self) {
        let mut inner = self.inner.borrow_mut();
        let current_node = inner.current_node.as_ref().unwrap();

        let style_type = borrow_element_as!(current_node, Style).style_type();

        if style_type == StyleType::TextCss {
            let stylesheet_text = current_node
                .children()
                .map(|child| {
                    // Note that here we assume that the only children of <style>
                    // are indeed text nodes.
                    let child_borrow = child.borrow_chars();
                    child_borrow.get_string()
                })
                .collect::<String>();

            if let Ok(stylesheet) = Stylesheet::from_data(
                &stylesheet_text,
                &self.load_options.url_resolver,
                Origin::Author,
                self.session.clone(),
            ) {
                inner.document_builder.append_stylesheet(stylesheet);
            } else {
                rsvg_log!(self.session, "invalid inline stylesheet");
            }
        }
    }

    fn inside_style_start_element(&self, name: &QualName) -> Context {
        self.unsupported_style_start_element(name)
    }

    fn unsupported_style_start_element(&self, _name: &QualName) -> Context {
        Context::UnsupportedStyleChild
    }

    fn xinclude_start_element(&self, _name: &QualName, attrs: Attributes) -> Context {
        let mut href = None;
        let mut parse = None;
        let mut encoding = None;

        let ln_parse = LocalName::from("parse");

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "href") => href = Some(value),
                ref v
                    if *v
                        == ExpandedName {
                            ns: &ns!(),
                            local: &ln_parse,
                        } =>
                {
                    parse = Some(value)
                }
                expanded_name!("", "encoding") => encoding = Some(value),
                _ => (),
            }
        }

        let need_fallback = match self.acquire(href, parse, encoding) {
            Ok(()) => false,
            Err(AcquireError::ResourceError) => true,
            Err(AcquireError::FatalError(s)) => {
                return Context::FatalError(LoadingError::XmlParseError(s));
            }
        };

        Context::XInclude(XIncludeContext { need_fallback })
    }

    fn inside_xinclude_start_element(&self, ctx: &XIncludeContext, name: &QualName) -> Context {
        if name.expanded() == xinclude_name!("fallback") {
            Context::XIncludeFallback(ctx.clone())
        } else {
            // https://www.w3.org/TR/xinclude/#include_element
            //
            // "Other content (text, processing instructions,
            // comments, elements not in the XInclude namespace,
            // descendants of child elements) is not constrained by
            // this specification and is ignored by the XInclude
            // processor"

            self.unsupported_xinclude_start_element(name)
        }
    }

    fn xinclude_fallback_start_element(
        &self,
        ctx: &XIncludeContext,
        name: &QualName,
        attrs: Attributes,
    ) -> Context {
        if ctx.need_fallback {
            if name.expanded() == xinclude_name!("include") {
                self.xinclude_start_element(name, attrs)
            } else {
                self.element_creation_start_element(name, attrs)
            }
        } else {
            Context::UnsupportedXIncludeChild
        }
    }

    fn xinclude_fallback_characters(&self, ctx: &XIncludeContext, text: &str) {
        if ctx.need_fallback && self.inner.borrow().current_node.is_some() {
            // We test for is_some() because with a bad "SVG" file like this:
            //
            //    <xi:include href="blah"><xi:fallback>foo</xi:fallback></xi:include>
            //
            // at the point we get "foo" here, there is no current_node because
            // no nodes have been created before the xi:include.
            self.element_creation_characters(text);
        }
    }

    fn acquire(
        &self,
        href: Option<&str>,
        parse: Option<&str>,
        encoding: Option<&str>,
    ) -> Result<(), AcquireError> {
        if let Some(href) = href {
            let aurl = self
                .load_options
                .url_resolver
                .resolve_href(href)
                .map_err(|e| {
                    // FIXME: should AlloweUrlError::UrlParseError be a fatal error,
                    // not a resource error?
                    rsvg_log!(self.session, "could not acquire \"{}\": {}", href, e);
                    AcquireError::ResourceError
                })?;

            // https://www.w3.org/TR/xinclude/#include_element
            //
            // "When omitted, the value of "xml" is implied (even in
            // the absence of a default value declaration). Values
            // other than "xml" and "text" are a fatal error."
            match parse {
                None | Some("xml") => self.include_xml(&aurl),

                Some("text") => self.acquire_text(&aurl, encoding),

                Some(v) => Err(AcquireError::FatalError(format!(
                    "unknown 'parse' attribute value: \"{v}\""
                ))),
            }
        } else {
            // The href attribute is not present.  Per
            // https://www.w3.org/TR/xinclude/#include_element we
            // should use the xpointer attribute, but we do not
            // support that yet.  So, we'll just say, "OK" and not
            // actually include anything.
            Ok(())
        }
    }

    fn include_xml(&self, aurl: &AllowedUrl) -> Result<(), AcquireError> {
        self.increase_xinclude_depth(aurl)?;

        let result = self.acquire_xml(aurl);

        self.decrease_xinclude_depth();

        result
    }

    fn increase_xinclude_depth(&self, aurl: &AllowedUrl) -> Result<(), AcquireError> {
        let mut inner = self.inner.borrow_mut();

        if inner.xinclude_depth == MAX_XINCLUDE_DEPTH {
            Err(AcquireError::FatalError(format!(
                "exceeded maximum level of nested xinclude in {aurl}"
            )))
        } else {
            inner.xinclude_depth += 1;
            Ok(())
        }
    }

    fn decrease_xinclude_depth(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.xinclude_depth -= 1;
    }

    fn acquire_text(&self, aurl: &AllowedUrl, encoding: Option<&str>) -> Result<(), AcquireError> {
        let binary = io::acquire_data(aurl, None).map_err(|e| {
            rsvg_log!(self.session, "could not acquire \"{}\": {}", aurl, e);
            AcquireError::ResourceError
        })?;

        let encoding = encoding.unwrap_or("utf-8");

        let encoder = Encoding::for_label_no_replacement(encoding.as_bytes()).ok_or_else(|| {
            AcquireError::FatalError(format!("unknown encoding \"{encoding}\" for \"{aurl}\""))
        })?;

        let utf8_data = encoder
            .decode_without_bom_handling_and_without_replacement(&binary.data)
            .ok_or_else(|| {
                AcquireError::FatalError(format!("could not convert contents of \"{aurl}\" from character encoding \"{encoding}\""))
            })?;

        self.element_creation_characters(&utf8_data);
        Ok(())
    }

    fn acquire_xml(&self, aurl: &AllowedUrl) -> Result<(), AcquireError> {
        // FIXME: distinguish between "file not found" and "invalid XML"

        let stream = io::acquire_stream(aurl, None).map_err(|e| match e {
            IoError::BadDataUrl => AcquireError::FatalError(String::from("malformed data: URL")),
            _ => AcquireError::ResourceError,
        })?;

        // FIXME: pass a cancellable
        self.parse_from_stream(&stream, None).map_err(|e| match e {
            LoadingError::Io(_) => AcquireError::ResourceError,
            LoadingError::XmlParseError(s) => AcquireError::FatalError(s),
            _ => AcquireError::FatalError(String::from("unknown error")),
        })
    }

    // Parses XML from a stream into an XmlState.
    //
    // This can be called "in the middle" of an XmlState's processing status,
    // for example, when including another XML file via xi:include.
    fn parse_from_stream(
        &self,
        stream: &gio::InputStream,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<(), LoadingError> {
        Xml2Parser::from_stream(self, self.load_options.unlimited_size, stream, cancellable)
            .and_then(|parser| parser.parse())
            .and_then(|_: ()| self.check_last_error())
    }

    fn unsupported_xinclude_start_element(&self, _name: &QualName) -> Context {
        Context::UnsupportedXIncludeChild
    }

    fn build_document(
        self,
        stream: &gio::InputStream,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<Document, LoadingError> {
        self.parse_from_stream(stream, cancellable)?;

        // consume self, then consume inner, then consume document_builder by calling .build()

        let XmlState { inner, .. } = self;
        let inner = inner.into_inner();

        let XmlStateInner {
            document_builder, ..
        } = inner;
        document_builder.build()
    }
}

/// Temporary holding space for data in an XML processing instruction.
///
/// We use a little hack via xml5ever to parse the contents of an XML processing instruction.
/// See the comment in parse_xml_stylesheet_processing_instruction() below.
#[derive(Default)]
struct ProcessingInstructionData {
    attributes: Vec<(String, String)>,
    error: bool,
}

struct ProcessingInstructionSink(Rc<RefCell<ProcessingInstructionData>>);

impl TokenSink for ProcessingInstructionSink {
    // xml5ever's tokenizer only uses this if we are actually using it to parse full XML;
    // here, the Handle associated type refers to a DOM script, which we know can't appear
    // in the way we use xml5ever, so we use the unit type instead.
    type Handle = ();

    fn process_token(&self, token: Token) -> ProcessResult<()> {
        let mut data = self.0.borrow_mut();

        match token {
            Token::Tag(tag) if tag.kind == TagKind::EmptyTag => {
                for a in &tag.attrs {
                    let name = a.name.local.as_ref().to_string();
                    let value = a.value.to_string();

                    data.attributes.push((name, value));
                }
            }

            Token::ParseError(_) => data.error = true,

            _ => (),
        }

        ProcessResult::Continue
    }
}

// https://www.w3.org/TR/xml-stylesheet/
//
// The syntax for the xml-stylesheet processing instruction we support
// is this:
//
//   <?xml-stylesheet href="uri" alternate="no" type="text/css"?>
//
// XML parsers just feed us the raw data after the target name
// ("xml-stylesheet"), so we'll create a mini-parser with a hackish
// element just to extract the data as attributes.
fn parse_xml_stylesheet_processing_instruction(data: &str) -> Result<Vec<(String, String)>, ()> {
    let pi_data = Rc::new(RefCell::new(ProcessingInstructionData {
        attributes: Vec::new(),
        error: false,
    }));

    let queue = BufferQueue::default();
    queue.push_back(format_tendril!("<rsvg-hack {} />", data));

    let sink = ProcessingInstructionSink(pi_data.clone());

    let tokenizer = XmlTokenizer::new(sink, XmlTokenizerOpts::default());

    match tokenizer.run(&queue) {
        TokenizerResult::Done => (),
        _ => unreachable!("got an unexpected TokenizerResult; did xml5ever change its API?"),
    }

    let pi_data = pi_data.borrow();

    if pi_data.error {
        Err(())
    } else {
        Ok(pi_data.attributes.clone())
    }
}

pub fn xml_load_from_possibly_compressed_stream(
    session: Session,
    document_builder: DocumentBuilder,
    load_options: Arc<LoadOptions>,
    stream: &gio::InputStream,
    cancellable: Option<&gio::Cancellable>,
) -> Result<Document, LoadingError> {
    let state = XmlState::new(session, document_builder, load_options);

    let stream = get_input_stream_for_loading(stream, cancellable)?;

    state.build_document(&stream, cancellable)
}

// Header of a gzip data stream
const GZ_MAGIC_0: u8 = 0x1f;
const GZ_MAGIC_1: u8 = 0x8b;

fn get_input_stream_for_loading(
    stream: &InputStream,
    cancellable: Option<&Cancellable>,
) -> Result<InputStream, LoadingError> {
    // detect gzipped streams (svgz)

    let buffered = BufferedInputStream::new(stream);
    let num_read = buffered.fill(2, cancellable)?;
    if num_read < 2 {
        // FIXME: this string was localized in the original; localize it
        return Err(LoadingError::XmlParseError(String::from(
            "Input file is too short",
        )));
    }

    let buf = buffered.peek_buffer();
    assert!(buf.len() >= 2);
    if buf[0..2] == [GZ_MAGIC_0, GZ_MAGIC_1] {
        let decomp = ZlibDecompressor::new(ZlibCompressorFormat::Gzip);
        let converter = ConverterInputStream::new(&buffered, &decomp);
        Ok(converter.upcast::<InputStream>())
    } else {
        Ok(buffered.upcast::<InputStream>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_processing_instruction_data() {
        let mut r =
            parse_xml_stylesheet_processing_instruction("foo=\"bar\" baz=\"beep\"").unwrap();
        r.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            r,
            vec![
                ("baz".to_string(), "beep".to_string()),
                ("foo".to_string(), "bar".to_string())
            ]
        );
    }
}
