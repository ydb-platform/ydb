Architecture of librsvg
=======================

This document roughly describes the architecture of librsvg, and
future plans for it.  The code is continually evolving, so don't
consider this as the ground truth, but rather like a cheap map you buy
at a street corner.

The library's internals are documented as Rust documentation comments;
you can look at the rendered version at
https://gnome.pages.gitlab.gnome.org/librsvg/internals/librsvg/index.html

You may also want to see the section below on [interesting parts of
the code](#some_interesting_parts_of_the_code).

# A bit of history

Librsvg is an old library.  It started around 2001, when Eazel (the
original makers of GNOME's file manager Nautilus) needed a library to
render SVG images.  At that time the SVG format was being
standardized, so librsvg grew along with the SVG specification.  This
is why you will sometimes see references to deprecated SVG features in
the source code.

Librsvg started as an experiment to use libxml2's new SAX parser, so
that SVG could be streamed in and rendered on the fly, instead of
first creating a DOM tree.  Originally it used [libart] as a rendering
library; this was GNOME's first antialiased renderer with alpha
compositing.  Later, the renderer was replaced with [Cairo].  Librsvg
is currently striving to support other rendering backends.

(These days librsvg indeed builds a DOM tree by itself; it needs the
tree to run the CSS cascade and do selector matching.)

Librsvg started as a C library with an ad-hoc API.  At some point it
got turned into a [GObject] library, so that the main `RsvgHandle`
object defines most of the entry points into the library.  Through
[GObject Introspection], this allows librsvg to be used from other
programming languages.

In 2016, librsvg started getting ported to Rust.  As of early 2021,
the whole library is implemented in Rust, and exports an intact C
API/ABI.  It also exports a more idiomatic Rust API as well.

[libart]: https://levien.com/libart/
[Cairo]: https://www.cairographics.org/
[GObject Introspection]: https://gi.readthedocs.io/en/latest/
[gi]: https://people.gnome.org/~federico/blog/magic-of-gobject-introspection.html

# The C and Rust APIs

Librsvg exports two public APIs, one for C and one for Rust.

The C API has hard requirements for API/ABI stability, because it is
used all over the GNOME project and API/ABI breaks would be highly
disruptive.  Also, the C API is what allows librsvg to be called from
other programming languages, through [GObject Introspection].

The Rust API is a bit more lax in its API stability, but we try to
stick to [semantic versioning][semver] as is common in Rust.

The public Rust API is implemented in `src/api.rs`.  This has all the
primitives needed to load and render SVG documents or individual
elements, and to configure loading/rendering options.

The public C API is implemented in `src/c_api/`, and it is implemented
in terms of the public Rust API.  Note that as of 2021/Feb the
corresponding C header files are hand-written in `include/librsvg/`;
maybe in the future they will be generated automatically with
[cbindgen].

We consider it good practice to provide simple and clean primitives in
the Rust API, and have `c_api` deal with all the idiosyncrasies and
historical considerations for the C API.

```
+----------------+
|  Public C API  |
|    src/c_api   |
+----------------+
        |
      calls
        |
        v
+-------------------+
|  Public Rust API  |
|     src/api.rs    |
+-------------------+
        |
      calls
        |
        v
+-------------------+
| library internals |
|      src/*.rs     |
+-------------------+
```

[semver]: https://semver.org/
[cbindgen]: https://github.com/eqrion/cbindgen/blob/master/docs.md

# The test suite

The test suite is documented in `tests/README.md`.

# Code flow

The caller of librsvg loads a document into a handle, and later may
ask to render the document or one of its elements, or measure their
geometries.

## Loading an SVG document

The Rust API starts by constructing an `SvgHandle` from a `Loader`;
both of those are public types.  Internally the `SvgHandle` is just a
wrapper around a `Handle`, which is a private type.  `Handle`
represents an SVG document loaded in memory; it acts as a wrapper
around a `Document`, and provides the basic primitive operations like
"render the whole document" or "compute the geometry of an element"
that are needed to implement the public APIs.

A `Document` gets created by loading XML from a stream, into a tree of
`Node` structures.  This is similar to a web browser's DOM tree.

Each XML element causes a new `Node` to get created with an `Element`
in it.  The `Element` enum can represent all the SVG element types;
for example, a `<path>` element from XML gets turned into a
`Node::Element(Element::Path)`.

When an `Element` is created from its corresponding XML, its
`Attributes` get parsed.  On one hand, attributes that are specific to
a particular element type, like the `d` in `<path d="...">` get parsed
by the `set_attributes` method of each particular element type (in
that case, `Path::set_attributes`).

On the other hand, attributes that refer to styles, and which may
appear for any kind of element, get all parsed into a
`SpecifiedValues` struct.  This is a memory-efficient representation
of the CSS style properties that an element has.

There is a lot of parsing going on; see below for the details of
[parsing](#parsing).

When the XML document is fully parsed, a `Document` contains a tree of
`Node` structs and their inner `Element` structs.  The tree has also
been validated to ensure that the root is an `<svg>` element.

After that, the CSS cascade step gets run.

## The CSS cascade

Each `Element` has a `SpecifiedValues`, which has the CSS style
properties that the XML specified for that element.  However,
`SpecifiedValues` is sparse, as not all the possible style properties
may have been filled in.  Cascading means following the CSS/SVG rules
for each property type to inherit missing properties from parent
elements.  For example, in this document fragment:

```
<g stroke-width="2" stroke="black">
  <path d="M0,0 L10,0" fill="blue"/>
  <path d="M20,0 L30,0" fill="green"/>
</g>
```

Each `<path>` element has a different fill color, but they both
*inherit* the `stroke-width` and `stroke` values from their parent
group.  This is because both the `stroke-width` and `stroke`
properties are defined in the CSS/SVG specifications to inherit
automatically.  Some other properties, like `opacity`, do not inherit
and are thus not copied to child elements.

In librsvg, the individual types for CSS properties are defined with
the `make_property` macro.

The cascading step takes each element's `SpecifiedValues` and composes
it by CSS inheritance onto a `ComputedValues`.  The latter is a
memory-efficient representation of all the possible CSS properties
that an element can have.

When cascading is done, each `Element` has a fully resolved
`ComputedValues` struct, which is what gets used during rendering to
look up things like the element's stroke width or fill color.

## Parsing

### XML into a tree of Nodes / Elements

Librsvg uses an XML parser (libxml2 at the time of this writing) to do
the first-stage parsing of the SVG document.  `XmlState` contains the
XML parsing state, which is a stack of contexts depending on the XML
nesting structure.  `XmlState` has public methods, called from the XML
parser as it goes.  The most important one is `start_element`; this
is responsible for creating new `Node` structures in the tree, within
the `DocumentBuilder` being built.

Nodes are either SVG elements (the `Element` enum), or text data
inside elements (the `Chars` struct); this last one will not concern
us here, and we will only talk about `Element`.

Each supported kind of `Element` parses its attributes in a
`set_attributes()` method.  Each attribute is just a key/value pair;
for example, the `<rect width="5px">` element has a `width` attribute
whose value is `5px`.

While parsing its attributes, an element may encounter an invalid
value, for example, a negative width where only nonnegative ones are
allowed.  In this case, the element's `set_attributes` method may
return a `Result::Err`.  The caller will then do `set_error` to mark
that element as being in an error state.  If an element is in error,
its children will get parsed as usual, but the element and its
children will be ignored during the rendering stage.

The SVG spec says that SVG rendering should stop on the first element
that is "in error".  However, most implementations simply seem to
ignore erroneous elements instead of completely stopping rendering,
and we do the same in librsvg.

### CSS and styles

Librsvg uses Servo's `cssparser` crate as a CSS tokenizer, and
`selectors` as a high-level parser for CSS style data.

With the `cssparser` crate, the caller is responsible for providing an
implementation of the `DeclarationParser` trait.  Its `parse_value`
method takes the name of a CSS property name like `fill`, plus a value
like `rgb(255, 0, 0)`, and it must return a value that represents a
parsed declaration.  Librsvg uses the `Declaration` struct for this.

The core of parsing CSS is the `parse_value` function, which returns a `ParsedProperty`:

```rust
pub enum ParsedProperty {
    BaselineShift(SpecifiedValue<BaselineShift>),
    ClipPath(SpecifiedValue<ClipPath>),
    Color(SpecifiedValue<Color>),
    // etc.
}
```

What is `SpecifiedValue`?  It is the parsed value for a CSS property directly as it comes out of the SVG document:

```rust
pub enum SpecifiedValue<T>
where
    T: Property<ComputedValues> + Clone + Default,
{
    Unspecified,
    Inherit,
    Specified(T),
}
```

A property declaration can look like `opacity: inherit;` - this would
create a `ParsedProperty::Opacity(SpecifiedValue::Inherit)`.

Or it can look like `opacity: 0.5;` - this would create a
`ParsedProperty::Opacity(SpecifiedValue::Specified(Opacity(UnitInterval(0.5))))`.
Let's break this down:

* `ParsedProperty::Opacity` - which property did we parse?

* `SpecifiedValue::Specified` - it actually was specified by the
  document with a value; the other interesting alternative is
  `Inherit`, which corresponds to the value `inherit` that all CSS
  property declarations can have.

* `Opacity(UnitInterval(0.5))` - This is the type `Opacity` property,
  which is a newtype around an internal `UnitInterval` type, which in
  turn guarantees that we have a float in the range `[0.0, 1.0]`.

There is a Rust type for every CSS property that librsvg supports;
many of these types are newtypes around primitive types like `f64`.

Eventually an entire CSS stylesheet, like the contents of a `<style>`
element, gets parsed into a `Stylesheet` struct.  A stylesheet has a
list of rules, where each rule is the CSS selectors defined for it,
and the style declarations that should be applied for the `Node`s that
match the selectors.  For example, in a little stylesheet like this:

```xml
<style type="text/css">
  rect, #some_id {
    fill: blue;
    stroke-width: 5px;
  }
</style>
```

This stylesheet has a single rule.  The rule has a selector list with
two selectors (`rect` and `#some_id`) and two style declarations
(`fill: blue` and `stroke-width: 5px`).

After parsing is done, there is a **cascading stage** where librsvg
walks the tree of nodes, and for each node it finds the CSS rules that
should be applied to it.

## Rendering

The rendering process starts at the `draw_tree()` function.  This sets
up a `DrawingCtx`, which carries around all the mutable state during
rendering.

Rendering is a recursive process, which goes back and forth between
the utility functions in `DrawingCtx` and the `draw` method in
elements.

The main job of `DrawingCtx` is to deal with the SVG drawing model.
Each element renders itself independently, and its result gets
modified before getting composited onto the final image:

1. Render an element to a temporary surface (example: stroke and fill a path).
2. Apply filter effects (blur, color mapping, etc.).
3. Apply clipping paths.
4. Apply masks.
5. Composite the result onto the final image.

The temporary result from the last step also gets put in a stack; this
is because filter effects sometimes need to look at the
currently-drawn background to apply further filtering to it.

You'll see that most of the rendering-related functions return a
`Result<BoundingBox, RenderingError>`.  Some SVG features require
knowing the bounding box of the object that is being rendered; for
historical reasons this bounding box is computed as part of the
rendering process in librsvg.  When computing a subtree's bounding
box, the bounding boxes from the leaves get aggregated up to the root
of the subtree.  Each node in the tree has its own coordinate system;
`BoundingBox` is able to transform coordinate systems to get a
bounding box that is meaningful with respect to the root's transform.

# Comparing floating-point numbers

Librsvg sometimes needs to compute things like "are these points
equal?" or "did this computed result equal this test reference
number?".

We use `f64` numbers in Rust for all computations on real numbers.
Floating-point numbers cannot be compared with `==` effectively, since
it doesn't work when the numbers are slightly different due to
numerical inaccuracies.

Similarly, we don't `assert_eq!(a, b)` for floating-point numbers.

Most of the time we are dealing with coordinates which will get passed
to Cairo.  In turn, Cairo converts them from doubles to a fixed-point
representation (as of March 2018, Cairo uses 24.8 fixnums with 24 bits of
integral part and 8 bits of fractional part).

So, we can consider two numbers to be "equal" if they would be represented
as the same fixed-point value by Cairo.  Librsvg implements this in
the [`ApproxEqCairo` trait][ApproxEqCairo] trait.  You can use it like
this:

```rust
use float_eq_cairo::ApproxEqCairo; // bring the trait into scope

let a: f64 = ...;
let b: f64 = ...;

if a.approx_eq_cairo(&b) { // not a == b
    ... // equal!
}

assert!(1.0_f64.approx_eq_cairo(&1.001953125_f64)); // 1 + 1/512 - cairo rounds to 1
```

[ApproxEqCairo]: src/float_eq_cairo.rs

# Some interesting parts of the code

* Are you adding support for a CSS property?  Look in the
  [`property_defs`] and [`properties`] modules.  `property_defs`
  defines most of the CSS properties that librsvg supports, and
  `properties` actually puts all those properties in the
  `SpecifiedValues` and `ComputedValues` structs.

* The [`Handle`] struct provides the primitives to implement the
  public APIs, such as loading an SVG file and rendering it.

* The [`DrawingCtx`] struct is active while an SVG handle is being
  drawn.  It has all the mutable state related to the drawing process,
  such as the stack of temporary rendered surfaces, and the viewport
  stack.

* The [`Document`] struct represents a loaded SVG document.  It holds
  the tree of [`Node`] structs, some of which contain [`Element`]s.  A
  `Document` also contains a mapping of `id` attributes to the
  corresponding element nodes.

* The [`xml`] module receives events from an XML parser, and builds a
  [`Document`] tree.

* The [`css`] module has the high-level machinery for parsing CSS and
  representing parsed stylesheets.  The low-level parsers for
  individual properties are in [`property_defs`] and [`font_props`].

[`css`]: src/css.rs
[`Document`]: src/document.rs
[`DrawingCtx`]: src/drawing_ctx.rs
[`Element`]: src/element.rs
[`font_props`]: src/font_props.rs
[`Handle`]: src/handle.rs
[`Node`]: src/node.rs
[`properties`]: src/properties.rs
[`property_defs`]: src/property_defs.rs
[`xml`]: src/xml/mod.rs
