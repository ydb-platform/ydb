# Reporting security bugs

Please mail the maintainer at federico@gnome.org.  You can use the GPG
public key from https://viruta.org/docs/fmq-gpg.asc to send encrypted
mail.

# Librsvg releases with security fixes

Librsvg releases have a version number like major.minor.micro. Note
that releases with an odd minor number (e.g. 2.47.x since 47 is odd)
are considered development releases and should not be used in
production systems.

The following list is only for stable release streams, where the minor
number is even (e.g. 2.50.x).

### 2.50.4

RUSTSEC-2020-0146 - lifetime erasure in generic-array.

### 2.48.10

CVE-2020-35905 - RUSTSEC-2020-0059 - data race in futures-util.

CVE-2020-35906 - RUSTSEC-2020-0060 - use-after-free in futures-task.

CVE-2021-25900 - RUSTSEC-2021-0003 - buffer overflow in smallvec.

RUSTSEC-2020-0146 - lifetime erasure in generic-array.

### 2.48.0

CVE-2019-20446 - guard against exponential growth of CPU time
from malicious SVGs.

### 2.46.5

RUSTSEC-2020-0146 - lifetime erasure in generic-array.

CVE-2021-25900 - RUSTSEC-2021-0003 - buffer overflow in smallvec.

See notes below on libcroco.

### 2.44.17

RUSTSEC-2020-0146 - lifetime erasure in generic-array.

CVE-2019-15554 - RUSTSEC-2019-0012 - memory corruption in smallvec.

CVE-2019-15551 - RUSTSEC-2019-0009 - double-free and use-after-free in smallvec.

CVE-2021-25900 - RUSTSEC-2021-0003 - buffer overflow in smallvec.

See notes below on libcroco.

### 2.44.16

CVE-2019-20446 - guard against exponential growth of CPU time
from malicious SVGs.

See notes below on libcroco.

### 2.42.8

CVE-2019-20446 - guard against exponential growth of CPU time
from malicious SVGs.

See notes below on libcroco.

### 2.42.9

CVE-2018-20991 - RUSTSEC-2018-0003 - double-free in smallvec.

See notes below on libcroco.

### 2.40.21

CVE-2019-20446 - guard against exponential growth of CPU time
from malicious SVGs.

See notes below on libcroco.

### 2.40.18

CVE-2017-11464 - Fix division-by-zero in the Gaussian blur code.

See notes below on libcroco.

### Earlier releases should be avoided and are not listed here.

**Important note on libcroco:** Note that librsvg 2.46.x and earlier use
[libcroco](https://gitlab.gnome.org/Archive/libcroco/) for parsing
CSS, but that library is deprecated, unmaintained, and has open CVEs as
of May 2021.

If your application processes untrusted data, please avoid using
librsvg 2.46.x or earlier.  The first release of librsvg that does not
use libcroco is 2.48.0.

# Librsvg's dependencies

Librsvg depends on the following libraries implemented in memory-unsafe languages:

* **libxml2** - loading XML data.
* **cairo** - 2D rendering engine.
* **gdk-pixbuf** - decoding raster images like JPEG/PNG.
* **freetype2** - font renderer.
* **harfbuzz** - text shaping engine.

And of course, their recursive dependencies as well, such as **glib/gio**.

## Security considerations for libxml2

Librsvg uses the following configuration for the SAX2 parser in libxml2:

 * `XML_PARSE_NONET` - forbid network access.
 * `XML_PARSE_BIG_LINES` - store big line numbers.

As a special case, librsvg enables `replaceEntities` in the
`_xmlParserCtxtPtr` struct so that libxml2 will expand references only
to internal entities declared in the DTD subset.  External entities
are disabled.

For example, the following document renders two rectangles that are
expanded from internal entities:

```
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1 Basic//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11-basic.dtd" [
  <!ENTITY Rect1 "<rect x='15' y='10' width='20' height='30' fill='blue'/>">
  <!ENTITY Rect2 "<rect x='10' y='5' width='10' height='20' fill='green'/>">
]>
<svg xmlns="http://www.w3.org/2000/svg" width="60" height="60">
  &Rect1;
  &Rect2;
</svg>
```

However, an external entity like

```
  <!ENTITY foo SYSTEM "foo.xml">
```

will generate an XML parse error and the document will not be loaded.

## Security considerations for Cairo

Cairo is easy to crash if given coordinates that fall outside the
range of its 24.8 fixed-point numbers.  Librsvg is working on
mitigating this.

## Security considerations for gdk-pixbuf

Gdk-pixbuf depends on **libpng**, **libjpeg**, and other libraries for
different image formats.

# Security considerations for librsvg

**Built-in limits:** Librsvg has built-in limits for the following:

* Limit on the maximum number of loaded XML elements, set to 1,000,000
  (one million).  SVG documents with more than this number of elements
  will fail to load.  This is a mitigation for malicious documents
  that would otherwise consume large amounts of memory, for example by
  including a huge number of `<g/>` elements with no useful content.
  This is set in the file `src/limits.rs` in the `MAX_LOADED_ELEMENTS`
  constant.

* Limit on the maximum number of referenced elements while rendering.
  The `<use>` element in SVG and others like `<pattern>` can reference
  other elements in the document.  Malicious documents can cause an
  exponential number of references to be resolved, so librsvg places a
  limit of 500,000 references (half a million) to avoid unbounded
  consumption of CPU time.  This is set in the file `src/limits.rs` in
  the `MAX_REFERENCED_ELEMENTS` constant.

Librsvg has no built-in limits on the total amount of memory or CPU
time consumed to process a document.  Your application may want to
place limits on this, especially if it processes untrusted SVG
documents.

**Processing external files:** Librsvg processes references to
external files by itself: XML XInclude, `xlink:href` attributes, etc.
Please see the section "Security and locations of referenced files" in
the [developer's
documentation](https://developer-old.gnome.org/rsvg/unstable/RsvgHandle.html)
to see what criteria is used to accept or reject a file based on its
location.  If your application has more stringent requirements, it may
need to sandbox its use of librsvg.

**SVG features:** Librsvg ignores animations, scripts, and events
declared in SVG documents.  It always handles referenced images,
similar to SVG's [static processing
mode](https://www.w3.org/TR/SVG2/conform.html#static-mode).

