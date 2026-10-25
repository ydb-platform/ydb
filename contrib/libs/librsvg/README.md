# Librsvg

This is librsvg - A small library to render Scalable Vector Graphics
([SVG][svg]), associated with the [GNOME Project][gnome].  It renders
SVG files to [Cairo][cairo] surfaces.  Cairo is the 2D, antialiased
drawing library that GNOME uses to draw things to the screen or to
generate output for printing.

Do you want to render non-animated SVGs to a Cairo surface with a
minimal API?  Librsvg may be adequate for you.

**Supported SVG/CSS features:** Please see the chapter for [supported
features][features] in the development guide.

***PLEASE DO NOT SEND PULL REQUESTS TO GITHUB.*** We use
[`gitlab.gnome.org`](https://gitlab.gnome.org/GNOME/librsvg) instead.
Please see [Contributing to librsvg][contributing] for details.

Table of contents:

[[_TOC_]]

## Supported branches

Only these versions are supported:

* 2.61.x
* 2.62.x

Older versions are not supported.  Please try a newer version before
reporting bugs or missing features.

See the [policy for supported versions][versions] for more details.

* [Release archive](https://gitlab.gnome.org/GNOME/librsvg/-/releases) in gitlab.gnome.org
* [Source tarballs for download](https://download.gnome.org/sources/librsvg/) at download.gnome.org

## Stable release series

Since librsvg 2.55.x, all release streams are considered stable, not
just ones with an even minor version number.  A micro version starting
at 90 means a beta release.  For example:

* 2.55.0, 2.55.1, etc. are stable releases in the 2.55 series.
* 2.55.90, 2.55.91 are the first two beta releases before the stable 2.56.0

See the [policy for supported versions][versions] for more details.

## Using librsvg

* [C API documentation][c-docs]
* [Rust API documentation][rust-docs]

**Compiling:** Librsvg uses the [meson] build system.  Compile-time
options are listed in the file [`meson_options.txt`][meson_options].
Please refer to the [Detailed compilation instructions][compiling] in
the development guide.

**Documentation:** You can read the documentation for librsvg's [C
API][c-docs] or the [Rust API][rust-docs].  Please [file an
issue][reporting-bugs] if you don't find something there that you
need.

**Bug tracking:** If you have found a bug, take a look at [our bug
tracker][bugs].  Please see the "[reporting bugs][reporting-bugs]"
page in the development guide to see how to provide a good bug report.

**Asking questions:** Feel free to ask questions about using librsvg
in the "Platform" category of [GNOME's Discourse][discourse].  You can
also ask via chat in the Matrix room for [GNOME Rust][gnome-rust].

**Programming languages:** Librsvg exports its API through [GObject
Introspection][gi].  This way, it is available in many programming
languages other than C. Please see your language binding's
documentation for information on how to load the `Rsvg` namespace.

**Security:** For a list of releases with security issues,
instructions on reporting security-related bugs, and the security
considerations for librsvg's dependencies, see the [Security
chapter][security] in the development guide.

[c-docs]: https://gnome.pages.gitlab.gnome.org/librsvg/Rsvg-2.0/index.html
[rust-docs]: https://gnome.pages.gitlab.gnome.org/librsvg/doc/rsvg/index.html

## Contributing to librsvg's development

There is a code of conduct for contributors to librsvg; please see the
[GNOME Code of Conduct][coc], which is duplicated in the file
[`code-of-conduct.md`][coc-local].

Please see the [Development Guide for librsvg][devel-guide] on how to
contribute to librsvg, how to report bugs, how set up your development
environment, and for a description of librsvg's architecture.

## Goals of librsvg

Librsvg aims to be a low-footprint library for rendering SVG1.1 and SVG2 images.
It is used primarily in the [GNOME project](https://www.gnome.org) to
render SVG icons and vector images that appear on the desktop.  It is
also used in Wikimedia to render the SVG images that appear in
Wikipedia, so that even old web browsers can display them.  Many
projects which casually need to render static SVG images use librsvg.

We aim to be a "render this SVG for me, quickly, and with a minimal
API" kind of library.

Feature additions will be considered on a case-by-case basis.

You can read about librsvg's [supported SVG and CSS features][features] in the
development guide.

## Non-goals of librsvg

We don't aim to:

* Implement every single SVG feature that is in the spec.

* Implement scripting or external access to the SVG's DOM.

* Implement support for CSS-based animations (but if you can think of a nice API
  to do this, we would be glad to know!)

* Replace the industrial-strength SVG rendering machinery in modern web
  browsers.

Of course, [contributions are welcome][contributing].  In particular,
if you find nice ways of doing the above while still maintaining the
existing API of librsvg, we would love to know about it!

## Who uses librsvg?

Librsvg is part of the [GNOME platform][platform].  Inside GNOME,
librsvg takes multiple roles:

* Loads SVGs from the generic gdk-pixbuf loader infrastructure, so any
  application which uses gdk-pixbuf can load SVGs as if they were raster images.

* Loads SVG icons for the desktop.

* Creates SVG thumbnails for the file manager.

* Loads SVGs within GNOME's default image viewer, Eye of Gnome.

Outside of GNOME's core:

* GNOME games (chess, five-or-more, etc. to draw game pieces)

* GIMP

* GCompris

* Claws-mail

* Darktable

* Mate-panel

* Evas/Enlightenment

* Emacs

* ImageMagick

* Wikipedia, to render SVGs as raster images for old browsers.
  *Special thanks to Wikimedia for providing excellent bug reports.*

## Presentations on librsvg

"[Replacing C library code with Rust: What I learned with
librsvg][guadec-presentation-1]" was presented at GUADEC 2017.  It gives
a little history of librsvg, and how/why it is being ported to Rust
from C.

"[Patterns of refactoring C to Rust: the case of
librsvg][guadec-presentation-2]" was presented at GUADEC 2018.  It
describes ways in which librsvg's C code was refactored to allow
porting it to Rust.

## Maintainers

The maintainer of librsvg is [Federico Mena Quintero][federico].  Feel
free to contact me for any questions you may have about librsvg, both
its usage and its development.  You can contact me in the following
ways:

* [Mail me][mail] at <federico@gnome.org>.

* Matrix: I am `@federico` on the [GNOME Hackers][gnome-hackers] and
  [Rust ❤️ GNOME][gnome-rust] channels on gnome.org's Matrix. I'm there most
  weekdays (Mon-Fri) starting at about UTC 14:00 (that's 08:00 my time; I am in
  the UTC-6 timezone). If this is not a convenient time for you, feel free to
  [mail me][mail] and we can arrange a time.

* I frequently [blog about librsvg][blog]. You may be interested in the articles
  about porting librsvg from C to Rust, which happened between 2016 and 2020.

[svg]: https://en.wikipedia.org/wiki/Scalable_Vector_Graphics
[gnome]: https://www.gnome.org/
[cairo]: https://www.cairographics.org/
[coc]: https://conduct.gnome.org
[coc-local]: code-of-conduct.md
[meson]: https://mesonbuild.com
[meson_options]: meson_options.txt
[compiling]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/compiling.html
<!-- rumdl doesn't like mailto: -->
<!-- rumdl-disable-next-line MD034 -->
[mail]: mailto:federico@gnome.org
[bugs]: https://gitlab.gnome.org/GNOME/librsvg/issues
[gi]: https://gi.readthedocs.io/en/latest/
[contributing]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/contributing.html
[reporting-bugs]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/bugs.html
[discourse]: https://discourse.gnome.org/c/platform/5
[federico]: https://viruta.org/
[platform]: https://developer.gnome.org/
[guadec-presentation-1]: https://viruta.org/docs/fmq-porting-c-to-rust.pdf
[guadec-presentation-2]: https://viruta.org/docs/fmq-refactoring-c-to-rust.pdf
[gnome-hackers]: https://matrix.to/#/#gnome-hackers:gnome.org
[gnome-rust]: https://matrix.to/#/#rust:gnome.org
[devel-guide]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/index.html
[security]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/security.html
[features]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/features.html
[versions]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/supported_versions.html
[blog]: https://viruta.org/tag/librsvg.html
