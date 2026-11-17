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

This is the `README.md` file for the Rust crate; you may want to see
the [main
README.md](https://gitlab.gnome.org/GNOME/librsvg/-/blob/main/README.md)
for the whole project.

## Using librsvg

* [Rust API documentation][rust-docs]
* [Release notes][release-notes]

**Non-Rust dependencies:**

Librsvg depends on a number of non-Rust libraries that must be installed on your
system. They need to have a pkg-config `.pc` file installed so that librsvg's
compilation can find them via [system-deps][system-deps]:

* Cairo - used for the main rendering
* FreeType2 - font renderer
* gio/glib - I/O primitives and streams
* Harfbuzz - text shaping
* libxml2 - XML parser
* Pangocairo - text rendering
* PangoFT2 - render text via Pango and FreeType2
* Fontconfig - system fonts and rules for using them

There are some [security considerations][sec-libs] for these non-Rust
libraries, which you may want to read.

[system-deps]: https://github.com/gdesmott/system-deps
[sec-libs]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/security.html#librsvgs-dependencies

**Rust dependencies:** Librsvg uses a bunch of Rust crates to handle
the many aspects of SVG and CSS.  Of particular interest are the
[image-rs][image-rs] crate and its dependencies, which librsvg uses to
load raster images.

**Bug tracking:** If you have found a bug, take a look at [our bug
tracker][bugs].  Please see the "[reporting bugs][reporting-bugs]"
page in the development guide to see how to provide a good bug report.

**Asking questions:** Feel free to ask questions about using librsvg
in the "Platform" category of [GNOME's Discourse][discourse].  You can
also ask via chat in the Matrix room for [GNOME Rust][gnome-rust].

**Security:** For a list of releases with security issues,
instructions on reporting security-related bugs, and the security
considerations for librsvg's dependencies, see the [Security
chapter][security] in the development guide.

[rust-docs]: https://gnome.pages.gitlab.gnome.org/librsvg/doc/rsvg/index.html
[release-notes]: https://gitlab.gnome.org/GNOME/librsvg/-/blob/main/NEWS

## Contributing to librsvg's development

There is a code of conduct for contributors to librsvg; please see the
[GNOME Code of Conduct][coc].

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
[contributing]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/contributing.html
[coc]: https://conduct.gnome.org
[devel-guide]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/index.html
[features]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/features.html
[security]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/security.html
[bugs]: https://gitlab.gnome.org/GNOME/librsvg/issues
[reporting-bugs]: https://gnome.pages.gitlab.gnome.org/librsvg/devel-docs/bugs.html
[discourse]: https://discourse.gnome.org/c/platform/5
[gnome-rust]: https://matrix.to/#/#rust:gnome.org
[platform]: https://developer.gnome.org/
[guadec-presentation-1]: https://viruta.org/docs/fmq-porting-c-to-rust.pdf
[guadec-presentation-2]: https://viruta.org/docs/fmq-refactoring-c-to-rust.pdf
[federico]: https://viruta.org/
<!-- rumdl doesn't like mailto: -->
<!-- rumdl-disable-next-line md034 -->
[mail]: mailto:federico@gnome.org
[gnome-hackers]: https://matrix.to/#/#gnome-hackers:gnome.org
[blog]: https://viruta.org/tag/librsvg.html
[image-rs]: https://github.com/image-rs/image
