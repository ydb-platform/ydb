# Librsvg

This is librsvg - A small library to render Scalable Vector Graphics
([SVG][svg]), associated with the [GNOME Project][gnome].  It renders
SVG files to [Cairo][cairo] surfaces.  Cairo is the 2D, antialiased
drawing library that GNOME uses to draw things to the screen or to
generate output for printing.

Do you want to render non-animated SVGs to a Cairo surface with a
minimal API?  Librsvg may be adequate for you.

**Supported SVG/CSS features:** Please see the [FEATURES.md](FEATURES.md) file.

***PLEASE DO NOT SEND PULL REQUESTS TO GITHUB.***  We use
[`gitlab.gnome.org`](https://gitlab.gnome.org/GNOME/librsvg) instead.
Please see [`CONTRIBUTING.md`][contributing] for details.

Table of contents:

[[_TOC_]]

# Using librsvg

* [C API documentation][c-docs]
* [Rust API documentation][rust-docs]

**Compiling:** Librsvg uses a mostly normal [autotools] setup.  You
may run into some peculiarities due to the Rust internals library if
you are **cross-compiling** or if you are in a **build system with no
network access**, or if you are **building binary packages from a
librsvg tarball**.  In those cases, please refer to the
[`COMPILING.md`][compiling] file.

**Documentation:** You can read the documentation for librsvg's [C
API][c-docs] or the [Rust API][rust-docs].  Please [file an
issue][reporting-bugs] if you don't find something there that you
need.

**Bug tracking:** If you have found a bug, take a look at [our bug
tracker][bugs].  Please see the "[reporting bugs][reporting-bugs]"
section in the file [CONTRIBUTING.md][contributing] to see how to
provide a good bug report.

**Asking questions:** Feel free to ask questions about using librsvg
in the [desktop-devel-list][d-d-l] mailing list.

**Programming languages:** Librsvg exports its API through [GObject
Introspection][gi].  This way, it is available in many programming
languages other than C.  Please see your language binding's
documentation for information on how to load the `Rsvg` namespace.

[c-docs]: https://gnome.pages.gitlab.gnome.org/librsvg/Rsvg-2.0/index.html
[rust-docs]: https://gnome.pages.gitlab.gnome.org/librsvg/doc/librsvg/index.html

# Contributing to librsvg's development

There is a code of conduct for contributors to librsvg; please see the
file [`code-of-conduct.md`][coc].

For information on how to report bugs, or how to contribute to librsvg
in general, please see the file [`CONTRIBUTING.md`][contributing].

# Goals of librsvg

Librsvg aims to be a low-footprint library for rendering SVG1.1 and SVG2 images.
It is used primarily in the [GNOME project](https://www.gnome.org) to
render SVG icons and vector images that appear on the desktop.  It is
also used in Wikimedia to render the SVG images that appear in
Wikipedia, so that even old web browsers can display them.  Many
projects which casually need to render static SVG images use librsvg.

We aim to be a "render this SVG for me, quickly, and with a minimal
API" kind of library.

Feature additions will be considered on a case-by-case basis.

You can read about librsvg's supported SVG and CSS features in the
[FEATURES.md](FEATURES.md) file.

# Non-goals of librsvg

We don't aim to:

* Implement every single SVG feature that is in the spec.

* Implement scripting or external access to the SVG's DOM.

* Implement support for CSS-based animations (but if you can think of
  a nice API to do this, we would be glad to know!)

* Replace the industrial-strength SVG rendering machinery in modern
  web browsers.

Of course, [contributions are welcome][contributing].  In particular,
if you find nice ways of doing the above while still maintaining the
existing API of librsvg, we would love to know about it!

# Who uses librsvg?

Librsvg is part of the [GNOME platform][platform].  Inside GNOME,
librsvg takes multiple roles:

* Loads SVGs from the generic gdk-pixbuf loader infrastructure, so any
  application which uses gdk-pixbuf can load SVGs as if they were
  raster images.

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


# Presentations on librsvg

"[Replacing C library code with Rust: What I learned with
librsvg][guadec-presentation-1]" was presented at GUADEC 2017.  It gives
a little history of librsvg, and how/why it is being ported to Rust
from C.

"[Patterns of refactoring C to Rust: the case of
librsvg][guadec-presentation-2]" was presented at GUADEC 2018.  It
describes ways in which librsvg's C code was refactored to allow
porting it to Rust.


# Maintainers

The maintainer of librsvg is [Federico Mena Quintero][federico].  Feel
free to contact me for any questions you may have about librsvg, both
its usage and its development.  You can contact me in the following
ways:

* [Mail me][mail] at federico@gnome.org.

* IRC: I am `federico` on `irc.gnome.org` in the `#rust` or
  `#gnome-hackers` channels.  I'm there most weekdays (Mon-Fri)
  starting at about UTC 14:00 (that's 08:00 my time; I am in the UTC-6
  timezone).  If this is not a convenient time for you, feel free to
  [mail me][mail] and we can arrange a time.

[svg]: https://en.wikipedia.org/wiki/Scalable_Vector_Graphics
[gnome]: https://www.gnome.org/
[cairo]: https://www.cairographics.org/
[coc]: code-of-conduct.md
[autotools]: https://autotools.io/index.html
[compiling]: COMPILING.md
[mail]: mailto:federico@gnome.org
[bugs]: https://gitlab.gnome.org/GNOME/librsvg/issues
[gi]: https://wiki.gnome.org/Projects/GObjectIntrospection
[contributing]: CONTRIBUTING.md
[reporting-bugs]: CONTRIBUTING.md#reporting-bugs
[d-d-l]: https://mail.gnome.org/mailman/listinfo/desktop-devel-list
[federico]: https://people.gnome.org/~federico/
[platform]: https://developer.gnome.org/
[guadec-presentation-1]: https://people.gnome.org/~federico/blog/docs/fmq-porting-c-to-rust.pdf
[guadec-presentation-2]: https://people.gnome.org/~federico/blog/docs/fmq-refactoring-c-to-rust.pdf
