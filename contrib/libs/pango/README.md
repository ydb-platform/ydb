Pango
=====

Pango is a library for layout and rendering of text, with an emphasis
on internationalization. Pango can be used anywhere that text layout
is needed; however, most of the work on Pango so far has been done using
the GTK widget toolkit as a test platform. Pango forms the core of text
and font handling for GTK.

Pango is designed to be modular; the core Pango layout can be used
with different font backends. There are three basic backends, with
multiple options for rendering with each.

- Client-side fonts using the FreeType and FontConfig libraries.
  Rendering can be with with Cairo or Xft libraries, or directly
  to an in-memory buffer with no additional libraries.
- Native fonts on Microsoft Windows. Rendering can be done via Cairo
  or directly using the native Win32 API.
- Native fonts on MacOS X with the CoreText framework, rendering via
  Cairo.

The integration of Pango with [Cairo](https://cairographics.org)
provides a complete solution with high quality text handling and
graphics rendering.

As well as the low level layout rendering routines, Pango includes
PangoLayout, a high level driver for laying out entire blocks of text,
and routines to assist in editing internationalized text.

For more information about Pango, see:

 https://www.pango.org/

Dependencies
------------
Pango depends on the GLib library; more information about GLib can
be found at https://www.gtk.org/.

To use the Free Software stack backend, Pango depends on the following
libraries:

- [FontConfig](https://www.fontconfig.org) for font discovery,
- [FreeType](https://www.freetype.org) for font access,
- [HarfBuzz](http://www.harfbuzz.org) for complex text shaping
- [fribidi](http://fribidi.org) for bidirectional text handling

Cairo support depends on the [Cairo](https://cairographics.org) library.
The Cairo backend is the preferred backend to use Pango with and is
subject of most of the development in the future.  It has the
advantage that the same code can be used for display and printing.

We suggest using Pango with Cairo as described above, but you can also
do X-specific rendering using the Xft library. The Xft backend uses
version 2 of the Xft library to manage client side fonts. Version 2 of
Xft is available from https://xlibs.freedesktop.org/release/.  You'll
need the libXft package, and possibly the libXrender and renderext
packages as well.  You'll also need FontConfig.

Installation of Pango on Win32 is possible, see README.win32.

License
-------
Most of the code of Pango is licensed under the terms of the
GNU Lesser Public License (LGPL) - see the file COPYING for details.

Versioning
----------

Historically, Pango was following the traditionally even/odd library
versioning scheme where stable releases are marked by even minor
and development releases by an odd minor.

In recent years, Pango development has slowed down so much that it
no longer makes sense to have unstable cycles, or even unstable releases.
Going forward, pango versions will simply be increasing triples, with
no particular significance to the parity of the minor version.
