## Preface

The Pango backend written for Win32 is PangoWin32. PangoWin32 uses
the Win32 GDI font API. GTK+ 2.8 and later on Win32 however actually
uses the PangoCairo backend (which then uses only small parts of
PangoWin32). Much of the GDI/DirectWrite font API calls are in Cairo.

The PangoFt2 backend was originally written with Win32 in mind, but
its main use nowadays is on other platforms than Win32.

## ToolChain Requirement for Building Pango for Win32

You need to have gcc (mingw-w64) or Visual Studio 2015 or later (Visual
Studio 2019 or later is recommended if introspection files are being built),
along with Python 3.7.x+ and Meson 0.60.0 at the time of writing. If building
the introspection files, you will also need to ensure that the Python
release series and build architecture (i.e. Win32 (x86)/ x64 (amd64/x86-64)/ARM64
(aarch64)) matches the Python release series and build architecture that was used
to build GObject-Introspection is installed onthe system.

Cross-compiling Pango for ARM64/aarch64 with Visual Studio 2017 or later is
also supported, provided if a cross compilation file for Meson is properly
setup for such builds (please refer to the Meson documentation for further
instructions). Support for introspection for cross-compiled ARM64 builds is
currently not supported, as introspection builds are currently not supported
for any cross-builds where the compiled code cannot run on the build system.

The Ninja build utility must also be in your PATH, unless using the
Visual Studio IDE as noted [below](#building-pango).

## Dependencies

You will also need the following libraries installed with their headers
and import libraries, and their DLLs, if applicable, needs to be found in
%PATH%.  All of their required dependencies are required as well.  Their
pkg-config files are needed on all builds unless otherwise noted, where
only their headers/import libraries are needed on Visual Studio (please also read
[this section](#additional-notes-on-building-pangoft2) for more info for enabling
PangoFt2):

* GLib
* Fribidi
* Cairo (also via headers and libs; with Win32 support built in, and FreeType and
FontConfig support built in if building PangoFT2; Cairo 1.18.0 or later is highly
recommended)
* HarfBuzz (GLib support required, and FreeType2 support if building PangoFT2;
for Visual Studio builds, this needs to be built with Visual Studio 2017 15.9.x or
later--if building introspection files, Visual Studio 2019 or later is highly
recommended)
* FreeType (needed if building PangoFT2)
* FontConfig (needed if building PangoFT2)

Please see `meson.build` to see what versions are needed for these dependencies.

## Building Pango

1. Invoke the Meson configuration as follows, in a directory separate from the
sources:

    1.1. MinGW (please adjust the paths accordingly, in a MSYS/MSYS2 bash prompt):
    ```
    PATH=/devel/dist/glib-2.8.0/bin:$PATH ACLOCAL_FLAGS="-I /devel/dist/glib-2.8.0/share/aclocal"  CC='gcc -mtune=pentium3' CPPFLAGS='-I/opt/gnu/include' LDFLAGS='-L/opt/gnu/lib' CFLAGS=-O meson setup $(pango_root_srcdir) --buildtype=$(buildtype) --prefix=$(PREFIX) [--pkg-config-path=...]
    ```
 
    1.2 Visual Studio (set the `%INCLUDE%`, `%LIB%`, `%PATH%` envvars as needed
    before running the following in a Visual Studio command prompt)

    ```
    meson $(pango_root_srcdir) --buildtype=$(buildtype) --prefix=$(PREFIX) [--pkg-config-path=...]
    ```

    For Visual Studio builds, support for building using Visual Studio Project
    Files is also supported to some extent.  Append `--backend=vs` to the Meson
    configuration command above to use this support.  Note that this support
    may not work as well as the builds that are carried out by `ninja`, and
    issues in regards to building with Meson using Visual Studio Project files
	(i.e. `msbuild`) should be reported to the Meson project.

2. Build Pango by running Ninja (or with the generated Visual Studio project
files).
	
3. Run tests and/or install the build using the `test` and `install` targets. To
run the tests, you may need to have GNU `diff` in your `%PATH%`, that can be
obtained via installing MSYS2 for Visual Studio users (the path where GNU `diff`
is located should be towards the end of your `%PATH%`).

## Additional notes on building PangoFt2
Please take note of the following items when attempting to build with PangoFt2 on
Windows, in addition to the things mentioned [here](#dependencies), when invoking
Meson to configure the build (builds of Cairo, HarfBuzz and FreeType as subprojects
will be built consistent with the descriptions below):
* Pango: You need to pass in `-Dfontconfig=enabled -Dfreetype=enabled` when
building Pango itself, as FontConfig/FreeType support needs to be explicitly
enabled for Windows builds, that is used to build PangoFt2.
* Cairo: Cairo needs to be built with FreeType *and* FontConfig support, which
must be explicitly enabled as they are disabled by default, in particular when
building Cairo 1.17.x or later, which is also done by Meson. Pass in
`-Dfontconfig=enabled -Dfreetype=enabled` when building Cairo.
* HarfBuzz: HarfBuzz needs to be built with FreeType support enabled, so ensure
that is true by using `-Dfreetype=enabled`.
* FreeType: If Pango is being built without FreeType *and* HarfBuzz
found, FreeType will be built without HarfBuzz support. In this case, if building
FreeType with HarfBuzz is desired, first build and install PangoFt2 along with
FreeType and HarfBuzz, and then do a separate standalone build of FreeType against
that PangoFt2 build, and you will have your FreeType build with HarfBuzz enabled.
This is consistent with the official instructions for building FreeType that has
HarfBuzz enabled, from scratch.

See the following GNOME Live! page for a more detailed description of building
Pango's dependencies with Visual Studio:

https://wiki.gnome.org/Projects/GTK/Win32/MSVCCompilationOfGTKStack
