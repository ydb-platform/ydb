GdkPixbuf: Image loading library
================================

GdkPixbuf is a library that loads image data in various formats and stores
it as linear buffers in memory. The buffers can then be scaled, composited,
modified, saved, or rendered.

GdkPixbuf can load image data encoded in different formats, such as:

 - PNG
 - JPEG
 - TIFF
 - TGA
 - GIF

Additionally, you can write a GdkPixbuf loader module and install it into
a well-known location, in order to load a file format.

GdkPixbuf is used by the [GTK](https://www.gtk.org) toolkit for loading
graphical assets.

**NOTE**: GdkPixbuf should be used to load UI assets in trusted scenarios;
you should not be using it to load arbitrary images. We recommend using
projects like [Glycin](https://gitlab.gnome.org/GNOME/glycin) instead.

## Building GdkPixbuf

### Requirements

In order to build GdkPixbuf you will need to have installed:

 - [Meson](http://mesonbuild.com)
 - A C99-compliant compiler and toolchain
 - [GLib's development files](https://gitlab.gnome.org/GNOME/glib/)

Depending on the image formats you want to support you will also need:

 - libpng's development files
 - libjpeg's development files
 - libtiff's development files

Additionally, you may need:

 - [shared-mime-info](https://freedesktop.org/wiki/Software/shared-mime-info/)
 - [GObject Introspection](https://gitlab.gnome.org/GNOME/gobject-introspection/)
 - [GI-DocGen](https://gitlab.gnome.org/ebassi/gi-docgen/)
 - mediaLib's development files

### Building and installing

You should use Meson to configure GdkPixbuf's build, and depending on the
platform you will be able to use Ninja, Visual Studio, or XCode to build
the project; typically, on most platforms, you should be able to use the
following commands to build and install GdkPixbuf in the default prefix:

```sh
$ meson setup _build .
$ meson compile -C _build
$ meson install -C _build
```

You can use Meson's `--prefix` argument to control the installation prefix
at configuration time.

You can also use `meson configure` from within the build directory to
check the current build configuration, and change its options.

#### Build options

You can specify the following options in the command line to `meson`:

 * `-Dgtk_doc=true` - Build the API reference documentation
 * `-Drelocatable=true` - Enable application bundle relocation support

For a complete list of build-time options, see the file
[`meson_options.txt`](meson_options.txt).  You can read about Meson
options in general [in the Meson manual](http://mesonbuild.com/Build-options.html).

## Running tests

You can run the test suite by running `meson test -C _build`, where
`_build` is the build directory you used during the build stage.

## License

GdkPixbuf is released under the terms of the GNU Lesser General Public
License version 2.1, or, at your option, any later version. See the
[COPYING](./COPYING) file for further details.
