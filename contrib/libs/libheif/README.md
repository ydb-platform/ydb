# libheif

[![Build Status](https://github.com/strukturag/libheif/workflows/build/badge.svg)](https://github.com/strukturag/libheif/actions) [![Build Status](https://ci.appveyor.com/api/projects/status/github/strukturag/libheif?svg=true)](https://ci.appveyor.com/project/strukturag/libheif) [![Coverity Scan Build Status](https://scan.coverity.com/projects/16641/badge.svg)](https://scan.coverity.com/projects/strukturag-libheif)

libheif is an ISO/IEC 23008-12 HEIF and AVIF (AV1 Image File Format) file format decoder and encoder.
HEIC and AVIF are new image file formats employing HEVC (H.265) or AV1 image coding, respectively, for the
best compression ratios currently possible.

On top of HEIC and AVIF, libheif also supports HEIF images coded with VVC, AVC, JPEG, JPEG-2000, and ISO/IEC 23001-17.
The ISO/IEC 23001-17 codec is built-in to libheif and allows to store lossless images and video in many different formats.

libheif makes use of various codec libraries for implementing each compression format.
For HEIC, [libde265](https://github.com/strukturag/libde265) is used by default for decoding and x265 for encoding.
For AVIF, libaom, dav1d, svt-av1, or rav1e are used as codecs.
libheif can be built with a subset of the supported codecs to keep the size and the number of dependencies low.
Alternatively, the libheif codecs can also be built as separate plugins that can be installed and loaded dynamically when used.

## Supported features

libheif has support for:

* HEIC, AVIF, VVC, AVC, JPEG-in-HEIF, JPEG2000, uncompressed (ISO/IEC 23001-17:2024) codecs
* alpha channels, depth maps, thumbnails, auxiliary images
* multiple images in a file
* HEIF image sequences and MP4 video, including alpha channels
* tiled images with decoding individual tiles and encoding tiled images by adding tiles one after another
* HDR images, correct color transform according to embedded color profiles
* image transformations (crop, mirror, rotate), overlay images
* plugin interface to add alternative codecs
* reading EXIF and XMP metadata
* region annotations and mask images
* streaming of images and video by requesting data from the network through a data-reader interface

Supported codecs:
| Format       |  Decoders           |  Encoders                    |
|:-------------|:-------------------:|:----------------------------:|
| HEIC         | libde265, ffmpeg    | x265, kvazaar                |
| AVIF         | libaom, dav1d       | libaom, rav1e, svt-av1       |
| VVC          | vvdec               | vvenc, uvg266                |
| AVC          | openh264, ffmpeg    | x264                         |
| JPEG         | libjpeg(-turbo)     | libjpeg(-turbo)              |
| JPEG2000     | OpenJPEG            | OpenJPEG                     |
| HTJ2K        | OpenJPEG            | OpenJPH                      |
| uncompressed | built-in            | built-in                     |

## Programming API

The library has a C API for easy integration and wide language support.

The decoder automatically supports both HEIF and AVIF (and the other compression formats) through the same API. The same decoding code can be used to decode any of them.
The encoder can be switched between HEIF and AVIF simply by setting `heif_compression_HEVC` or `heif_compression_AV1`
to `heif_context_get_encoder_for_format()`, or using any of the other compression formats.

### Loading the primary image from a HEIF file

```C
heif_context* ctx = heif_context_alloc();
heif_context_read_from_file(ctx, input_filename, nullptr);

// get a handle to the primary image
heif_image_handle* handle;
heif_context_get_primary_image_handle(ctx, &handle);

// decode the image and convert colorspace to RGB, saved as 24bit interleaved
heif_image* img;
heif_decode_image(handle, &img, heif_colorspace_RGB, heif_chroma_interleaved_RGB, nullptr);

int stride;
const uint8_t* data = heif_image_get_plane_readonly(img, heif_channel_interleaved, &stride);

// ... process data as needed ...

// clean up resources
heif_image_release(img);
heif_image_handle_release(handle);
heif_context_free(ctx);
```

### Writing a HEIF file

```C
heif_context* ctx = heif_context_alloc();

// get the default encoder
heif_encoder* encoder;
heif_context_get_encoder_for_format(ctx, heif_compression_HEVC, &encoder);

// set the encoder parameters
heif_encoder_set_lossy_quality(encoder, 50);

// encode the image
heif_image* image; // code to fill in the image omitted in this example
heif_context_encode_image(ctx, image, encoder, nullptr, nullptr);

heif_encoder_release(encoder);

heif_context_write_to_file(ctx, "output.heic");

heif_context_free(ctx);
```

### Get the EXIF data from a HEIF file

```C
heif_item_id exif_id;

int n = heif_image_handle_get_list_of_metadata_block_IDs(image_handle, "Exif", &exif_id, 1);
if (n==1) {
  size_t exifSize = heif_image_handle_get_metadata_size(image_handle, exif_id);
  uint8_t* exifData = malloc(exifSize);
  struct heif_error error = heif_image_handle_get_metadata(image_handle, exif_id, exifData);
}
```

### Image sequences, MP4 video

See the [image sequences API documentation](https://github.com/strukturag/libheif/wiki/Reading-and-Writing-Sequences).

Since HEIF image sequences are very similar to MP4 video, libheif can also read and write MP4 video (without audio)
with all supported codecs.

### High-resolution tiled images

For very large resolution images, it is not always feasible to process the whole image.
In this case, `libheif` can process the image tile by tile.
See the [image tiling API documentation](https://github.com/strukturag/libheif/wiki/Reading-and-Writing-Tiled-Images).

### More documenation

See the header files for the complete C API.

There is also a C++ API which is a header-only wrapper to the C API.
Hence, you can use the C++ API and still be binary compatible.
Code using the C++ API is much less verbose than using the C API directly.


## Compiling

This library uses the CMake build system (the earlier autotools build files have been removed in v1.16.0).

For a minimal configuration, we recommend to use the codecs libde265 and x265 for HEIC and AOM for AVIF.
Make sure that you compile and install [libde265](https://github.com/strukturag/libde265)
first, so that the configuration script will find this.
Also install x265 and its development files if you want to use HEIF encoding, but note that x265 is GPL.
An alternative to x265 is kvazaar (BSD).

The basic build steps are as follows (--preset argument needs CMake >= 3.21):

````sh
mkdir build
cd build
cmake --preset=release ..
make
````

There are CMake presets to cover the most frequent use cases.

* `release`: the preferred preset which compiles all codecs as separate plugins.
  If you do not want to distribute some of these plugins (e.g. HEIC), you can omit packaging these.
* `release-noplugins`: this is a smaller, self-contained build of libheif without using the plugin system.
  A single library is built with support for HEIC and AVIF.
* `testing`: for building and executing the unit tests. Also the internal library symbols are exposed. Do not use for distribution.
* `fuzzing`: all codecs like in release build, but configured into a self-contained library with enabled fuzzers. The library should not distributed.

You can optionally adapt these standard configurations to your needs.
This can be done, for example, by calling `ccmake .` from within the `build` directory.

### CMake configuration variables

Libheif supports many different codecs. In order to reduce the number of dependencies and the library size,
you can choose which of these codecs to include. Each codec can be compiled either as built-in to the library
with a hard dependency, or as a separate plugin file that is loaded dynamically.

For each codec, there are two configuration variables:

* `WITH_{codec}`: enables the codec
* `WITH_{codec}_PLUGIN`: when enabled, the codec is compiled as a separate plugin.

In order to use dynamic plugins, also make sure that `ENABLE_PLUGIN_LOADING` is enabled.
The placeholder `{codec}` can have these values: `LIBDE265`, `X265`, `AOM_DECODER`, `AOM_ENCODER`, `SvtEnc`, `DAV1D`, `OpenH264`, `X264`, `FFMPEG_DECODER`, `JPEG_DECODER`, `JPEG_ENCODER`, `KVAZAAR`, `OpenJPEG_DECODER`, `OpenJPEG_ENCODER`, `OPENJPH_ENCODER`, `VVDEC`, `VVENC`, `UVG266`, `WEBCODECS`.

Further options are:

* `WITH_UNCOMPRESSED_CODEC`: enable support for uncompressed images according to ISO/IEC 23001-17:2024. This is *experimental*
   and not available as a dynamic plugin. When enabled, it adds a dependency to `zlib`, and optionally will use `brotli`.
* `WITH_HEADER_COMPRESSION`: enables support for compressed metadata. When enabled, it adds a dependency to `zlib`.
   Note that header compression is not widely supported yet.
* `WITH_LIBSHARPYUV`: enables high-quality YCbCr/RGB color space conversion algorithms (requires `libsharpyuv`,
   e.g. from the `third-party` directory).
* `ENABLE_EXPERIMENTAL_FEATURES`: enables functions that are currently in development and for which the API is not stable yet.
   When this is enabled, a header `heif_experimental.h` will be installed that contains this unstable API.
   Distributions that rely on a stable API should not enable this.
* `ENABLE_MULTITHREADING_SUPPORT`: can be used to disable any multithreading support, e.g. for embedded platforms.
* `ENABLE_PARALLEL_TILE_DECODING`: when enabled, libheif will decode tiled images in parallel to speed up compilation.
* `PLUGIN_DIRECTORY`: the directory where libheif will search for dynamic plugins when the environment
  variable `LIBHEIF_PLUGIN_PATH` is not set.
* `WITH_REDUCED_VISIBILITY`: only export those symbols into the library that are public API.
  Has to be turned off for running some tests.

### macOS

1. Install dependencies with Homebrew

    ```sh
    brew install cmake make pkg-config x265 libde265 libjpeg libtool
    ```

2. Configure and build project (--preset argument needs CMake >= 3.21):

    ```sh
    mkdir build
    cd build
    cmake --preset=release ..
    ./configure
    make
    ```

### Windows

You can build and install libheif using the [vcpkg](https://github.com/Microsoft/vcpkg/) dependency manager:

```sh
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.bat
./vcpkg integrate install
./vcpkg install libheif
```

The libheif port in vcpkg is kept up to date by Microsoft team members and community contributors. If the version is out of date, please [create an issue or pull request](https://github.com/Microsoft/vcpkg) on the vcpkg repository.

### Adding libaom encoder/decoder for AVIF

* Run the `aom.cmd` script in the `third-party` directory to download libaom and
  compile it.

When running `cmake` or `configure`, make sure that the environment variable
`PKG_CONFIG_PATH` includes the absolute path to `third-party/aom/dist/lib/pkgconfig`.

### Adding rav1e encoder for AVIF

* Install `cargo`.
* Install `cargo-c` by executing

```sh
cargo install --force cargo-c
```

* Run the `rav1e.cmd` script in the `third-party` directory to download rav1e
  and compile it.

When running `cmake`, make sure that the environment variable
`PKG_CONFIG_PATH` includes the absolute path to `third-party/rav1e/dist/lib/pkgconfig`.

### Adding dav1d decoder for AVIF

* Install [`meson`](https://mesonbuild.com/).
* Run the `dav1d.cmd` script in the `third-party` directory to download dav1d
  and compile it.

When running `cmake`, make sure that the environment variable
`PKG_CONFIG_PATH` includes the absolute path to `third-party/dav1d/dist/lib/x86_64-linux-gnu/pkgconfig`.

### Adding SVT-AV1 encoder for AVIF

You can either use the SVT-AV1 encoder libraries installed in the system or use a self-compiled current version.
If you want to compile SVT-AV1 yourself,

* Run the `svt.cmd` script in the `third-party` directory to download SVT-AV1
  and compile it.

You have to enable SVT-AV1 with CMake.

When running `cmake`, make sure that the environment variable
`PKG_CONFIG_PATH` includes the absolute path to `third-party/SVT-AV1/Build/linux/install/lib/pkgconfig`.
You may have to replace `linux` in this path with your system's identifier.

## Codec plugins

Starting with v1.14.0, each codec backend can be compiled statically into libheif or as a dynamically loaded plugin.
You can choose this individually for each codec backend in the CMake settings using `WITH_{codec}_PLUGIN` options.
Compiling a codec backend as dynamic plugin will generate a shared library that is installed in the system together with libheif.
The advantage is that only the required plugins have to be installed and libheif has fewer dependencies.

The plugins are loaded from the colon-separated (semicolon-separated on Windows) list of directories stored in the environment variable `LIBHEIF_PLUGIN_PATH`.
If this variable is empty, they are loaded from a directory specified in the CMake configuration.
You can also add plugin directories programmatically.

### Codec specific notes

* The FFMPEG decoding plugin can make use of h265 hardware decoders. However, it currently (v1.17.0, ffmpeg v4.4.2) does not work
  correctly with all streams. Thus, libheif still prefers the libde265 decoder if it is available.

* The "webcodecs" HEVC decoder can only be used in emscripten builds since it uses the web-browser's API. For the same reason, it is not available as a plugin.

## Usage

### Security limits

Libheif defines some security limits that prevent that very large images exceed the available memory or malicious input files can be used for a denial-of-service attack.
When you are programming against the libheif API and you need to process very large images, you can set the `heif_security_limits` individually.
When using `heif-dec`, there is the option to switch off security limits with `--disable-limits`.
In case a third-party software is using libheif, but does not give you a way to switch off the limits, you can set an environment variable `LIBHEIF_SECURITY_LIMITS=off` to switch it off globally.
Clearly, only do this if you know what you are doing and you are sure not to process malicious files.

## Encoder benchmark

A current benchmark of the AVIF encoders (as of 14 Oct 2022) can be found on the Wiki page
[AVIF encoding benchmark](https://github.com/strukturag/libheif/wiki/AVIF-Encoder-Benchmark).

## Language bindings

* .NET Platform (C#, F#, and other languages): [libheif-sharp](https://github.com/0xC0000054/libheif-sharp)
* C++: part of libheif
* Go: [libheif-go](https://github.com/strukturag/libheif-go), the wrapper distributed with libheif is deprecated
* JavaScript: by compilation with emscripten (see below)
* NodeJS module: [libheif-js](https://www.npmjs.com/package/libheif-js)
* Python: [pyheif](https://pypi.org/project/pyheif/), [pillow_heif](https://pypi.org/project/pillow-heif/)
* Rust: [libheif-sys](https://github.com/Cykooz/libheif-sys)
* Swift: [libheif-Xcode](https://swiftpackageregistry.com/SDWebImage/libheif-Xcode)
* JavaFX: [LibHeifFx](https://github.com/lanthale/LibHeifFX)

Languages that can directly interface with C libraries (e.g., Swift, C#) should work out of the box.

## Compiling to JavaScript / WASM

libheif can also be compiled to JavaScript using
[emscripten](http://kripken.github.io/emscripten-site/).
It can be built like this (in the libheif directory):
````
mkdir buildjs
cd buildjs
USE_WASM=0 ../build-emscripten.sh ..
````
Set `USE_WASM=1` to build with WASM output.
See the `build-emscripten.sh` script for further options.

## Online demo

Check out this [online demo](https://strukturag.github.io/libheif/).
This is `libheif` running in JavaScript in your browser.

## Example programs

Some example programs are provided in the `examples` directory.
The program `heif-dec` converts all images stored in an HEIF/AVIF file to JPEG or PNG.
`heif-enc` lets you convert JPEG files to HEIF/AVIF.
The program `heif-info` is a simple, minimal decoder that dumps the file structure to the console.

For example convert `example.heic` to JPEGs and one of the JPEGs back to HEIF:

```sh
cd examples/
./heif-dec example.heic example.jpeg
./heif-enc example-1.jpeg -o example.heif
```

In order to convert `example-1.jpeg` to AVIF use:

```sh
./heif-enc example-1.jpeg -A -o example.avif
```

There is also a GIMP plugin using libheif [here](https://github.com/strukturag/heif-gimp-plugin).

## HEIF/AVIF thumbnails for the Gnome desktop

The program `heif-thumbnailer` can be used as an HEIF/AVIF thumbnailer for the Gnome desktop.
The matching Gnome configuration files are in the `gnome` directory.
Place the files `heif.xml` and `avif.xml` into `/usr/share/mime/packages` and `heif.thumbnailer` into `/usr/share/thumbnailers`.
You may have to run `update-mime-database /usr/share/mime` to update the list of known MIME types.

## gdk-pixbuf loader

libheif also includes a gdk-pixbuf loader for HEIF/AVIF images. 'make install' will copy the plugin
into the system directories. However, you will still have to run `gdk-pixbuf-query-loaders --update-cache`
to update the gdk-pixbuf loader database.

## Software using libheif

* [GIMP](https://www.gimp.org/)
* [Krita](https://krita.org)
* [ImageMagick](https://imagemagick.org/)
* [GraphicsMagick](http://www.graphicsmagick.org/)
* [darktable](https://www.darktable.org)
* [digiKam 7.0.0](https://www.digikam.org/)
* [libvips](https://github.com/libvips/libvips)
* [kImageFormats](https://api.kde.org/frameworks/kimageformats/html/index.html)
* [libGD](https://libgd.github.io/)
* [Kodi HEIF image decoder plugin](https://kodi.wiki/view/Add-on:HEIF_image_decoder)
* [bimg](https://github.com/h2non/bimg)
* [GDAL](https://gdal.org/drivers/raster/heif.html)
* [OpenImageIO](https://sites.google.com/site/openimageio/)
* [XnView](https://www.xnview.com)

## Packaging status

[![libheif packaging status](https://repology.org/badge/vertical-allrepos/libheif.svg?exclude_unsupported=1&columns=3&exclude_sources=modules,site&header=libheif%20packaging%20status)](https://repology.org/project/libheif/versions)

## Sponsors

Since I work as an independent developer, I need your support to be able to allocate time for libheif.
You can [sponsor](https://github.com/sponsors/farindk) the development using the link in the right hand column.

A big thank you goes to these major sponsors for supporting the development of libheif:

* AOMedia
* OGC (Open Geospatial Consortium)
* Pinterest
* Shopify <img src="logos/sponsors/shopify.svg" alt="shopify-logo" height="20"/>
* StrukturAG

## License

The libheif is distributed under the terms of the GNU Lesser General Public License.
The sample applications are distributed under the terms of the MIT License.

See COPYING for more details.

Copyright (c) 2017-2020 Struktur AG</br>
Copyright (c) 2017-2026 Dirk Farin</br>
Contact: Dirk Farin <dirk.farin@gmail.com>
