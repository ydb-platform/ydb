
libde265 - open h.265 codec implementation
==========================================

![libde265](libde265.png)

libde265 is an open source implementation of the h.265 video codec.
It is written from scratch and has a plain C API to enable
a simple integration into other software.

libde265 supports WPP and tile-based multithreading and includes SSE optimizations.
The decoder includes all features of the Main profile and correctly decodes almost all
conformance streams (see [[wiki page](https://github.com/strukturag/libde265/wiki/Decoder-conformance)]).

A list of supported features are available in the [wiki](https://github.com/strukturag/libde265/wiki/Supported-decoding-features).

For latest news check our website at http://www.libde265.org

The library comes with two example programs:

- dec265, a simple player for raw h.265 bitstreams.
          It serves nicely as an example program how to use libde265.

- sherlock265, a Qt-based video player with the additional capability
          to overlay some graphical representations of the h.265
          bitstream (like CU-trees, intra-prediction modes).

Example bitstreams can be found, e.g., at this site:
  ftp://ftp.kw.bbc.co.uk/hevc/hm-10.1-anchors/bitstreams/ra_main/

Approximate performance for WPP, non-tiles streams (measured using the `timehevc`
tool from [the GStreamer plugin](https://github.com/strukturag/gstreamer-libde265)).
The tool plays a Matroska movie to the GStreamer fakesink and measures
the average framerate.

| Resolution        | avg. fps | CPU usage |
| ----------------- | -------- | --------- |
| [720p][1]         |  284 fps |      39 % |
| [1080p][2]        |  150 fps |      45 % |
| [4K][3]           |   36 fps |      56 % |

Environment:
- Intel(R) Core(TM) i7-2700K CPU @ 3.50GHz (4 physical CPU cores)
- Ubuntu 12.04, 64bit
- GStreamer 0.10.36

[1]: http://trailers.divx.com/hevc/TearsOfSteel_720p_24fps_27qp_831kbps_720p_GPSNR_41.65_HM11_2aud_7subs.mkv
[2]: http://trailers.divx.com/hevc/TearsOfSteel_1080p_24fps_27qp_1474kbps_GPSNR_42.29_HM11_2aud_7subs.mkv
[3]: http://trailers.divx.com/hevc/TearsOfSteel_4K_24fps_9500kbps_2aud_9subs.mkv


Building
========

[![Build Status](https://github.com/strukturag/libde265/workflows/build/badge.svg)](https://github.com/strukturag/libde265/actions) [![Build Status](https://ci.appveyor.com/api/projects/status/github/strukturag/libde265?svg=true)](https://ci.appveyor.com/project/strukturag/libde265)

If you got libde265 from the git repository, you will first need to run
the included `autogen.sh` script to generate the `configure` script.

libde265 has no dependencies on other libraries, but both optional example programs
have dependencies on:

- SDL2 (optional for dec265's YUV overlay output),

- Qt (required for sherlock265),

- libswscale (required for sherlock265 if libvideogfx is not available).

- libvideogfx (required for sherlock265 if libswscale is not available,
  optional for dec265).

Libvideogfx can be obtained from
  http://www.dirk-farin.net/software/libvideogfx/index.html
or
  http://github.com/farindk/libvideogfx


You can disable building of the example programs by running `./configure` with
<pre>
  --disable-dec265        Do not build the dec265 decoder program.
  --disable-sherlock265   Do not build the sherlock265 visual inspection program.
</pre>

Additional logging information can be turned on and off using these `./configure` flags:
<pre>
  --enable-log-error      turn on logging at error level (default=yes)
  --enable-log-info       turn on logging at info level (default=no)
  --enable-log-trace      turn on logging at trace level (default=no)
</pre>


Build using cmake
=================

cmake scripts to build libde265 and the sample scripts `dec265` and `enc265` are
included and can be compiled using these commands:

```
mkdir build
cd build
cmake ..
make
```

See the [cmake documentation](http://www.cmake.org) for further information on
using cmake on other platforms.


Building using vcpkg
====================

You can build and install libde265 using the [vcpkg](https://github.com/Microsoft/vcpkg/) dependency manager:

```
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh
./vcpkg integrate install
./vcpkg install libde265
```

The libde265 port in vcpkg is kept up to date by Microsoft team members and community contributors. If the version is out of date, please [create an issue or pull request](https://github.com/Microsoft/vcpkg) on the vcpkg repository.


Prebuilt binaries
=================

Binary packages can be obtained from this [launchpad site](https://launchpad.net/~strukturag/+archive/libde265).


Software using libde265
=======================

Libde265 has been integrated into these applications:

- gstreamer plugin, [source](https://github.com/strukturag/gstreamer-libde265), [binary packages](https://launchpad.net/~strukturag/+archive/libde265).

- VLC plugin [source](https://github.com/strukturag/vlc-libde265), [binary packages](https://launchpad.net/~strukturag/+archive/libde265).

- Windows DirectShow filters, https://github.com/strukturag/LAVFilters/releases

- ffmpeg fork, https://github.com/farindk/ffmpeg

- ffmpeg decoder [source](https://github.com/strukturag/libde265-ffmpeg)

- libde265.js JavaScript decoder [source](https://github.com/strukturag/libde265.js), [demo](https://strukturag.github.io/libde265.js/).


## Packaging status

[![libde265 packaging status](https://repology.org/badge/vertical-allrepos/libde265.svg?exclude_unsupported=1&columns=3&exclude_sources=modules,site&header=libde265%20packaging%20status)](https://repology.org/project/libheif/versions)


License
=======

The library `libde265` is distributed under the terms of the GNU Lesser
General Public License. The sample applications are distributed under
the terms of the MIT license.

See `COPYING` for more details.

The short video clip in the 'testdata' directory is from the movie 'Girl Shy', which is in the public domain.

Copyright (c) 2013-2014 Struktur AG<br>
Copyright (c) 2013-2024 Dirk Farin<br>
Contact: Dirk Farin <dirk.farin@gmail.com>
