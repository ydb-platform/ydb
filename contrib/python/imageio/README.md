# IMAGEIO

[![CI](https://github.com/imageio/imageio/workflows/CI/badge.svg)](https://github.com/imageio/imageio/actions/workflows/ci.yml)
[![CD](https://github.com/imageio/imageio/workflows/CD/badge.svg)](https://github.com/imageio/imageio/actions/workflows/cd.yml)
[![codecov](https://codecov.io/gh/imageio/imageio/branch/master/graph/badge.svg?token=81Zhu9MDec)](https://codecov.io/gh/imageio/imageio)
[![Docs](https://readthedocs.org/projects/imageio/badge/?version=latest)](https://imageio.readthedocs.io)

[![Supported Python Versions](https://img.shields.io/pypi/pyversions/imageio.svg)](https://pypi.python.org/pypi/imageio/)
[![PyPI Version](https://img.shields.io/pypi/v/imageio.svg)](https://pypi.python.org/pypi/imageio/)
![PyPI Downloads](https://img.shields.io/pypi/dm/imageio?color=blue)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1488561.svg)](https://doi.org/10.5281/zenodo.1488561)

Website: <https://imageio.readthedocs.io/>

Imageio is a mature Python library that makes it easy to read and write image
and video data. This includes animated images, video, volumetric data, and
scientific formats. It is cross-platform, runs on Python 3.9+, and is easy to
install.

Professional support is available via
[Tidelift](https://tidelift.com/funding/github/pypi/imageio).

## Example

Here's a minimal example of how to use imageio. See the docs for [more
examples](https://imageio.readthedocs.io/en/stable/examples.html).

```python
import imageio.v3 as iio
im = iio.imread('imageio:chelsea.png')  # read a standard image
im.shape  # im is a NumPy array of shape (300, 451, 3)
iio.imwrite('chelsea.jpg', im)  # convert to jpg
```

## API in a nutshell

You just have to remember a handful of functions:

```python
imread()  # for reading
imwrite() # for writing
imiter()  # for iterating image series (animations/videos/OME-TIFF/...)
improps() # for standardized metadata
immeta()  # for format-specific metadata
imopen()  # for advanced usage
```

See the [API docs](https://imageio.readthedocs.io/en/stable/reference/index.html) for more information.

## Features

- Simple interface via a concise set of functions
- Easy to
  [install](https://imageio.readthedocs.io/en/stable/getting_started/installation.html)
  using Conda or pip
- Few core dependencies (only NumPy and Pillow)
- Pure Python, runs on Python 3.9+, and PyPy
- Cross platform, runs on Windows, Linux, macOS
- More than 295 supported
  [formats](https://imageio.readthedocs.io/en/stable/formats/index.html)
- Read/Write support for various
  [resources](https://imageio.readthedocs.io/en/stable/getting_started/requests.html)
  (files, URLs, bytes, FileLike objects, ...)
- High code quality and large test suite including functional, regression, and
  integration tests

## Dependencies

Minimal requirements:

- Python 3.9+
- NumPy
- Pillow >= 8.3.2

Optional Python packages:

- imageio-ffmpeg (for working with video files)
- pyav (for working with video files)
- tifffile (for working with TIFF files)
- itk or SimpleITK (for ITK plugin)
- astropy (for FITS plugin)
- [imageio-flif](https://codeberg.org/monilophyta/imageio-flif) (for working
  with [FLIF](https://github.com/FLIF-hub/FLIF) image files)

## Security contact information

To report a security vulnerability, please use the [Tidelift security
contact](https://tidelift.com/security). Tidelift will coordinate the fix and
disclosure.

## ImageIO for enterprise

Available as part of the Tidelift Subscription.

The maintainers of imageio and thousands of other packages are working with
Tidelift to deliver commercial support and maintenance for the open source
dependencies you use to build your applications. Save time, reduce risk, and
improve code health, while paying the maintainers of the exact dependencies you
use. ([Learn
more](https://tidelift.com/subscription/pkg/pypi-imageio?utm_source=pypi-imageio&utm_medium=referral&utm_campaign=readme))

## Details

The core of ImageIO is a set of user-facing APIs combined with a plugin manager.
API calls choose sensible defaults and then call the plugin manager, which
deduces the correct plugin/backend to use for the given resource and file
format. The plugin manager adds sensible backend-specific defaults and then
calls one of ImageIOs many backends to perform the actual loading. This allows
ImageIO to take care of most of the gory details of loading images for you,
while still allowing you to customize the behavior when and where you need to.
You can find a more detailed explanation of this process in [our
documentation](https://imageio.readthedocs.io/en/stable/user_guide/overview.html).

## Contributing

We welcome contributions of any kind. Here are some suggestions on how you are
able to contribute

- add missing formats to the format list
- suggest/implement support for new backends
- report/fix any bugs you encounter while using ImageIO

To assist you in getting started with contributing code, take a look at the
[development
section](https://imageio.readthedocs.io/en/stable/development/index.html) of the
docs. You will find instructions on setting up the dev environment as well as
examples on how to contribute code.
