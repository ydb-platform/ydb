# Scour

[![PyPI](https://img.shields.io/pypi/v/scour.svg)](https://pypi.python.org/pypi/scour "Package listing on PyPI")
 
[![Build status](https://img.shields.io/travis/scour-project/scour.svg)](https://travis-ci.org/scour-project/scour "Build status (via TravisCI)")
[![Codecov](https://img.shields.io/codecov/c/github/scour-project/scour.svg)](https://codecov.io/gh/scour-project/scour "Code coverage (via Codecov)")

---

Scour is an SVG optimizer/cleaner that reduces the size of scalable vector graphics by optimizing structure and removing unnecessary data written in Python.

It can be used to create streamlined vector graphics suitable for web deployment, publishing/sharing or further processing.

The goal of Scour is to output a file that renderes identically at a fraction of the size by removing a lot of redundant information created by most SVG editors. Optimization options are typically lossless but can be tweaked for more agressive cleaning.

Scour is open-source and licensed under [Apache License 2.0](https://github.com/codedread/scour/blob/master/LICENSE).

Scour was originally developed by Jeff "codedread" Schiller and Louis Simard in in 2010.
The project moved to GitLab in 2013 an is now maintained by Tobias "oberstet" Oberstein and Patrick "Ede_123" Storz.

## Installation

Scour requires [Python](https://www.python.org) 2.7 or 3.4+. Further, for installation, [pip](https://pip.pypa.io) should be used.

To install the [latest release](https://pypi.python.org/pypi/scour) of Scour from PyPI:

```console
pip install scour
```

To install the [latest trunk](https://github.com/codedread/scour) version (which might be broken!) from GitHub:

```console
pip install https://github.com/codedread/scour/archive/master.zip
```

## Usage

Standard:

```console
scour -i input.svg -o output.svg
```

Better (for older versions of Internet Explorer):

```console
scour -i input.svg -o output.svg --enable-viewboxing
```

Maximum scrubbing:

```console
scour -i input.svg -o output.svg --enable-viewboxing --enable-id-stripping \
  --enable-comment-stripping --shorten-ids --indent=none
```

Maximum scrubbing and a compressed SVGZ file:

```console
scour -i input.svg -o output.svgz --enable-viewboxing --enable-id-stripping \
  --enable-comment-stripping --shorten-ids --indent=none
```
