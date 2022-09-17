![logo](https://raw.githubusercontent.com/boostorg/python/develop/doc/images/bpl.png)

# Synopsis

Welcome to Boost.Python, a C++ library which enables seamless interoperability between C++ and the Python programming language. The library includes support for:

* References and Pointers
* Globally Registered Type Coercions
* Automatic Cross-Module Type Conversions
* Efficient Function Overloading
* C++ to Python Exception Translation
* Default Arguments
* Keyword Arguments
* Manipulating Python objects in C++
* Exporting C++ Iterators as Python Iterators
* Documentation Strings

See the [Boost.Python](http://boostorg.github.io/python) documentation for details.

**Hint :** Check out the [development version](http://boostorg.github.io/python/develop) of the documentation to see work in progress.

# Building ![Build Status](https://travis-ci.org/boostorg/python.svg?branch=develop)

While Boost.Python is part of the Boost C++ Libraries super-project, and thus can be compiled as part of Boost, it can also be compiled and installed stand-alone, i.e. against a pre-installed Boost package.

## Prerequisites

* [Python](http://www.python.org)
* [Boost](http://www.boost.org)
* [Faber](https://stefanseefeld.github.io/faber)

## Build

Run

```
faber
```
to build the library.

## Test

Run

```
faber test.report
```
to run the tests.

## Build docs

Run

```
faber doc.html
```
to build the documentation.
