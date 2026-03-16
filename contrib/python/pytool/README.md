# Pytool Package

`pytool` is a collection of often used helper methods and classes that make life
easier or fill in gaps in the Python Standard Library.

Pytool is compatible with and tested against Python 3.8 and higher. End of life
Python versions are not supported.

> [!NOTE]
> Pytool 4.x is a major release that is not backwards compatible with earlier
> versions. It now requires Python 3.8 or higher, and adds Python 3 type
> annotations among other improvements. New features will only be added to the
> 4.x release line.

Pytool's source is hosted on Github:
<https://github.com/shakefu/pytool>

Any comments, issues or requests should be submitted via Github:
<https://github.com/shakefu/pytool/issues>

![CI](https://github.com/shakefu/pytool/actions/workflows/ci.yaml/badge.svg)
[![Coveralls](https://coveralls.io/repos/shakefu/pytool/badge.svg?branch=master&service=github)](https://coveralls.io/github/shakefu/pytool?branch=master)

## Features

Pytool provides a rich set of utilities across several key areas:

### Command Line Tools (`pytool.cmd`)

- Create robust command-line applications with minimal boilerplate, featuring
  built-in subcommand support and automatic help generation
- Seamless integration with argparse for argument parsing, with automatic
  support for configargparse when available
- Built-in signal handling for graceful process management and reloading
- Console script entry point support for easy distribution and installation
- Automatic environment variable integration through configargparse

### Language Utilities (`pytool.lang`)

- `Namespace` class for creating flexible, nested data structures with
  dot-notation access and automatic nesting
- `singleton` and `hashed_singleton` decorators for efficient instance
  management and caching
- `classproperty` decorator for creating class-level properties, filling a gap
  in Python's standard library
- `get_name` utility for detailed frame inspection and debugging
- `UNSET` sentinel value for clear handling of optional parameters

### Time Management (`pytool.time`)

- Comprehensive UTC-aware datetime handling with proper timezone support
- `Timer` class for precise performance measurements and benchmarking
- `ago()` helper for intuitive relative time calculations with flexible
  time unit support
- Week-based time calculations for scheduling and reporting
- Robust timezone conversion utilities with daylight savings handling

### JSON Handling (`pytool.json`)

- Enhanced JSON serialization with built-in support for:
  - Datetime objects with proper timezone handling
  - BSON ObjectId types for MongoDB integration
  - Custom object serialization via the `for_json()` protocol
- Intelligent fallback between simplejson and stdlib json for optimal
  performance
- Consistent datetime formatting across different JSON implementations

### Text Processing (`pytool.text`)

- Terminal-aware text wrapping that adapts to your console width
- Smart column width detection with fallback options
- Intelligent indentation preservation for code and documentation
- Paragraph-aware text formatting that maintains document structure
- Automatic handling of mixed indentation styles

### Proxy Objects (`pytool.proxy`)

- `ListProxy` for transparent list manipulation with custom behavior
  modification
- `DictProxy` for dictionary behavior modification without copying data
- Support for custom JSON serialization of proxy objects
- Efficient memory usage through reference-based operations
- Seamless integration with existing list and dict operations

## Documentation

Read the [complete documentation](https://pytool.readthedocs.org/en/latest/) on
[Read the Docs](https://readthedocs.org)!

## Installation

Pytool is available on PyPI for your installation ease.

```bash
pip install pytool
```

## Usage

Pytool is a collection of common and useful functions designed to help your code
be easier to read, write and maintain. The API is well documented with examples
of how they might be used.

See the [complete documentation](https://pytool.readthedocs.org/en/latest/) on
Read The Docs for more information.

## Contributors

- [shakefu](https://github.com/shakefu) (creator, maintainer)
- [dshen109](https://github.com/dshen109) (contributor)
- [abendig](https://github.com/abendig)
