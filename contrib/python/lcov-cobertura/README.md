# lcov to cobertura XML converter

[![CI](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/ci.yml/badge.svg)](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/ci.yml)
[![Docs](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/sphinx.yml/badge.svg)](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/sphinx.yml)
[![Security check - Bandit](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/bandit.yml/badge.svg)](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/bandit.yml)
[![Release](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/release.yml/badge.svg)](https://github.com/eriwen/lcov-to-cobertura-xml/actions/workflows/release.yml)

This project does as the name implies: it converts code coverage report files in [lcov](http://ltp.sourceforge.net/coverage/lcov.php) format to [Cobertura](http://cobertura.sourceforge.net/)'s XML report format so that CI servers like [Jenkins](http://jenkins-ci.org) can aggregate results and determine build stability etc.

Coverage metrics supported:

 - Package/folder overall line and branch coverage
 - Class/file overall line and branch coverage
 - Functions hit
 - Line and Branch hits
 
## Quick usage

[Grab it raw](https://raw.github.com/eriwen/lcov-to-cobertura-xml/master/lcov_cobertura/lcov_cobertura.py) and run it with python:
```bash
python lcov_cobertura.py lcov-file.dat
```

 - `-b/--base-dir` - (Optional) Directory where source files are located. Defaults to the current directory
 - `-e/--excludes` - (Optional) Comma-separated list of regexes of packages to exclude
 - `-o/--output` - (Optional) Path to store cobertura xml file. _Defaults to ./coverage.xml_
 - `-d/--demangle` - (Optional) Demangle C++ function names. _Requires c++filt_

```bash
python lcov_cobertura.py lcov-file.dat --base-dir src/dir --excludes test.lib --output build/coverage.xml --demangle
```
 
## With [pip](http://pypi.python.org/pypi/pip):
```bash
pip install lcov_cobertura
```

### Command-line usage
```bash
lcov_cobertura lcov-file.dat
```

 - `-b/--base-dir` - (Optional) Directory where source files are located. Defaults to the current directory
 - `-e/--excludes` - (Optional) Comma-separated list of regexes of packages to exclude
 - `-o/--output` - (Optional) Path to store cobertura xml file. _Defaults to ./coverage.xml_
 - `-d/--demangle` - (Optional) Demangle C++ function names. _Requires c++filt_

```bash
lcov_cobertura lcov-file.dat --base-dir src/dir --excludes test.lib --output build/coverage.xml --demangle
```

### Usage as a Python module

Use it anywhere in your python:
```python
from lcov_cobertura import LcovCobertura

LCOV_INPUT = 'SF:foo/file.ext\nDA:1,1\nDA:2,0\nend_of_record\n'
converter = LcovCobertura(LCOV_INPUT)
cobertura_xml = converter.convert()
print(cobertura_xml)
```

## Environment Support

Python 3.8+ is supported. The last release with Python 2.x support is [version 1.6](https://pypi.org/project/lcov_cobertura/1.6/). 

## Contributions
This project is made possible due to the efforts of these fine people:

 - [Eric Wendelin](https://eriwen.com)
 - [Björge Dijkstra](https://github.com/bjd)
 - [Jon Schewe](http://mtu.net/~jpschewe)
 - [Yury V. Zaytsev](http://yury.zaytsev.net)
 - [Steve Arnold](https://github.com/sarnold)

## License
This project is provided under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
