# Flask Babel - 1.0.0

[![Build Status](https://travis-ci.org/python-babel/flask-babel.svg?branch=master)](https://travis-ci.org/python-babel/flask-babel)
[![PyPI](https://img.shields.io/pypi/v/flask-babel.svg?maxAge=2592000)](https://pypi.python.org/pypi/Flask-Babel)
![license](https://img.shields.io/github/license/python-babel/flask-babel.svg?maxAge=2592000)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/flask-babel.svg)

Implements i18n and l10n support for Flask.  This is based on the Python
[babel][] module as well as [pytz][] both of which are installed automatically
for you if you install this library.

# Documention

The latest documentation is available [here][docs].

# Changelog

## v1.0.0 - 06/02/2020

Starting with version 1, flask-babel has changed to
[Semantic Versioning][semver].

### Changed

- pytz is an explicit dependency. (#14)
- pytz.gae, used for Google App Engine, is no longer necessary and has been
  removed. (#153)
- Fixed a deprecated werkzeug import (#158).
- Fix issues switching locales in threaded contexts (#125).

[babel]: https://github.com/python-babel/babel
[pytz]: https://pypi.python.org/pypi/pytz/
[docs]: https://pythonhosted.org/Flask-Babel/
[semver]: https://semver.org/
