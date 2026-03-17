Flask-Caching
=============

[![Build Status](https://travis-ci.org/sh4nks/flask-caching.svg?branch=master)](https://travis-ci.org/sh4nks/flask-caching)
[![Coverage Status](https://coveralls.io/repos/sh4nks/flask-caching/badge.png)](https://coveralls.io/r/sh4nks/flask-caching)
[![PyPI Version](https://img.shields.io/pypi/v/Flask-Caching.svg)](https://pypi.python.org/pypi/Flask-Caching)
[![Documentation Status](https://readthedocs.org/projects/flask-caching/badge/?version=latest)](https://flask-caching.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/license-BSD-yellow.svg)](https://github.com/sh4nks/flask-caching)

Adds easy cache support to Flask.

This is a fork of the [Flask-Cache](https://github.com/thadeusb/flask-cache)
extension.

Flask-Caching also includes the ``cache`` module from werkzeug licensed under a
BSD-3 Clause License.


Setup
-----

Flask-Caching is available on PyPI and can be installed with:

    pip install flask-caching

The Cache Extension can either be initialized directly:

```python
from flask import Flask
from flask_caching import Cache

app = Flask(__name__)
# For more configuration options, check out the documentation
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
```

Or through the factory method:

```python
cache = Cache(config={'CACHE_TYPE': 'simple'})

app = Flask(__name__)
cache.init_app(app)
```

Compatibility with Flask-Cache
-----
There are no known incompatibilities or breaking changes between the latest [Flask-Cache](https://github.com/thadeusb/flask-cache)
release (version 0.13, April 2014) and the current version of Flask-Caching. Due to the change to the Flask-Caching name
and the [extension import transition](http://flask.pocoo.org/docs/0.11/extensiondev/#extension-import-transition),
Python import lines like:

 ```from flask.ext.cache import Cache```

 will need to be changed to:

 ```from flask_caching import Cache```


Links
=====

* [Documentation](https://flask-caching.readthedocs.io)
* [Source Code](https://github.com/sh4nks/flask-caching)
* [Issues](https://github.com/sh4nks/flask-caching/issues)
* [Original Flask-Cache Extension](https://github.com/thadeusb/flask-cache)
