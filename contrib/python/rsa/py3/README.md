# Pure Python RSA implementation

[![PyPI](https://img.shields.io/pypi/v/rsa.svg)](https://pypi.org/project/rsa/)
[![Build Status](https://travis-ci.org/sybrenstuvel/python-rsa.svg?branch=master)](https://travis-ci.org/sybrenstuvel/python-rsa)
[![Coverage Status](https://coveralls.io/repos/github/sybrenstuvel/python-rsa/badge.svg?branch=master)](https://coveralls.io/github/sybrenstuvel/python-rsa?branch=master)
[![Code Climate](https://api.codeclimate.com/v1/badges/a99a88d28ad37a79dbf6/maintainability)](https://codeclimate.com/github/codeclimate/codeclimate/maintainability)

[Python-RSA](https://stuvel.eu/rsa) is a pure-Python RSA implementation. It supports
encryption and decryption, signing and verifying signatures, and key
generation according to PKCS#1 version 1.5. It can be used as a Python
library as well as on the commandline. The code was mostly written by
Sybren A.  Stüvel.

Documentation can be found at the [Python-RSA homepage](https://stuvel.eu/rsa). For all changes, check [the changelog](https://github.com/sybrenstuvel/python-rsa/blob/master/CHANGELOG.md).

Download and install using:

    pip install rsa

or download it from the [Python Package Index](https://pypi.org/project/rsa/).

The source code is maintained at [GitHub](https://github.com/sybrenstuvel/python-rsa/) and is
licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## Security

Because of how Python internally stores numbers, it is very hard (if not impossible) to make a pure-Python program secure against timing attacks. This library is no exception, so use it with care. See https://securitypitfalls.wordpress.com/2018/08/03/constant-time-compare-in-python/ for more info.

## Setup of Development Environment

```
python3 -m venv .venv
. ./.venv/bin/activate
pip install poetry
poetry install
```

## Publishing a New Release

Since this project is considered critical on the Python Package Index,
two-factor authentication is required. For uploading packages to PyPi, an API
key is required; username+password will not work.

First, generate an API token at https://pypi.org/manage/account/token/. Then,
use this token when publishing instead of your username and password.

As username, use `__token__`.
As password, use the token itself, including the `pypi-` prefix.

See https://pypi.org/help/#apitoken for help using API tokens to publish. This
is what I have in `~/.pypirc`:

```
[distutils]
index-servers =
    rsa

# Use `twine upload -r rsa` to upload with this token.
[rsa]
  repository = https://upload.pypi.org/legacy/
  username = __token__
  password = pypi-token
```

```
. ./.venv/bin/activate
pip install twine

poetry build
twine check dist/rsa-4.9.tar.gz dist/rsa-4.9-*.whl
twine upload -r rsa dist/rsa-4.9.tar.gz dist/rsa-4.9-*.whl
```

The `pip install twine` is necessary as Python-RSA requires Python >= 3.6, and
Twine requires at least version 3.7. This means Poetry refuses to add it as
dependency.
