# environs: simplified environment variable parsing

[![Latest version](https://badgen.net/pypi/v/environs)](https://pypi.org/project/environs/)
[![Build Status](https://github.com/sloria/environs/actions/workflows/build-release.yml/badge.svg)](https://github.com/sloria/environs/actions/workflows/build-release.yml)

**environs** is a Python library for parsing environment variables.
It allows you to store configuration separate from your code, as per
[The Twelve-Factor App](https://12factor.net/config) methodology.

## Contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Features](#features)
- [Install](#install)
- [Basic usage](#basic-usage)
- [Supported types](#supported-types)
- [Reading `.env` files](#reading-env-files)
  - [Reading a specific file](#reading-a-specific-file)
- [Handling prefixes](#handling-prefixes)
- [Variable expansion](#variable-expansion)
- [Validation](#validation)
- [Deferred validation](#deferred-validation)
- [URL schemes](#url-schemes)
- [Serialization](#serialization)
- [Reading Docker-style secret files](#reading-docker-style-secret-files)
- [Defining custom parser behavior](#defining-custom-parser-behavior)
- [Usage with Flask](#usage-with-flask)
- [Usage with Django](#usage-with-django)
- [Why\...?](#why%5C)
  - [Why envvars?](#why-envvars)
  - [Why not `os.environ`?](#why-not-osenviron)
  - [Why another library?](#why-another-library)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Features

- Type-casting
- Read `.env` files into `os.environ` (useful for local development)
- Validation
- Define custom parser behavior
- Framework-agnostic, but integrates well with [Flask](#usage-with-flask) and [Django](#usage-with-django)

## Install

    pip install environs

## Basic usage

With some environment variables set...

```bash
export GITHUB_USER=sloria
export MAX_CONNECTIONS=100
export SHIP_DATE='1984-06-25'
export TTL=42
export ENABLE_LOGIN=true
export GITHUB_REPOS=webargs,konch,ped
export GITHUB_REPO_PRIORITY="webargs=2,konch=3"
export LOCATIONS="x:234 y:123"
export COORDINATES=23.3,50.0
export LOG_LEVEL=DEBUG
```

Parse them with environs...

```python
from environs import env

env.read_env()  # read .env file, if it exists
# required variables
gh_user = env("GITHUB_USER")  # => 'sloria'
secret = env("SECRET")  # => raises error if not set

# casting
max_connections = env.int("MAX_CONNECTIONS")  # => 100
ship_date = env.date("SHIP_DATE")  # => datetime.date(1984, 6, 25)
ttl = env.timedelta("TTL")  # => datetime.timedelta(seconds=42)
log_level = env.log_level("LOG_LEVEL")  # => logging.DEBUG

# providing a default value
enable_login = env.bool("ENABLE_LOGIN", False)  # => True
enable_feature_x = env.bool("ENABLE_FEATURE_X", False)  # => False

# parsing lists
gh_repos = env.list("GITHUB_REPOS")  # => ['webargs', 'konch', 'ped']
coords = env.list("COORDINATES", subcast=float)  # => [23.3, 50.0]

# parsing dicts
gh_repos_priorities = env.dict(
    "GITHUB_REPO_PRIORITY", subcast_values=int
)  # => {'webargs': 2, 'konch': 3}

# parsing dicts with different delimiters
locations = env.dict(
    "LOCATIONS", subcast_values=int, delimiter=" ", key_value_delimiter=":"
)  # => {'x': 234, 'y': 123}
```

## Supported types

The following are all type-casting methods of `Env`:

- `env.str`
- `env.bool`
- `env.int`
- `env.float`
- `env.decimal`
- `env.list` (accepts optional `subcast` and `delimiter` keyword arguments)
- `env.dict` (accepts optional `subcast_keys`, `subcast_values`, `delimiter`, and `key_value_delimiter` keyword arguments)
- `env.json`
- `env.datetime`
- `env.date`
- `env.time`
- `env.timedelta` (assumes value is an integer in seconds, or an ordered duration string like `7h7s` or `7w 7d 7h 7m 7s 7ms 7us`)
- `env.url`
  - This returns a `urllib.parse.ParseResult` and therefore expects a `ParseResult` for its default.

```python
from urllib.parse import urlparse

from environs import env

MY_API_URL = env.url(
    "MY_API_URL",
    default=urlparse("http://api.example.com"),
)
```

If you want the return value to be a string, use `env.str` with `validate.URL` instead.

```python
from environs import env, validate

MY_API_URL = env.str(
    "MY_API_URL",
    default="http://api.example.com",
    validate=validate.URL(),
)
```

- `env.uuid`
- `env.log_level`
- `env.path` (casts to a [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html))
- `env.enum` (casts to any given enum type specified in `enum` keyword argument)
  - Pass `by_value=True` to parse and validate by the Enum's values.

## Reading `.env` files

```bash
# .env
DEBUG=true
PORT=4567
```

Call `Env.read_env` before parsing variables.

```python
from environs import env

# Read .env into os.environ
env.read_env()

env.bool("DEBUG")  # => True
env.int("PORT")  # => 4567
```

### Reading a specific file

By default, `Env.read_env` will look for a `.env` file in current
directory and (if no .env exists in the CWD) recurse
upwards until a `.env` file is found.

You can also read a specific file:

```python
from environs import env

with open(".env.test", "w") as fobj:
    fobj.write("A=foo\n")
    fobj.write("B=123\n")

env.read_env(".env.test", recurse=False)

assert env("A") == "foo"
assert env.int("B") == 123
```

## Handling prefixes

Pass `prefix` to the constructor if all your environment variables have the same prefix.

```python
from environs import Env

# export MYAPP_HOST=lolcathost
# export MYAPP_PORT=3000


env = Env(prefix="MYAPP_")

host = env("HOST", "localhost")  # => 'lolcathost'
port = env.int("PORT", 5000)  # => 3000
```

Alternatively, you can use the `prefixed` context manager.

```python
from environs import env

# export MYAPP_HOST=lolcathost
# export MYAPP_PORT=3000

with env.prefixed("MYAPP_"):
    host = env("HOST", "localhost")  # => 'lolcathost'
    port = env.int("PORT", 5000)  # => 3000

# nested prefixes are also supported:

# export MYAPP_DB_HOST=lolcathost
# export MYAPP_DB_PORT=10101

with env.prefixed("MYAPP_"):
    with env.prefixed("DB_"):
        db_host = env("HOST", "lolcathost")
        db_port = env.int("PORT", 10101)
```

## Variable expansion

```python
# export CONNECTION_URL=https://${USER:-sloria}:${PASSWORD}@${HOST:-localhost}/
# export PASSWORD=secret
# export YEAR=${CURRENT_YEAR:-2020}

from environs import Env

env = Env(expand_vars=True)

connection_url = env("CONNECTION_URL")  # =>'https://sloria:secret@localhost'
year = env.int("YEAR")  # =>2020
```

## Validation

```python
# export TTL=-2
# export NODE_ENV='invalid'
# export EMAIL='^_^'

from environs import env, validate, ValidationError


# built-in validators (provided by marshmallow)
env.str(
    "NODE_ENV",
    validate=validate.OneOf(
        ["production", "development"], error="NODE_ENV must be one of: {choices}"
    ),
)
# => Environment variable "NODE_ENV" invalid: ['NODE_ENV must be one of: production, development']

# multiple validators
env.str("EMAIL", validate=[validate.Length(min=4), validate.Email()])
# => Environment variable "EMAIL" invalid: ['Shorter than minimum length 4.', 'Not a valid email address.']


# custom validator
def validator(n):
    if n <= 0:
        raise ValidationError("Invalid value.")


env.int("TTL", validate=validator)
# => Environment variable "TTL" invalid: ['Invalid value.']
```

`environs.validate` is equivalent to [`marshmallow.validate`](https://marshmallow.readthedocs.io/en/stable/marshmallow.validate.html), so you can use any of the validators provided by that module.

## Deferred validation

By default, a validation error is raised immediately upon calling a parser method for an invalid environment variable.
To defer validation and raise an exception with the combined error messages for all invalid variables, pass `eager=False` to `Env`.
Call `env.seal()` after all variables have been parsed.

```python
# export TTL=-2
# export NODE_ENV='invalid'
# export EMAIL='^_^'

from environs import Env
from marshmallow.validate import OneOf, Email, Length, Range

env = Env(eager=False)

TTL = env.int("TTL", validate=Range(min=0, max=100))
NODE_ENV = env.str(
    "NODE_ENV",
    validate=OneOf(
        ["production", "development"], error="NODE_ENV must be one of: {choices}"
    ),
)
EMAIL = env.str("EMAIL", validate=[Length(min=4), Email()])

env.seal()
# environs.EnvValidationError: Environment variables invalid: {'TTL': ['Must be greater than or equal to 0 and less than or equal to 100.'], 'NODE_ENV': ['NODE_ENV must be one of: production, development'], 'EMAIL': ['Shorter than minimum length 4.', 'Not a valid email address.']}
```

`env.seal()` validates all parsed variables and prevents further parsing (calling a parser method will raise an error).

## URL schemes

`env.url()` supports non-standard URL schemes via the `schemes` argument.

```python
from urllib.parse import urlparse

REDIS_URL = env.url(
    "REDIS_URL", urlparse("redis://redis:6379"), schemes=["redis"], require_tld=False
)
```

## Serialization

```python
# serialize to a dictionary of simple types (numbers and strings)
env.dump()
# {'COORDINATES': [23.3, 50.0],
# 'ENABLE_FEATURE_X': False,
# 'ENABLE_LOGIN': True,
# 'GITHUB_REPOS': ['webargs', 'konch', 'ped'],
# 'GITHUB_USER': 'sloria',
# 'MAX_CONNECTIONS': 100,
# 'MYAPP_HOST': 'lolcathost',
# 'MYAPP_PORT': 3000,
# 'SHIP_DATE': '1984-06-25',
# 'TTL': 42}
```

## Reading Docker-style secret files

Some values should not be stored in the environment. For this use case a commonly
used technique is to store the value (e.g., a password) in a file and set the path
to that file in an environment variable. Use `FileAwareEnv` in place of `Env` to automatically check for environment variables with the `_FILE` suffix. If the
file is found, its contents will be read and returned.

```python
from environs import FileAwareEnv

# printf 'my secret password' >/run/secrets/password
# export PASSWORD_FILE=/run/secrets/password

env = FileAwareEnv()
password = env.str("PASSWORD")  # => 'my secret  password'
```

It's also possible to set a different suffix for the variable names:

```python
from environs import FileAwareEnv

# printf 'my secret password' >/run/secrets/password
# export PASSWORD_SECRET=/run/secrets/password

env = FileAwareEnv(file_suffix="_SECRET")
password = env.str("PASSWORD")  #  => 'my secret password'
```

To strip leading and trailing whitespaces from the value:

```python
from environs import FileAwareEnv

# printf '  value with leading and trailing whitespaces \n' >/run/secrets/password
# export PASSWORD_FILE=/run/secrets/password

env: FileAwareEnv = FileAwareEnv(strip_whitespace=True)
password: str = env.str("PASSWORD")  # => `value with leading and trailing whitespaces`
```

## Defining custom parser behavior

```python
# export DOMAIN='http://myapp.com'
# export COLOR=invalid

from furl import furl


# Register a new parser method for paths
@env.parser_for("furl")
def furl_parser(value):
    return furl(value)


domain = env.furl("DOMAIN")  # => furl('https://myapp.com')


# Custom parsers can take extra keyword arguments
@env.parser_for("choice")
def choice_parser(value, choices):
    if value not in choices:
        raise environs.EnvError("Invalid!")
    return value


color = env.choice("COLOR", choices=["black"])  # => raises EnvError
```

## Usage with Flask

```python
# myapp/settings.py

from environs import Env

env = Env()
env.read_env()

# Override in .env for local development
DEBUG = env.bool("FLASK_DEBUG", default=False)
# SECRET_KEY is required
SECRET_KEY = env.str("SECRET_KEY")
```

Load the configuration after you initialize your app.

```python
# myapp/app.py

from flask import Flask

app = Flask(__name__)
app.config.from_object("myapp.settings")
```

For local development, use a `.env` file to override the default
configuration.

```bash
# .env
DEBUG=true
SECRET_KEY="not so secret"
```

Note: Because environs depends on [python-dotenv](https://github.com/theskumar/python-dotenv),
the `flask` CLI will automatically read .env and .flaskenv files.

## Usage with Django

environs includes a number of helpers for parsing connection URLs. To
install environs with django support:

    pip install environs[django]

Use `env.dj_db_url`, `env.dj_cache_url` and `env.dj_email_url` to parse the `DATABASE_URL`, `CACHE_URL`
and `EMAIL_URL` environment variables, respectively.

For more details on URL patterns, see the following projects that environs is using for converting URLs.

- [dj-database-url](https://github.com/jacobian/dj-database-url)
- [django-cache-url](https://github.com/epicserve/django-cache-url)
- [dj-email-url](https://github.com/migonzalvar/dj-email-url)

Basic example:

```python
# myproject/settings.py
from environs import Env

env = Env()
env.read_env()

# Override in .env for local development
DEBUG = env.bool("DEBUG", default=False)
# SECRET_KEY is required
SECRET_KEY = env.str("SECRET_KEY")

# Parse database URLs, e.g.  "postgres://localhost:5432/mydb"
DATABASES = {"default": env.dj_db_url("DATABASE_URL")}

# Parse email URLs, e.g. "smtp://"
email = env.dj_email_url("EMAIL_URL", default="smtp://")
EMAIL_HOST = email["EMAIL_HOST"]
EMAIL_PORT = email["EMAIL_PORT"]
EMAIL_HOST_PASSWORD = email["EMAIL_HOST_PASSWORD"]
EMAIL_HOST_USER = email["EMAIL_HOST_USER"]
EMAIL_USE_TLS = email["EMAIL_USE_TLS"]

# Parse cache URLS, e.g "redis://localhost:6379/0"
CACHES = {"default": env.dj_cache_url("CACHE_URL")}
```

For local development, use a `.env` file to override the default
configuration.

```bash
# .env
DEBUG=true
SECRET_KEY="not so secret"
```

For a more complete example, see
[django_example.py](https://github.com/sloria/environs/blob/master/examples/django_example.py)
in the `examples/` directory.

## Why\...?

### Why envvars?

See [The 12-factor App](http://12factor.net/config) section on
[configuration](http://12factor.net/config).

### Why not `os.environ`?

While `os.environ` is enough for simple use cases, a typical application
will need a way to manipulate and validate raw environment variables.
environs abstracts common tasks for handling environment variables.

environs will help you

- cast envvars to the correct type
- specify required envvars
- define default values
- validate envvars
- parse list and dict values
- parse dates, datetimes, and timedeltas
- parse expanded variables
- serialize your configuration to JSON, YAML, etc.

### Why another library?

There are many great Python libraries for parsing environment variables.
In fact, most of the credit for environs\' public API goes to the
authors of [envparse](https://github.com/rconradharris/envparse) and
[django-environ](https://github.com/joke2k/django-environ).

environs aims to meet three additional goals:

1.  Make it easy to extend parsing behavior and develop plugins.
2.  Leverage the deserialization and validation functionality provided
    by a separate library (marshmallow).
3.  Clean up redundant API.

See [this GitHub
issue](https://github.com/rconradharris/envparse/issues/12#issue-151036722)
which details specific differences with envparse.

## License

MIT licensed. See the
[LICENSE](https://github.com/sloria/environs/blob/master/LICENSE) file
for more details.
