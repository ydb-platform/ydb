# pyconfig - Python-based singleton configuration

[![Build Status](https://travis-ci.org/shakefu/pyconfig.svg?branch=master)](https://travis-ci.org/shakefu/pyconfig)

This module provides python based configuration that is stored in a singleton
object to ensure consistency across your project.

## Table of Contents

- [Command Line](#command-line)
- [Etcd](#etcd)
- [Code Examples](#code-examples)
- [Changes](#changes)
- [Contributors](#contributors)

## Command Line

Pyconfig has a command line utility that lets you inspect your project to find
all the configuration keys defined.

```bash
$ pyconfig -h
usage: pyconfig [-h] [-f F | -m M] [-v] [-l] [-a | -k] [-n] [-s] [-c]

Helper for working with pyconfigs

optional arguments:
  -h, --help          show this help message and exit
  -f F, --filename F  parse an individual file or directory
  -m M, --module M    parse a package or module, recursively looking inside it
  -v, --view-call     show the actual pyconfig call made (default: show namespace)
  -l, --load-configs  query the currently set value for each key found
  -a, --all           show keys which don't have defaults set
  -k, --only-keys     show a list of discovered keys without values
  -n, --natural-sort  sort by filename and line (default: alphabetical by key)
  -s, --source        show source annotations (implies --natural-sort)
  -c, --color         toggle output colors (default: True)
```

### Examples

```bash
$ pyconfig --file .
humbledb.allow_explicit_request = True
humbledb.auto_start_request = True
humbledb.connection_pool = 300
humbledb.tz_aware = True
humbledb.use_greenlets = False
humbledb.write_concern = 1

$ pyconfig --view-call --file .
pyconfig.get('humbledb.allow_explicit_request', True)
pyconfig.setting('humbledb.auto_start_request', True)
pyconfig.setting('humbledb.connection_pool', 300)
pyconfig.setting('humbledb.tz_aware', True)
pyconfig.setting('humbledb.use_greenlets', False)
pyconfig.setting('humbledb.write_concern', 1)

$ pyconfig --source --file .
# ./humbledb/mongo.py, line 98
humbledb.allow_explicit_request = True
# ./humbledb/mongo.py, line 178
humbledb.connection_pool = 300
# ./humbledb/mongo.py, line 181
humbledb.auto_start_request = True
# ./humbledb/mongo.py, line 185
humbledb.use_greenlets = False
# ./humbledb/mongo.py, line 188
humbledb.tz_aware = True
# ./humbledb/mongo.py, line 191
humbledb.write_concern = 1
```

## Etcd

### Version added: 3.0.0

Pyconfig has read-only support for configurations stored in etcd. The preferred
method for configuring Pyconfig to work with etcd is via ENV variables, since
they must be set as early as possible. It is also possible to use the Python
API to make Pyconfig work with etcd.

Pyconfig uses a directory namespace to store its dot notation configuration key
names. By default, that namespace is `/config/`.

At a minimum, `PYCONFIG_ETCD_HOSTS` must be set to get Pyconfig to try to
read a configuration from etcd using the default settings.

You can set a value with `etcdctl` like:

```bash
# The etcdctl command is provided by etcd and not part of pyconfig
etcdctl set /pyconfig/example/my.setting "from etcd"
```

And configure Pyconfig to connect and use that setting:

```bash
$ export PYCONFIG_ETCD_PREFIX="/pyconfig/example/"
$ export PYCONFIG_ETCD_HOSTS="127.0.0.1:2379"
$ python
>>> import pyconfig
>>> pyconfig.get('my.setting')
'from etcd'
```

Because of Pyconfig's singleton nature, only one configuration can be accessed
at a time in this way.

**Environment variables:**

- `PYCONFIG_ETCD_PREFIX` - The namespace to prefix settings with (default:
  `'/config/'`)
- `PYCONFIG_ETCD_HOSTS` - A comma separated list of hosts, like
  `10.0.0.1:2379,10.0.0.2:2379`
- `PYCONFIG_ETCD_CACERT` - CA cert file to use for SSL
- `PYCONFIG_ETCD_CERT` - Client cert file to use for SSL client authentication
- `PYCONFIG_ETCD_KEY` - Client private key file to use for SSL client auth
- `PYCONFIG_ETCD_WATCH` - If this is set to a truthy value (a non-empty
  string), then pyconfig will keep the local configuration synchronized with
  etcd (_Version added: 3.1.0_)
- `PYCONFIG_ETCD_PROTOCOL` - Set this to force HTTPS connections even if not
  using certificates. This should be a string of the form `https` or `http`.
  (_Version added: 3.2.0_)
- `PYCONFIG_ETCD_AUTH` - Set this use Basic Authentication with requests.
  This should be a string of the format `username:password`. (_Version added:
  3.2.0_)

**Inheritance:**

If you want to create a configuration that inherits from an existing
configuration, Pyconfig will look for a special key, which by default is set to
`config.inherit`. If this exists and is set to an etcd namespace, that
configuration will be used as the base for the current config.

A typical use case would be a Test environment configuration which is derived
from a Development config. Below is a barebones example of how that might be
set up using `etcdctl` and Pyconfig.

```bash
$ # Create the development settings
$ etcdctl set /config/app/dev/my.name example
$ etcdctl set /config/app/dev/my.hostname localhost
$ etcdctl set /config/app/dev/my.api.key abcdef0123456789
$ # Create the test settings
$ etcdctl set /config/app/test/my.hostname test.example.com
$ # Tell it to inherit from the development settings
$ etcdctl set /config/app/test/config.inherit /config/app/dev/
$ # Configure Pyconfig to use the test configuration
$ export PYCONFIG_ETCD_PREFIX="/config/app/test/"
$ export PYCONFIG_ETCD_HOSTS="127.0.0.1:2379"
$ python
>>> import pyconfig
>>> pyconfig.get('my.hostname')
'test.example.com'
>>> pyconfig.get('my.name')
'example'
```

## Code Examples

The most basic usage allows you to get, retrieve and modify values. Pyconfig's
singleton provides convenient accessor methods for these actions:

### Version changed: 3.0.0

As of version 3.0.0, keys are not case sensitive by default.

```python
>>> import pyconfig
>>> pyconfig.get('my.setting', 'default')
'default'
>>> pyconfig.set('my.setting', 'new')
>>> pyconfig.get('my.setting', 'default')
'new'
>>> pyconfig.reload(clear=True)
>>> pyconfig.get('my.setting', 'default')
'default'
```

You can also opt-out of default values:

```python
>>> import pyconfig
>>> pyconfig.get('my.setting', allow_default=False)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "pyconfig/__init__.py", line 275, in get
    return Config().get(name, default, allow_default=allow_default)
  File "pyconfig/__init__.py", line 234, in get
    return self.settings[name]
 LookupError: No setting "my.setting"
```

Pyconfig also provides shortcuts for giving classes property descriptors which
map to the current setting stored in the singleton:

```python
>>> import pyconfig
>>> class MyClass(object):
...     my_setting = pyconfig.setting('my.setting', 'default')
...
>>> MyClass.my_setting
'default'
>>> MyClass().my_setting
'default'
>>> pyconfig.set('my.setting', "Hello World!")
>>> MyClass.my_setting
'Hello World!'
>>> MyClass().my_setting
'Hello World!'
>>> pyconfig.reload(clear=True)
>>> MyClass.my_setting
'default'
```

The `Setting` class also supports preventing default values. When set this way,
all reads on the attribute will prevent the use of defaults:

```python
>>> import pyconfig
>>> class MyClass(object):
...     my_setting = pyconfig.setting('my.setting', allow_default=False)
...
>>> MyClass.my_setting
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "pyconfig/__init__.py", line 84, in __get__
    allow_default=self.allow_default)
  File "pyconfig/__init__.py", line 232, in get
    raise LookupError('No setting "{}"'.format(name))
LookupError: No setting "my.setting"
>>> pyconfig.set('my.setting', 'new_value')
>>> MyClass.my_setting
'value'
```

Pyconfig allows you to override settings via a python configuration file, that
defines its configuration keys as a module namespace. By default, Pyconfig will
look on your `PYTHONPATH` for a module named `localconfig`, and if it exists, it
will use this module namespace to update all configuration settings:

```python
# __file__ = "$PYTHONPATH/localconfig.py"
from pyconfig import Namespace

# Namespace objects allow you to use attribute assignment to create setting
# key names
my = Namespace()
my.setting = 'from_localconfig'
# Namespace objects implicitly return new nested Namespaces when accessing
# attributes that don't exist
my.nested.setting = 'also_from_localconfig'
```

With a `localconfig` on the `PYTHONPATH`, it will be loaded before any settings
are read:

```python
>>> import pyconfig
>>> pyconfig.get('my.setting')
'from_localconfig'
>>> pyconfig.get('my.nested.setting')
'also_from_localconfig'
```

Pyconfig also allows you to create distutils plugins that are automatically
loaded. An example `setup.py`:

```python
# __file__ = setup.py
from setuptools import setup

setup(
        name='pytest',
        version='0.1.0-dev',
        py_modules=['myconfig', 'anyconfig'],
        entry_points={
            # The "my" in "my =" indicates a base namespace to use for
            # the contained configuration. If you do not wish a base
            # namespace, use "any"
            'pyconfig':[
                  'my = myconfig',
                  'any = anyconfig',
                  ],
            },
        )
```

An example distutils plugin configuration file:

```python
# __file__ = myconfig.py
from pyconfig import Namespace

def some_callable():
    print "This callable was called."
    print "You can execute any arbitrary code."

setting = 'from_plugin'
nested = Namespace()
nested.setting = 'also_from_plugin'
```

Another example configuration file, without a base namespace:

```python
# __file__ = anyconfig.py
from pyconfig import Namespace
other = Namespace()
other.setting = 'anyconfig_value'
```

Showing the plugin-specified settings:

```python
>>> import pyconfig
>>> pyconfig.get('my.setting', 'default')
This callable was called.
You can execute any arbitrary code.
'from_plugin'
>>> pyconfig.get('my.nested.setting', 'default')
'also_from_plugin'
>>> pyconfig.get('other.setting', 'default')
'anyconfig_value'
```

More fancy stuff:

```python
>>> # Reloading changes re-calls functions...
>>> pyconfig.reload()
This callable was called.
You can execute any arbitrary code.
>>> # This can be used to inject arbitrary code by changing a
>>> # localconfig.py or plugin and reloading a config... especially
>>> # when pyconfig.reload() is attached to a signal
>>> import signal
>>> signal.signal(signal.SIGUSR1, pyconfig.reload)
```

Pyconfig provides a `@reload_hook` decorator that allows you to register
functions or methods to be called when the configuration is reloaded:

```python
>>> import pyconfig
>>> @pyconfig.reload_hook
... def reload():
...     print "Do something here."
...
>>> pyconfig.reload()
Do something here.
```

**Warning**: It should not be used to register large numbers of functions (e.g.
registering a bound method in a class's `__init__` method), since there is no
way to un-register a hook and it will cause a memory leak, since a bound method
maintains a strong reference to the bound instance.

**Note**: Because the reload hooks are called without arguments, it will not
work with unbound methods or classmethods.

## Changes

This section contains descriptions of changes in each new version.

### 3.2.0

- Adds `PYCONFIG_ETCD_PROTOCOL` and `PYCONFIG_ETCD_AUTH`.

_Released August 17, 2017._

### 3.1.1

- Documentation fixes that makes rendering work on PyPI and GitHub again.

_Released June 16, 2016._

### 3.1.0

- Adds the ability to watch etcd for changes to values. This can be enabled by
  setting the environment variable `PYCONFIG_ETCD_WATCH=true`.

_Released June 3, 2016._

### 3.0.2

- Fixes an issue when using Python 3 compatibility in Python 2.7 and PyOpenSSL.

_Released September 28, 2015._

### 3.0.1

- Changes the default inherit depth to 2, which is more useful than 1.

### 3.0.0

- Adds support for loading configurations from etcd, with inheritance.
- Use `pytool.lang.Namespace` instead of alternate implementation.
- Drops support for Python 2.6 and 3.2.
- Pyconfig setting keys are now case insensitive by default (Use
  `pyconfig.set('pyconfig.case_sensitive', True)` to change the behavior)
- Adds new `clear()` method for wiping out the cached configuration.

### Older Versions

#### 2.2.1

- The command line tool will now attempt to handle source files which specify a
  non-ascii encoding gracefully.

#### 2.2.0

- Add `allow_default` keyword option to `get()` and `setting()`. Thanks
  to [yarbelk](https://github.com/yarbelk)!

#### 2.1.5

- Fix regression where `localconfig.py` wasn't being loaded on Python 2.7 due
  to a logic flow error. Whoops!

#### 2.1.4

- Broke Python 2.6 in 2.1.1, fixed again.

#### 2.1.2-2.1.3

- Package clean up and fixing README to work on PyPI again.

#### 2.1.1

- Fix bug that would break on Python 2.6 and 2.7 when using a localconfig.py.

#### 2.1.0

- Pyconfig now works on Python 3, thanks to
  [hfalcic](https://github.com/hfalcic)!

#### 2.0.0

- Pyconfig now has the ability to show you what config keys are defined in a
  directory.

#### 1.2.0

- No longer uses Python 2.7 `format()`. Should work on 2.6 and maybe earlier.

#### 1.1.2

- Move version string into `pyconfig.__version__`

#### 1.1.1

- Fix bug with setup.py that prevented installation

#### 1.1.0

- Allow for implicitly nesting Namespaces when accessing attributes that are
  undefined

## Contributors

- [shakefu](http://github.com/shakefu) - Creator and maintainer
- [hfalcic](https://github.com/hfalcic) - Python 3 compatability
- [yarbelk](https://github.com/yarbelk) - `allow_default` option
