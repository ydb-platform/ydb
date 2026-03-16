Confuse: painless YAML config files
===================================

.. image:: https://github.com/beetbox/confuse/actions/workflows/main.yml/badge.svg
    :target: https://github.com/beetbox/confuse/actions

.. image:: https://img.shields.io/pypi/v/confuse.svg
    :target: https://pypi.org/project/confuse/

**Confuse** is a configuration library for Python that uses YAML_. It takes care
of defaults, overrides, type checking, command-line integration, environment
variable support, human-readable errors, and standard OS-specific locations.

What It Does
------------

Here's what Confuse brings to the table:

- An **utterly sensible API** resembling dictionary-and-list structures but
  providing **transparent validation** without lots of boilerplate code. Type
  ``config['num_goats'].get(int)`` to get the configured number of goats and
  ensure that it's an integer.
- Combine configuration data from **multiple sources**. Using *layering*,
  Confuse allows user-specific configuration to seamlessly override system-wide
  configuration, which in turn overrides built-in defaults. An in-package
  ``config_default.yaml`` can be used to provide bottom-layer defaults using the
  same syntax that users will see. A runtime overlay allows the program to
  programmatically override and add configuration values.
- Look for configuration files in **platform-specific paths**. Like
  ``$XDG_CONFIG_HOME`` or ``~/.config`` on Unix; "Application Support" on macOS;
  ``%APPDATA%`` on Windows. Your program gets its own directory, which you can
  use to store additional data. You can transparently create this directory on
  demand if, for example, you need to initialize the configuration file on first
  run. And an environment variable can be used to override the directory's
  location.
- Integration with **command-line arguments** via argparse_ or optparse_ from
  the standard library. Use argparse's declarative API to allow command-line
  options to override configured defaults.
- Include configuration values from **environment variables**. Values undergo
  automatic type conversion, and nested dicts and lists are supported.

Installation
------------

Confuse is available on `PyPI <https://pypi.org/project/confuse/>`_ and can be
installed using ``pip``:

.. code-block:: sh

    pip install confuse

Using Confuse
-------------

`Confuse's documentation`_ describes its API in detail.

Credits
-------

Confuse was made to power beets_. Like beets, it is available under the `MIT
license`_.

.. _argparse: https://docs.python.org/dev/library/argparse.html

.. _beets: https://github.com/beetbox/beets

.. _configparser: https://docs.python.org/library/configparser.html

.. _confuse's documentation: https://confuse.readthedocs.io/en/latest/usage.html

.. _logging: https://docs.python.org/library/logging.html

.. _mit license: https://opensource.org/license/mit

.. _optparse: https://docs.python.org/dev/library/optparse.html

.. _yaml: https://yaml.org/
