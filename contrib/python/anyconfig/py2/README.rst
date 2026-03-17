=================
python-anyconfig
=================

.. image:: https://img.shields.io/pypi/v/anyconfig.svg
   :target: https://pypi.python.org/pypi/anyconfig/
   :alt: [Latest Version]

.. image:: https://img.shields.io/pypi/pyversions/anyconfig.svg
   :target: https://pypi.python.org/pypi/anyconfig/
   :alt: [Python versions]

.. image:: https://img.shields.io/pypi/l/anyconfig.svg
   :target: https://pypi.python.org/pypi/anyconfig/
   :alt: MIT License

.. image:: https://img.shields.io/travis/ssato/python-anyconfig.svg
   :target: https://travis-ci.org/ssato/python-anyconfig
   :alt: [Test status]

.. image:: https://img.shields.io/coveralls/ssato/python-anyconfig.svg
   :target: https://coveralls.io/r/ssato/python-anyconfig
   :alt: [Coverage Status]

.. .. image:: https://landscape.io/github/ssato/python-anyconfig/master/landscape.svg?style=flat
   :target: https://landscape.io/github/ssato/python-anyconfig/master
   :alt: [Code Health]

.. image:: https://scrutinizer-ci.com/g/ssato/python-anyconfig/badges/quality-score.png?b=master
   :target: https://scrutinizer-ci.com/g/ssato/python-anyconfig
   :alt: [Code Quality by Scrutinizer]

.. image:: https://img.shields.io/lgtm/grade/python/g/ssato/python-anyconfig.svg
   :target: https://lgtm.com/projects/g/ssato/python-anyconfig/context:python
   :alt: [Code Quality by LGTM]

.. .. image:: https://www.openhub.net/p/python-anyconfig/widgets/project_thin_badge.gif
   :target: https://www.openhub.net/p/python-anyconfig
   :alt: [Open HUB]

.. image:: https://readthedocs.org/projects/python-anyconfig/badge/?version=latest
   :target: http://python-anyconfig.readthedocs.io/en/latest/?badge=latest
   :alt: [Doc Status]


Introduction
=============

python-anyconfig [#]_ is a python library provides common APIs to load and dump
configuration files in various formats with some useful features such as
contents merge, templates, query, schema validation and generation support.

- Home: https://github.com/ssato/python-anyconfig
- Author: Satoru SATOH <satoru.satoh@gmail.com>
- License: `MIT licensed <http://opensource.org/licenses/MIT>`_
- Document: http://python-anyconfig.readthedocs.org/en/latest/
- Download:

  - PyPI: https://pypi.python.org/pypi/anyconfig
  - Copr RPM repos: https://copr.fedoraproject.org/coprs/ssato/python-anyconfig/

.. [#] This name took an example from the 'anydbm' python standard library.

Features
----------

python-anyconfig provides very simple and unified APIs to process configuration
files in various formats and related functions:

- Loading configuration files:

  **anyconfig.load** (path_specs, ac_parser=None, ac_dict=None, ac_template=False, ac_context=None, \*\*options)
    loads configuration data from `path_specs`. `path_specs` may be a list of
    file paths, files or file-like objects, `~pathlib.Path` class object, a
    namedtuple `~anyconfig.globals.IOInfo` objects represents some inputs to
    load data from, and return a dict or dict like object, or a primitive
    types' data other than dict represents loaded configuration.

  **anyconfig.loads** (content, ac_parser=None, ac_dict=None, ac_template=False, ac_context=None, \*\*options)
    loads configuration data from a string just like json.loads does.

- Dumping configuration files:

  **anyconfig.dump** (data, out, ac_parser=None, \*\*options)
    dumps a configuration data `data` in given format to the output `out`, may
    be a file, file like object.

  **anyconfig.dumps** (data, ac_parser=None, \*\*options)
    dumps a configuration data loaded from a string

- Open configuration files:

  **anyconfig.open** (path, mode=None, ac_parser=None, \*\*options)
    open configuration files with suitable flags and return file/file-like
    objects, and this object can be passed to the anyconfig.load().

- Merge dicts:

  **anyconfig.merge** (self, other, ac_merge=MS_DICTS, \*\*options)
    Update (merge) a mapping object 'self' with other mapping object 'other' or
    an iterable 'other' yields (key, value) tuples according to merge strategy
    'ac_merge'.

- Schema validation and generation of configuration files:

  **anyconfig.validate** (data, schema, ac_schema_safe=True, ac_schema_errors=False, \*\*options)
    validates configuration data loaded with anyconfig.load() with JSON schema
    [#]_ object also loaded with anyconfig.load(). anyconfig.load() may help
    loading JSON schema file[s] in any formats anyconfig supports.

  **anyconfig.gen_schema** (data, \*\*options)
    generates a mapping object represents a minimum JSON schema to validate
    configuration data later. This result object can be serialized to any
    formats including JSON with anyconfig.dump or anyconfig.dumps.

It enables to load configuration file[s] in various formats in the same manner,
and in some cases, even there is no need to take care of the actual format of
configuration file[s] like the followings:

.. code-block:: python

  import anyconfig

  # Config type (format) is automatically detected by filename (file
  # extension) in some cases.
  conf1 = anyconfig.load("/path/to/foo/conf.d/a.yml")

  # Similar to the above but the input is pathlib.Path object.
  import pathlib
  path_1 = pathlib.Path("/path/to/foo/conf.d/a.yml")
  conf1_1 = anyconfig.load(path_1)

  # Similar to the first one but load from file object opened:
  with anyconfig.open("/path/to/foo/conf.d/a.yml") as fileobj:
      conf1_2 = anyconfig.load(fileobj)

  # Loaded config data is a mapping object, for example:
  #
  #   conf1["a"] => 1
  #   conf1["b"]["b1"] => "xyz"
  #   conf1["c"]["c1"]["c13"] => [1, 2, 3]

  # Or you can specify the format (config type) explicitly if its automatic
  # detection may not work.
  conf2 = anyconfig.load("/path/to/foo/conf.d/b.conf", ac_parser="yaml")

  # Likewise.
  with anyconfig.open("/path/to/foo/conf.d/b.conf") as fileobj:
      conf2_2 = anyconfig.load(fileobj, ac_parser="yaml")

  # Specify multiple config files by the list of paths. Configurations of each
  # files will be merged.
  conf3 = anyconfig.load(["/etc/foo.d/a.json", "/etc/foo.d/b.json"])

  # Similar to the above but all or one of config file[s] might be missing.
  conf4 = anyconfig.load(["/etc/foo.d/a.json", "/etc/foo.d/b.json"],
                         ac_ignore_missing=True)

  # Specify config files by glob path pattern:
  conf5 = anyconfig.load("/etc/foo.d/*.json")

  # Similar to the above, but parameters in the former config file will be simply
  # overwritten by the later ones instead of merge:
  conf6 = anyconfig.load("/etc/foo.d/*.json", ac_merge=anyconfig.MS_REPLACE)

Also, it can process configuration files which are
`jinja2-based template <http://jinja.pocoo.org>`_ files:

- Enables to load a substantial configuration rendered from half-baked configuration template files with given context
- Enables to load a series of configuration files indirectly 'include'-d from a/some configuration file[s] with using jinja2's 'include' directive.

.. code-block:: console

  In [1]: import anyconfig

  In [2]: open("/tmp/a.yml", 'w').write("a: {{ a|default('aaa') }}\n")

  In [3]: anyconfig.load("/tmp/a.yml", ac_template=True)
  Out[3]: {'a': 'aaa'}

  In [4]: anyconfig.load("/tmp/a.yml", ac_template=True, ac_context=dict(a='bbb'))
  Out[4]: {'a': 'bbb'}

  In [5]: open("/tmp/b.yml", 'w').write("{% include 'a.yml' %}\n")  # 'include'

  In [6]: anyconfig.load("/tmp/b.yml", ac_template=True, ac_context=dict(a='ccc'))
  Out[6]: {'a': 'ccc'}

And python-anyconfig enables to validate configuration files in various formats
with using JSON schema like the followings:

.. code-block:: python

  # Validate a JSON config file (conf.json) with JSON schema (schema.yaml).
  # If validatation suceeds, `rc` -> True, `err` -> ''.
  conf1 = anyconfig.load("/path/to/conf.json")
  schema1 = anyconfig.load("/path/to/schema.yaml")
  (rc, err) = anyconfig.validate(conf1, schema1)  # err is empty if success, rc == 0

  # Validate a config file (conf.yml) with JSON schema (schema.yml) while
  # loading the config file.
  conf2 = anyconfig.load("/a/b/c/conf.yml", ac_schema="/c/d/e/schema.yml")

  # Validate config loaded from multiple config files with JSON schema
  # (schema.json) while loading them.
  conf3 = anyconfig.load("conf.d/*.yml", ac_schema="/c/d/e/schema.json")

  # Generate jsonschema object from config files loaded and get string
  # representation.
  conf4 = anyconfig.load("conf.d/*.yml")
  scm4 = anyconfig.gen_schema(conf4)
  scm4_s = anyconfig.dumps(scm4, "json")

And you can query loaded data with JMESPath [#]_ expressions:

.. code-block:: python

  In [2]: dic = dict(a=dict(b=[dict(c="C", d=0)]))

  In [3]: anyconfig.loads(anyconfig.dumps(dic, ac_parser="json"),
     ...:                 ac_parser="json", ac_query="a.b[0].c")
  Out[3]: u'C'

  In [4]:

And in the last place, python-anyconfig provides a CLI tool called
anyconfig_cli to process configuration files and:

- Convert a/multiple configuration file[s] to another configuration files in different format
- Get configuration value in a/multiple configuration file[s]
- Validate configuration file[s] with JSON schema
- Generate minimum JSON schema file to validate given configuration file[s]

.. [#] http://json-schema.org
.. [#] http://jmespath.org

Supported configuration formats
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

python-anyconfig supports various file formats if requirements are satisfied
and backends in charge are enabled and ready to use:

- Always supported formats of which backends are enabled by default:

.. csv-table:: Always supported formats
   :header: "Format", "Type", "Requirement"
   :widths: 15, 10, 40

   JSON, json, ``json`` (standard lib) or ``simplejson`` [#]_
   Ini-like, ini, ``configparser`` (standard lib)
   Pickle, pickle, ``pickle`` (standard lib)
   XML, xml, ``ElementTree`` (standard lib)
   Java properties [#]_ , properties, None (native implementation with standard lib)
   B-sh, shellvars, None (native implementation with standard lib)

- Supported formats of which backends are enabled automatically if requirements are satisfied:

.. csv-table:: Supported formarts if requirements are satisfied
   :header: "Format", "Type", "Requirement"
   :widths: 15, 10, 40

   YAML, yaml, ``ruamel.yaml`` [#]_ or ``PyYAML`` [#]_
   TOML, toml, ``toml`` [#]_

- Supported formats of which backends are enabled automatically if required plugin modules are installed: python-anyconfig utilizes plugin mechanism provided by setuptools [#]_ and may support other formats if corresponding plugin backend modules are installed along with python-anyconfig:

.. csv-table:: Supported formats by pluggable backend modules
   :header: "Format", "Type", "Required backend"
   :widths: 15, 10, 40

   Amazon Ion, ion, ``anyconfig-ion-backend`` [#]_
   BSON, bson, ``anyconfig-bson-backend`` [#]_
   CBOR, cbor, ``anyconfig-cbor-backend`` [#]_ or ``anyconfig-cbor2-backend`` [#]_
   ConifgObj, configobj, ``anyconfig-configobj-backend`` [#]_
   MessagePack, msgpack, ``anyconfig-msgpack-backend`` [#]_

The supported formats of python-anyconfig on your system are able to be listed
by 'anyconfig_cli -L' like this:

.. code-block:: console

  $ anyconfig_cli -L
  Supported config types: bson, configobj, ini, json, msgpack, toml, xml, yaml
  $

or with the API 'anyconfig.list_types()' will show them: 

.. code-block:: console

   In [8]: anyconfig.list_types()
   Out[8]: ['bson', 'configobj', 'ini', 'json', 'msgpack', 'toml', 'xml', 'yaml']

   In [9]:

.. [#] https://pypi.python.org/pypi/simplejson
.. [#] ex. https://docs.oracle.com/javase/7/docs/api/java/util/Properties.html
.. [#] https://pypi.python.org/pypi/ruamel.yaml
.. [#] https://pypi.python.org/pypi/PyYAML
.. [#] https://pypi.python.org/pypi/toml
.. [#] http://peak.telecommunity.com/DevCenter/setuptools#dynamic-discovery-of-services-and-plugins
.. [#] https://pypi.python.org/pypi/anyconfig-ion-backend
.. [#] https://pypi.python.org/pypi/anyconfig-bson-backend
.. [#] https://pypi.python.org/pypi/anyconfig-cbor-backend
.. [#] https://pypi.python.org/pypi/anyconfig-cbor2-backend
.. [#] https://pypi.python.org/pypi/anyconfig-configobj-backend
.. [#] https://pypi.python.org/pypi/anyconfig-msgpack-backend

Installation
-------------

Requirements
^^^^^^^^^^^^^^

Many runtime dependencies are resolved dynamically and python-anyconfig just
disables specific features if required dependencies are not satisfied.
Therefore, only python standard library is required to install and use
python-anyconfig at minimum.

The following packages need to be installed along with python-anyconfig to
enable the features.

.. csv-table::
   :header: "Feature", "Requirements", "Notes"
   :widths: 20, 10, 25

   YAML load/dump, ruamel.yaml or PyYAML, ruamel.yaml will be used instead of PyYAML if it's available to support the YAML 1.2 specification.
   TOML load/dump, toml, none
   BSON load/dump, bson, bson from pymongo package may work and bson [#]_ does not
   Template config, Jinja2 [#]_ , none
   Validation with JSON schema, jsonschema [#]_ , Not required to generate JSON schema.
   Query with JMESPath expression, jmespath [#]_ , none

.. [#] https://pypi.python.org/pypi/bson/
.. [#] https://pypi.python.org/pypi/Jinja2/
.. [#] https://pypi.python.org/pypi/jsonschema/
.. [#] https://pypi.python.org/pypi/jmespath/

How to install
^^^^^^^^^^^^^^^^

There is a couple of ways to install python-anyconfig:

- Binary RPMs:

  If you're running Fedora 27 or later, or CentOS, you can install RPMs from
  these official yum repos. And if you're running Red Hat Enterprise Linux 7 or
  later, you can install RPMs from EPEL repos [#]_ .

  Or if you want to install the latest version, optionally, you can enable my
  copr repo, http://copr.fedoraproject.org/coprs/ssato/python-anyconfig/ .

- PyPI: You can install python-anyconfig from PyPI with using pip:

  .. code-block:: console

    $ pip install anyconfig

- pip from git repo:

  .. code-block:: console

     $ pip install git+https://github.com/ssato/python-anyconfig/

- Build RPMs from source: It's easy to build python-anyconfig with using rpm-build and mock:

  .. code-block:: console

    # Build Source RPM first and then build it with using mock (better way)
    $ python setup.py bdist_rpm --source-only && mock dist/python-anyconfig-<ver_dist>.src.rpm

  or

  .. code-block:: console

    # Build Binary RPM to install
    $ python setup.py bdist_rpm

  and install RPMs built.

- Build from source: Of course you can build and/or install python modules in usual way such like 'python setup.py bdist'.

.. [#] Thanks to Brett-san! https://src.fedoraproject.org/rpms/python-anyconfig/

Help and feedbak
-----------------

If you have any issues / feature request / bug reports with python-anyconfig,
please open issue tickets on github.com,
https://github.com/ssato/python-anyconfig/issues.

The following areas are still insufficient, I think.

- Make python-anyconfig robust for invalid inputs
- Make python-anyconfig scalable: some functions are limited by max recursion depth.
- Make python-anyconfig run faster: current implementation might be too complex and it run slower than expected as a result.
- Documentation:

  - Especially API docs need more fixes and enhancements! CLI doc is non-fulfilling also.
  - English is not my native lang and there may be many wrong and hard-to-understand expressions.

Any feedbacks, helps, suggestions are welcome! Please open github issues for
these kind of problems also!

.. vim:sw=2:ts=2:et:
