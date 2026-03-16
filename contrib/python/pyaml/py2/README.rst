pretty-yaml (or pyaml)
======================

PyYAML-based python module to produce pretty and readable YAML-serialized data.

This module is for serialization only, see `ruamel.yaml`_ module for literate
YAML parsing (keeping track of comments, spacing, line/column numbers of values, etc).

[note: to dump stuff parsed by ruamel.yaml with this module, use only ``YAML(typ='safe')`` there]

.. contents::
  :backlinks: none


Warning
-------

Prime goal of this module is to produce human-readable output that can be easily
manipulated and re-used, but maybe with some occasional caveats.

One good example of such "caveat" is that e.g. ``{'foo': '123'}`` will serialize
to ``foo: 123``, which for PyYAML would be a bug, as 123 will then be read back
as an integer from that, but here it's a feature.

So please do not rely on the thing to produce output that can always be
deserialized exactly to what was exported, at least - use PyYAML (e.g. with
options from the next section) for that.


What this module does and why
-----------------------------

YAML is generally nice and easy format to read *if* it was written by humans.

PyYAML can a do fairly decent job of making stuff readable, and the best
combination of parameters for such output that I've seen so far is probably this one::

  >>> m = [123, 45.67, {1: None, 2: False}, u'some text']
  >>> data = dict(a=u'asldnsa\nasldpáknsa\n', b=u'whatever text', ma=m, mb=m)
  >>> yaml.safe_dump(data, sys.stdout, allow_unicode=True, default_flow_style=False)
  a: 'asldnsa

    asldpáknsa

    '
  b: whatever text
  ma: &id001
  - 123
  - 45.67
  - 1: null
    2: false
  - some text
  mb: *id001

pyaml tries to improve on that a bit, with the following tweaks:

* Most human-friendly representation options in PyYAML (that I know of) get
  picked as defaults.

* Does not dump "null" values, if possible, replacing these with just empty
  strings, which have the same meaning but reduce visual clutter and are easier
  to edit.

* Dicts, sets, OrderedDicts, defaultdicts, namedtuples, etc are representable
  and get sorted on output (OrderedDicts and namedtuples keep their ordering),
  so that output would be as diff-friendly as possible, and not arbitrarily
  depend on python internals.

  It appears that at least recent PyYAML versions also do such sorting for
  python dicts.

* List items get indented, as they should be.

* bytestrings that can't be auto-converted to unicode raise error, as yaml has
  no "binary bytes" (i.e. unix strings) type.

* Attempt is made to pick more readable string representation styles, depending
  on the value, e.g.::

    >>> yaml.safe_dump(cert, sys.stdout)
    cert: '-----BEGIN CERTIFICATE-----

      MIIH3jCCBcagAwIBAgIJAJi7AjQ4Z87OMA0GCSqGSIb3DQEBCwUAMIHBMRcwFQYD

      VQQKFA52YWxlcm9uLm5vX2lzcDEeMBwGA1UECxMVQ2VydGlmaWNhdGUgQXV0aG9y
    ...

    >>> pyaml.p(cert):
    cert: |
      -----BEGIN CERTIFICATE-----
      MIIH3jCCBcagAwIBAgIJAJi7AjQ4Z87OMA0GCSqGSIb3DQEBCwUAMIHBMRcwFQYD
      VQQKFA52YWxlcm9uLm5vX2lzcDEeMBwGA1UECxMVQ2VydGlmaWNhdGUgQXV0aG9y
    ...

* "force_embed" option to avoid having &id stuff scattered all over the output
  (which might be beneficial in some cases, hence the option).

* "&id" anchors, if used, get labels from the keys they get attached to,
  not just use meaningless enumerators.

* "string_val_style" option to only apply to strings that are values, not keys,
  i.e::

    >>> pyaml.p(data, string_val_style='"')
    key: "value\nasldpáknsa\n"
    >>> yaml.safe_dump(data, sys.stdout, allow_unicode=True, default_style='"')
    "key": "value\nasldpáknsa\n"

* "sort_dicts=False" option to leave dict item ordering to python, and not
  force-sort them in yaml output, which can be important for python 3.6+ where
  they retain ordering info.

* Has an option to add vertical spacing (empty lines) between keys on different
  depths, to make output much more seekable.

Result for the (rather meaningless) example above (without any additional
tweaks)::

  >>> pyaml.p(data)
  a: |
    asldnsa
    asldpáknsa
  b: 'whatever text'
  ma: &ma
    - 123
    - 45.67
    - 1:
      2: false
    - 'some text'
  mb: *ma

----------

Extended example::

  >>> pyaml.dump(conf, sys.stdout, vspacing=[2, 1]):
  destination:

    encoding:
      xz:
        enabled: true
        min_size: 5120
        options:
        path_filter:
          - \.(gz|bz2|t[gb]z2?|xz|lzma|7z|zip|rar)$
          - \.(rpm|deb|iso)$
          - \.(jpe?g|gif|png|mov|avi|ogg|mkv|webm|mp[34g]|flv|flac|ape|pdf|djvu)$
          - \.(sqlite3?|fossil|fsl)$
          - \.git/objects/[0-9a-f]+/[0-9a-f]+$

    result:
      append_to_file:
      append_to_lafs_dir:
      print_to_stdout: true

    url: http://localhost:3456/uri


  filter:
    - /(CVS|RCS|SCCS|_darcs|\{arch\})/$
    - /\.(git|hg|bzr|svn|cvs)(/|ignore|attributes|tags)?$
    - /=(RELEASE-ID|meta-update|update)$


  http:

    ca_certs_files: /etc/ssl/certs/ca-certificates.crt

    debug_requests: false

    request_pool_options:
      cachedConnectionTimeout: 600
      maxPersistentPerHost: 10
      retryAutomatically: true


  logging:

    formatters:
      basic:
        datefmt: '%Y-%m-%d %H:%M:%S'
        format: '%(asctime)s :: %(name)s :: %(levelname)s: %(message)s'

    handlers:
      console:
        class: logging.StreamHandler
        formatter: basic
        level: custom
        stream: ext://sys.stderr

    loggers:
      twisted:
        handlers:
          - console
        level: 0

    root:
      handlers:
        - console
      level: custom

Note that unless there are many moderately wide and deep trees of data, which
are expected to be read and edited by people, it might be preferrable to
directly use PyYAML regardless, as it won't introduce another (rather pointless
in that case) dependency and a point of failure.


Some Tricks
-----------

* Pretty-print any yaml or json (yaml subset) file from the shell::

    % python -m pyaml /path/to/some/file.yaml
    % curl -s https://www.githubstatus.com/api/v2/summary.json | python -m pyaml

* Process and replace json/yaml file in-place::

    % python -m pyaml -r file-with-json.data

* Easier "debug printf" for more complex data (all funcs below are aliases to
  same thing)::

    pyaml.p(stuff)
    pyaml.pprint(my_data)
    pyaml.pprint('----- HOW DOES THAT BREAKS!?!?', input_data, some_var, more_stuff)
    pyaml.print(data, file=sys.stderr) # needs "from __future__ import print_function"

* Force all string values to a certain style (see info on these in
  `PyYAML docs`_)::

    pyaml.dump(many_weird_strings, string_val_style='|')
    pyaml.dump(multiline_words, string_val_style='>')
    pyaml.dump(no_want_quotes, string_val_style='plain')

  Using ``pyaml.add_representer()`` (note \*p\*yaml) as suggested in
  `this SO thread`_ (or `github-issue-7`_) should also work.

* Control indent and width of the results::

    pyaml.dump(wide_and_deep, indent=4, width=120)

  These are actually keywords for PyYAML Emitter (passed to it from Dumper),
  see more info on these in `PyYAML docs`_.

* Dump multiple yaml documents into a file: ``pyaml.dump_all([data1, data2, data3], dst_file)``

  explicit_start=True is implied, unless explicit_start=False is passed.

.. _PyYAML docs: http://pyyaml.org/wiki/PyYAMLDocumentation#Scalars
.. _this SO thread: http://stackoverflow.com/a/7445560
.. _github-issue-7: https://github.com/mk-fg/pretty-yaml/issues/7


Installation
------------

It's a regular package for Python (3.x or 2.x).

Module uses PyYAML_ for processing of the actual YAML files and should pull it
in as a dependency.

Dependency on unidecode_ module is optional and should only be necessary if
same-id objects or recursion is used within serialized data.

Be sure to use python3/python2, pip3/pip2, easy_install-... binaries below,
based on which python version you want to install the module for, if you have
several on the system (as is norm these days for py2-py3 transition).

Using pip_ is the best way::

  % pip install pyaml

(add --user option to install into $HOME for current user only)

Or, if you don't have "pip" command::

  % python -m ensurepip
  % python -m pip install --upgrade pip
  % python -m pip install pyaml

(same suggestion wrt "install --user" as above)

On a very old systems, one of these might work::

  % curl https://bootstrap.pypa.io/get-pip.py | python
  % pip install pyaml

  % easy_install pyaml

  % git clone --depth=1 https://github.com/mk-fg/pretty-yaml
  % cd pretty-yaml
  % python setup.py install

(all of install-commands here also have --user option,
see also `pip docs "installing" section`_)

Current-git version can be installed like this::

  % pip install 'git+https://github.com/mk-fg/pretty-yaml#egg=pyaml'

Note that to install stuff to system-wide PATH and site-packages (without
--user), elevated privileges (i.e. root and su/sudo) are often required.

Use "...install --user", `~/.pydistutils.cfg`_ or virtualenv_ to do unprivileged
installs into custom paths.

More info on python packaging can be found at `packaging.python.org`_.

.. _ruamel.yaml: https://bitbucket.org/ruamel/yaml/
.. _PyYAML: http://pyyaml.org/
.. _unidecode: http://pypi.python.org/pypi/Unidecode
.. _pip: http://pip-installer.org/
.. _pip docs "installing" section: http://www.pip-installer.org/en/latest/installing.html
.. _~/.pydistutils.cfg: http://docs.python.org/install/index.html#distutils-configuration-files
.. _virtualenv: http://pypi.python.org/pypi/virtualenv
.. _packaging.python.org: https://packaging.python.org/installing/
