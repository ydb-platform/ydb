#
# Copyright (C) 2017, 2018 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
r"""Pickle backend:

- Format to support: Pickle
- Requirements: It should be available always.

  - pickle/cPickle in python 2 standard library:
    https://docs.python.org/2/library/pickle.html

  - pickle in python 3 standard library:
    https://docs.python.org/3/library/pickle.html

- Development Status :: 4 - Beta
- Limitations: The parser cannot load some primitive data such like '' (empty
  string), ' ' (white space) and [] (empty list) as these are because of the
  implementation of :func:`anyconfig.backend.base.load_with_fn`.
- Special options: All options of pickle.{load{s,},dump{s,}} should work.

Changelog:

.. versionchanged:: 0.9.7

   - Add support of loading primitives other than mapping objects.

.. versionadded:: 0.8.3
"""
from __future__ import absolute_import

try:
    import cPickle as pickle
except ImportError:
    import pickle

import anyconfig.backend.base
import anyconfig.compat


if anyconfig.compat.IS_PYTHON_3:
    LOAD_OPTS = ["fix_imports", "encoding", "errors"]
    DUMP_OPTS = ["protocol", "fix_imports"]
else:
    LOAD_OPTS = []
    DUMP_OPTS = ["protocol"]


class Parser(anyconfig.backend.base.StringStreamFnParser,
             anyconfig.backend.base.BinaryFilesMixin):
    """
    Parser for Pickle files.
    """
    _cid = "pickle"
    _type = "pickle"
    _extensions = ["pkl", "pickle"]
    _load_opts = LOAD_OPTS
    _dump_opts = DUMP_OPTS
    _allow_primitives = True

    _load_from_string_fn = anyconfig.backend.base.to_method(pickle.loads)
    _load_from_stream_fn = anyconfig.backend.base.to_method(pickle.load)
    _dump_to_string_fn = anyconfig.backend.base.to_method(pickle.dumps)
    _dump_to_stream_fn = anyconfig.backend.base.to_method(pickle.dump)

# vim:sw=4:ts=4:et:
