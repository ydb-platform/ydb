#
# Copyright (C) 2012 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# pylint: disable=unused-import,import-error,invalid-name
r"""Public APIs of anyconfig module.

.. versionchanged:: 0.9.9

   - Removed the API 'find_loader'
   - Added new APIs :func:`find` and :func:`findall` to :func:`find parsers`
     (loaders and dumpers) to suppport to find multiple parsers, and replace
     the API 'find_loader'
   - Added new APIs :func:`list_by_cid`, :func:`list_by_type` and
     :func:`list_by_extension` to list parsers by various viewpoints.

.. versionadded:: 0.9.8

   - Added new API load_plugins to [re-]load plugins

.. versionadded:: 0.9.5

   - Added pathlib support. Now all of load and dump APIs can process
     pathlib.Path object basically.
   - 'ignore_missing' keyword option for load APIs are now marked as deprecated
     and will be removed soon.
   - Allow to load data other than mapping objects for some backends such as
     JSON and YAML.

.. versionadded:: 0.8.3

   - Added ac_dict keyword option to pass dict factory (any callable like
     function or class) to make dict-like object in backend parsers.
   - Added ac_query keyword option to query data with JMESPath expression.
   - Added experimental query api to query data with JMESPath expression.
   - Removed ac_namedtuple keyword option.
   - Export :func:`merge`.
   - Stop exporting :func:`to_container` which was deprecated and removed.

.. versionadded:: 0.8.2

   - Added new API, version to provide version information.

.. versionadded:: 0.8.0

   - Removed set_loglevel API as it does not help much.
   - Added :func:`open` API to open files with appropriate open mode.
   - Added custom exception classes, :class:`UnknownProcessorTypeError` and
     :class:`UnknownFileTypeError` to express specific errors.
   - Change behavior of the API :func:`find_loader` and others to make them
     fail firt and raise exceptions (ValueError, UnknownProcessorTypeError or
     UnknownFileTypeError) as much as possible if wrong parser type for uknown
     file type was given.

.. versionadded:: 0.5.0

   - Most keyword arguments passed to APIs are now position independent.
   - Added ac_namedtuple parameter to \*load and \*dump APIs.

.. versionchanged:: 0.3

   - Replaced 'forced_type' optional argument of some public APIs with
     'ac_parser' to allow skip of config parser search by passing parser object
     previously found and instantiated.

     Also removed some optional arguments, 'ignore_missing', 'merge' and
     'marker', from definitions of some public APIs as these may not be changed
     from default in common use cases.

.. versionchanged:: 0.2

   - Now APIs :func:`find_loader`, :func:`single_load`, :func:`multi_load`,
     :func:`load` and :func:`dump` can process a file/file-like object or a
     list of file/file-like objects instead of a file path or a list of file
     paths.

.. versionadded:: 0.2

   - Export factory method (create) of anyconfig.mergeabledict.MergeableDict
"""
from __future__ import absolute_import

import warnings

# Import some global constants will be re-exported:
from anyconfig.globals import (  # noqa: F401
    LOGGER, IOI_PATH_OBJ, UnknownProcessorTypeError, UnknownFileTypeError
)
import anyconfig.globals
import anyconfig.dicts
import anyconfig.ioinfo
import anyconfig.template
import anyconfig.utils

from anyconfig.dicts import (  # noqa: F401
    MS_REPLACE, MS_NO_REPLACE, MS_DICTS, MS_DICTS_AND_LISTS, MERGE_STRATEGIES,
    get, set_, merge
)
from anyconfig.backends import Parsers
from anyconfig.schema import validate, gen_schema  # noqa: F401
from anyconfig.query import query


def version():
    """
    :return: A tuple of version info, (major, minor, release), e.g. (0, 8, 2)
    """
    return anyconfig.globals.VERSION.split('.')


def load_plugins():
    """[Re-]Load pluggable parsers.
    """
    Parsers().load_plugins()


def list_types():
    """List supported parser types.
    """
    return sorted(Parsers().list_x("type"))


def list_by_cid():
    """List supported parsers, [(cid, [Parser_class])].
    """
    return Parsers().list_by_x("cid")


def list_by_type():
    """List supported parser by types, [(type, [Parser_class])].
    """
    return Parsers().list_by_x("type")


def list_by_extension():
    """
    List supported parser by file extension supported, [(extension,
    [Parser_class])].
    """
    return Parsers().list_by_x("extensions")


def _try_validate(cnf, schema, **options):
    """
    :param cnf: Mapping object represents configuration data
    :param schema: JSON schema object
    :param options: Keyword options passed to :func:`jsonschema.validate`

    :return: Given 'cnf' as it is if validation succeeds else None
    """
    valid = True
    if schema:
        (valid, msg) = validate(cnf, schema, **options)
        if msg:
            LOGGER.warning(msg)

    if valid:
        return cnf

    return None


def _try_query(cnf, jexp, **options):
    """
    :param cnf: Mapping object represents configuration data
    :param jexp: JMESPath expression to query or None/False
    :param options: Keyword options currently not used

    :return: Maybe the result by querying with the JMESPath exp.
    :raises: jmespath.exceptions.*Error (ValueError)
    """
    if jexp:
        (cnf, exc) = query(cnf, jexp, **options)
        if exc:
            raise exc

    return cnf


def findall(obj=None, forced_type=None):
    """
    Find out parser objects can load and/or dump data from given 'obj' which
    may be a file path, file or file-like object, pathlib.Path object or an
    'anyconfig.globals.IOInfo' (namedtuple) object.

    :param obj:
        a file path, file or file-like object, pathlib.Path object, an
        'anyconfig.globals.IOInfo' (namedtuple) object, or None
    :param forced_type:
        Forced configuration parser type or parser object itself
    :return: A list of instances of processor classes to process 'obj'
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    return Parsers().findall(obj, forced_type=forced_type)


def find(obj=None, forced_type=None):
    """
    This function is very similar to the above :func:`find` but returns a
    parser objects instead of a list of parser objects.

    :param obj:
        a file path, file or file-like object, pathlib.Path object, an
        'anyconfig.globals.IOInfo' (namedtuple) object, or None
    :param forced_type:
        Forced configuration parser type or parser object itself
    :return:
        An instance of processor class of highest priority to process 'obj'
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    return Parsers().find(obj, forced_type=forced_type)


def _maybe_schema(**options):
    """
    :param options: Optional keyword arguments such as

        - ac_template: Assume configuration file may be a template file and try
          to compile it AAR if True
        - ac_context: Mapping object presents context to instantiate template
        - ac_schema: JSON schema file path to validate configuration files

    :return: Mapping object or None means some errors
    """
    ac_schema = options.get("ac_schema", None)
    if ac_schema is not None:
        # Try to detect the appropriate parser to load the schema data as it
        # may be different from the original config file's format, perhaps.
        options["ac_parser"] = None
        options["ac_schema"] = None  # Avoid infinite loop.
        LOGGER.info("Loading schema: %s", ac_schema)
        return load(ac_schema, **options)

    return None


# pylint: disable=redefined-builtin
def open(path, mode=None, ac_parser=None, **options):
    """
    Open given configuration file with appropriate open flag.

    :param path: Configuration file path
    :param mode:
        Can be 'r' and 'rb' for reading (default) or 'w', 'wb' for writing.
        Please note that even if you specify 'r' or 'w', it will be changed to
        'rb' or 'wb' if selected backend, xml and configobj for example, for
        given config file prefer that.
    :param options:
        Optional keyword arguments passed to the internal file opening APIs of
        each backends such like 'buffering' optional parameter passed to
        builtin 'open' function.

    :return: A file object or None on any errors
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    psr = find(path, forced_type=ac_parser)

    if mode is not None and mode.startswith('w'):
        return psr.wopen(path, **options)

    return psr.ropen(path, **options)


def _single_load(input_, ac_parser=None, ac_template=False,
                 ac_context=None, **options):
    """
    :param input_:
        File path or file or file-like object or pathlib.Path object represents
        the file or a namedtuple 'anyconfig.globals.IOInfo' object represents
        some input to load some data from
    :param ac_parser: Forced parser type or parser object itself
    :param ac_template:
        Assume configuration file may be a template file and try to compile it
        AAR if True
    :param ac_context: A dict presents context to instantiate template
    :param options:
        Optional keyword arguments :func:`single_load` supports except for
        ac_schema and ac_query

    :return: Mapping object
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    ioi = anyconfig.ioinfo.make(input_)
    psr = find(ioi, forced_type=ac_parser)
    filepath = ioi.path

    # .. note::
    #    This will be kept for backward compatibility until 'ignore_missing'
    #    option is deprecated and removed completely.
    if "ignore_missing" in options:
        warnings.warn("keyword option 'ignore_missing' is deprecated, use "
                      "'ac_ignore_missing' instead", DeprecationWarning)
        options["ac_ignore_missing"] = options["ignore_missing"]

    LOGGER.info("Loading: %s", filepath)
    if ac_template and filepath is not None:
        content = anyconfig.template.try_render(filepath=filepath,
                                                ctx=ac_context, **options)
        if content is not None:
            return psr.loads(content, **options)

    return psr.load(ioi, **options)


def single_load(input_, ac_parser=None, ac_template=False,
                ac_context=None, **options):
    r"""
    Load single configuration file.

    .. note::

       :func:`load` is a preferable alternative and this API should be used
       only if there is a need to emphasize given input 'input\_' is single
       one.

    :param input\_:
        File path or file or file-like object or pathlib.Path object represents
        the file or a namedtuple 'anyconfig.globals.IOInfo' object represents
        some input to load some data from
    :param ac_parser: Forced parser type or parser object itself
    :param ac_template:
        Assume configuration file may be a template file and try to compile it
        AAR if True
    :param ac_context: A dict presents context to instantiate template
    :param options: Optional keyword arguments such as:

        - Options common in :func:`single_load`, :func:`multi_load`,
          :func:`load` and :func:`loads`:

          - ac_dict: callable (function or class) to make mapping objects from
            loaded data if the selected backend can customize that such as JSON
            which supports that with 'object_pairs_hook' option, or None. If
            this option was not given or None, dict or
            :class:`collections.OrderedDict` will be used to make result as
            mapping object depends on if ac_ordered (see below) is True and
            selected backend can keep the order of items loaded. See also
            :meth:`_container_factory` of
            :class:`anyconfig.backend.base.Parser` for more implementation
            details.

          - ac_ordered: True if you want to keep resuls ordered. Please note
            that order of items may be lost depends on the selected backend.

          - ac_schema: JSON schema file path to validate given config file
          - ac_query: JMESPath expression to query data

        - Common backend options:

          - ac_ignore_missing:
            Ignore and just return empty result if given file 'input\_' does
            not exist actually.

        - Backend specific options such as {"indent": 2} for JSON backend

    :return: Mapping object
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    cnf = _single_load(input_, ac_parser=ac_parser, ac_template=ac_template,
                       ac_context=ac_context, **options)
    schema = _maybe_schema(ac_template=ac_template, ac_context=ac_context,
                           **options)
    cnf = _try_validate(cnf, schema, **options)
    return _try_query(cnf, options.get("ac_query", False), **options)


def multi_load(inputs, ac_parser=None, ac_template=False, ac_context=None,
               **options):
    r"""
    Load multiple config files.

    .. note::

       :func:`load` is a preferable alternative and this API should be used
       only if there is a need to emphasize given inputs are multiple ones.

    The first argument 'inputs' may be a list of a file paths or a glob pattern
    specifying them or a pathlib.Path object represents file[s] or a namedtuple
    'anyconfig.globals.IOInfo' object represents some inputs to load some data
    from.

    About glob patterns, for example, is, if a.yml, b.yml and c.yml are in the
    dir /etc/foo/conf.d/, the followings give same results::

      multi_load(["/etc/foo/conf.d/a.yml", "/etc/foo/conf.d/b.yml",
                  "/etc/foo/conf.d/c.yml", ])

      multi_load("/etc/foo/conf.d/*.yml")

    :param inputs:
        A list of file path or a glob pattern such as r'/a/b/\*.json'to list of
        files, file or file-like object or pathlib.Path object represents the
        file or a namedtuple 'anyconfig.globals.IOInfo' object represents some
        inputs to load some data from
    :param ac_parser: Forced parser type or parser object
    :param ac_template: Assume configuration file may be a template file and
        try to compile it AAR if True
    :param ac_context: Mapping object presents context to instantiate template
    :param options: Optional keyword arguments:

        - ac_dict, ac_ordered, ac_schema and ac_query are the options common in
          :func:`single_load`, :func:`multi_load`, :func:`load`: and
          :func:`loads`. See the descriptions of them in :func:`single_load`.

        - Options specific to this function and :func:`load`:

          - ac_merge (merge): Specify strategy of how to merge results loaded
            from multiple configuration files. See the doc of
            :mod:`anyconfig.dicts` for more details of strategies. The default
            is anyconfig.dicts.MS_DICTS.

          - ac_marker (marker): Globbing marker to detect paths patterns.

        - Common backend options:

          - ignore_missing: Ignore and just return empty result if given file
            'path' does not exist.

        - Backend specific options such as {"indent": 2} for JSON backend

    :return: Mapping object or any query result might be primitive objects
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    marker = options.setdefault("ac_marker", options.get("marker", '*'))
    schema = _maybe_schema(ac_template=ac_template, ac_context=ac_context,
                           **options)
    options["ac_schema"] = None  # Avoid to load schema more than twice.

    paths = anyconfig.utils.expand_paths(inputs, marker=marker)
    if anyconfig.utils.are_same_file_types(paths):
        ac_parser = find(paths[0], forced_type=ac_parser)

    cnf = ac_context
    for path in paths:
        opts = options.copy()
        cups = _single_load(path, ac_parser=ac_parser,
                            ac_template=ac_template, ac_context=cnf, **opts)
        if cups:
            if cnf is None:
                cnf = cups
            else:
                merge(cnf, cups, **options)

    if cnf is None:
        return anyconfig.dicts.convert_to({}, **options)

    cnf = _try_validate(cnf, schema, **options)
    return _try_query(cnf, options.get("ac_query", False), **options)


def load(path_specs, ac_parser=None, ac_dict=None, ac_template=False,
         ac_context=None, **options):
    r"""
    Load single or multiple config files or multiple config files specified in
    given paths pattern or pathlib.Path object represents config files or a
    namedtuple 'anyconfig.globals.IOInfo' object represents some inputs.

    :param path_specs:
        A list of file path or a glob pattern such as r'/a/b/\*.json'to list of
        files, file or file-like object or pathlib.Path object represents the
        file or a namedtuple 'anyconfig.globals.IOInfo' object represents some
        inputs to load some data from.
    :param ac_parser: Forced parser type or parser object
    :param ac_dict:
        callable (function or class) to make mapping object will be returned as
        a result or None. If not given or ac_dict is None, default mapping
        object used to store resutls is dict or
        :class:`collections.OrderedDict` if ac_ordered is True and selected
        backend can keep the order of items in mapping objects.

    :param ac_template: Assume configuration file may be a template file and
        try to compile it AAR if True
    :param ac_context: A dict presents context to instantiate template
    :param options:
        Optional keyword arguments. See also the description of 'options' in
        :func:`single_load` and :func:`multi_load`

    :return: Mapping object or any query result might be primitive objects
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    marker = options.setdefault("ac_marker", options.get("marker", '*'))

    if anyconfig.utils.is_path_like_object(path_specs, marker):
        return single_load(path_specs, ac_parser=ac_parser, ac_dict=ac_dict,
                           ac_template=ac_template, ac_context=ac_context,
                           **options)

    if not anyconfig.utils.is_paths(path_specs, marker):
        raise ValueError("Possible invalid input %r" % path_specs)

    return multi_load(path_specs, ac_parser=ac_parser, ac_dict=ac_dict,
                      ac_template=ac_template, ac_context=ac_context,
                      **options)


def loads(content, ac_parser=None, ac_dict=None, ac_template=False,
          ac_context=None, **options):
    """
    :param content: Configuration file's content (a string)
    :param ac_parser: Forced parser type or ID or parser object
    :param ac_dict:
        callable (function or class) to make mapping object will be returned as
        a result or None. If not given or ac_dict is None, default mapping
        object used to store resutls is dict or
        :class:`collections.OrderedDict` if ac_ordered is True and selected
        backend can keep the order of items in mapping objects.
    :param ac_template: Assume configuration file may be a template file and
        try to compile it AAR if True
    :param ac_context: Context dict to instantiate template
    :param options:
        Optional keyword arguments. See also the description of 'options' in
        :func:`single_load` function.

    :return: Mapping object or any query result might be primitive objects
    :raises: ValueError, UnknownProcessorTypeError
    """
    if ac_parser is None:
        LOGGER.warning("ac_parser was not given but it's must to find correct "
                       "parser to load configurations from string.")
        return None

    psr = find(None, forced_type=ac_parser)
    schema = None
    ac_schema = options.get("ac_schema", None)
    if ac_schema is not None:
        options["ac_schema"] = None
        schema = loads(ac_schema, ac_parser=psr, ac_dict=ac_dict,
                       ac_template=ac_template, ac_context=ac_context,
                       **options)

    if ac_template:
        compiled = anyconfig.template.try_render(content=content,
                                                 ctx=ac_context, **options)
        if compiled is not None:
            content = compiled

    cnf = psr.loads(content, ac_dict=ac_dict, **options)
    cnf = _try_validate(cnf, schema, **options)
    return _try_query(cnf, options.get("ac_query", False), **options)


def dump(data, out, ac_parser=None, **options):
    """
    Save 'data' to 'out'.

    :param data: A mapping object may have configurations data to dump
    :param out:
        An output file path, a file, a file-like object, :class:`pathlib.Path`
        object represents the file or a namedtuple 'anyconfig.globals.IOInfo'
        object represents output to dump some data to.
    :param ac_parser: Forced parser type or parser object
    :param options:
        Backend specific optional arguments, e.g. {"indent": 2} for JSON
        loader/dumper backend

    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    ioi = anyconfig.ioinfo.make(out)
    psr = find(ioi, forced_type=ac_parser)
    LOGGER.info("Dumping: %s", ioi.path)
    psr.dump(data, ioi, **options)


def dumps(data, ac_parser=None, **options):
    """
    Return string representation of 'data' in forced type format.

    :param data: Config data object to dump
    :param ac_parser: Forced parser type or ID or parser object
    :param options: see :func:`dump`

    :return: Backend-specific string representation for the given data
    :raises: ValueError, UnknownProcessorTypeError
    """
    psr = find(None, forced_type=ac_parser)
    return psr.dumps(data, **options)

# vim:sw=4:ts=4:et:
