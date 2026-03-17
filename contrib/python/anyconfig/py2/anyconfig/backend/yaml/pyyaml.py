#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# type() is used to exactly match check instead of isinstance here.
# pylint: disable=unidiomatic-typecheck
r"""YAML backend:

- Format to support: YAML, http://yaml.org
- Requirements: PyYAML (yaml), http://pyyaml.org
- Development Status :: 5 - Production/Stable
- Limitations:

  - Resuls is not ordered even if 'ac_ordered' or 'ac_dict' was given.

- Special options:

  - All keyword options of yaml.safe_load, yaml.load, yaml.safe_dump and
    yaml.dump should work.

  - Use 'ac_safe' boolean keyword option if you prefer to call yaml.safe_load
    and yaml.safe_dump instead of yaml.load and yaml.dump. Please note that
    this option conflicts with 'ac_dict' option and these options cannot be
    used at the same time.

  - See also: http://pyyaml.org/wiki/PyYAMLDocumentation

Changelog:

.. versionchanged:: 0.9.6

   - Add support of loading primitives other than mapping objects.

.. versionchanged:: 0.3

   - Changed special keyword option 'ac_safe' from 'safe' to avoid
     possibility of option conflicts in the future.
"""
from __future__ import absolute_import

import yaml
try:
    from yaml import CSafeLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import SafeLoader as Loader, Dumper

import anyconfig.backend.base
import anyconfig.compat
import anyconfig.utils

from . import common


_MAPPING_TAG = yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG


def _customized_loader(container, loader=Loader, mapping_tag=_MAPPING_TAG):
    """
    Create or update loader with making given callble 'container' to make
    mapping objects such as dict and OrderedDict, used to construct python
    object from yaml mapping node internally.

    :param container: Set container used internally
    """
    def construct_mapping(loader, node, deep=False):
        """Construct python object from yaml mapping node, based on
        :meth:`yaml.BaseConstructor.construct_mapping` in PyYAML (MIT).
        """
        loader.flatten_mapping(node)
        if not isinstance(node, yaml.MappingNode):
            msg = "expected a mapping node, but found %s" % node.id
            raise yaml.constructor.ConstructorError(None, None, msg,
                                                    node.start_mark)
        mapping = container()
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as exc:
                eargs = ("while constructing a mapping",
                         node.start_mark,
                         "found unacceptable key (%s)" % exc,
                         key_node.start_mark)
                raise yaml.constructor.ConstructorError(*eargs)
            value = loader.construct_object(value_node, deep=deep)
            mapping[key] = value

        return mapping

    tag = "tag:yaml.org,2002:python/unicode"

    def construct_ustr(loader, node):
        """Unicode string constructor"""
        return loader.construct_scalar(node)

    try:
        loader.add_constructor(tag, construct_ustr)
    except NameError:
        pass

    if type(container) != dict:
        loader.add_constructor(mapping_tag, construct_mapping)
    return loader


def _customized_dumper(container, dumper=Dumper):
    """
    Coutnerpart of :func:`_customized_loader` for dumpers.
    """
    def container_representer(dumper, data, mapping_tag=_MAPPING_TAG):
        """Container representer.
        """
        return dumper.represent_mapping(mapping_tag, data.items())

    def ustr_representer(dumper, data):
        """Unicode string representer"""
        tag = "tag:yaml.org,2002:python/unicode"
        return dumper.represent_scalar(tag, data)

    try:
        dumper.add_representer(unicode, ustr_representer)
    except NameError:
        pass

    if type(container) != dict:
        dumper.add_representer(container, container_representer)
    return dumper


def yml_fnc(fname, *args, **options):
    """An wrapper of yaml.safe_load, yaml.load, yaml.safe_dump and yaml.dump.

    :param fname:
        "load" or "dump", not checked but it should be OK.
        see also :func:`yml_load` and :func:`yml_dump`
    :param args: [stream] for load or [cnf, stream] for dump
    :param options: keyword args may contain "ac_safe" to load/dump safely
    """
    key = "ac_safe"
    fnc = getattr(yaml, r"safe_" + fname if options.get(key) else fname)
    return fnc(*args, **common.filter_from_options(key, options))


def yml_load(stream, container, yml_fnc=yml_fnc, **options):
    """An wrapper of yaml.safe_load and yaml.load.

    :param stream: a file or file-like object to load YAML content
    :param container: callble to make a container object

    :return: Mapping object
    """
    if options.get("ac_safe", False):
        options = {}  # yaml.safe_load does not process Loader opts.
    elif not options.get("Loader"):
        maybe_container = options.get("ac_dict", False)
        if maybe_container and callable(maybe_container):
            container = maybe_container

        options["Loader"] = _customized_loader(container)

    ret = yml_fnc("load", stream,
                  **common.filter_from_options("ac_dict", options))
    if ret is None:
        return container()

    return ret


def yml_dump(data, stream, yml_fnc=yml_fnc, **options):
    """An wrapper of yaml.safe_dump and yaml.dump.

    :param data: Some data to dump
    :param stream: a file or file-like object to dump YAML data
    """
    _is_dict = anyconfig.utils.is_dict_like(data)

    if options.get("ac_safe", False):
        options = {}
    elif not options.get("Dumper", False) and _is_dict:
        # TODO: Any other way to get its constructor?
        maybe_container = options.get("ac_dict", type(data))
        options["Dumper"] = _customized_dumper(maybe_container)

    if _is_dict:
        # Type information and the order of items are lost on dump currently.
        data = anyconfig.dicts.convert_to(data, ac_dict=dict)
        options = common.filter_from_options("ac_dict", options)

    return yml_fnc("dump", data, stream, **options)


class Parser(common.Parser):
    """
    Parser for YAML files.
    """
    _cid = "pyyaml"
    _priority = 30  # Higher priority than ruamel.yaml.
    _load_opts = ["Loader", "ac_safe", "ac_dict"]
    _dump_opts = ["stream", "ac_safe", "Dumper", "default_style",
                  "default_flow_style", "canonical", "indent", "width",
                  "allow_unicode", "line_break", "encoding", "explicit_start",
                  "explicit_end", "version", "tags"]

    load_from_stream = anyconfig.backend.base.to_method(yml_load)
    dump_to_stream = anyconfig.backend.base.to_method(yml_dump)

# vim:sw=4:ts=4:et:
