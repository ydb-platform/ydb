#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
r"""YAML backend by ruamel.yaml:

- Format to support: YAML, http://yaml.org
- Requirement: ruamel.yaml, https://bitbucket.org/ruamel/yaml
- Development Status :: 4 - Beta
- Limitations:

  - Multi-documents YAML stream load and dump are not supported.

- Special options:

  - All keyword options of yaml.safe_load, yaml.load, yaml.safe_dump and
    yaml.dump should work.

  - Use 'ac_safe' boolean keyword option if you prefer to call yaml.safe_load
    and yaml.safe_dump instead of yaml.load and yaml.dump. Please note that
    this option conflicts with 'ac_dict' option and these options cannot be
    used at the same time.

  - Also, you can give keyword options for ruamel.yaml.YAML.__init__ such like
    typ and pure, and can give some members of ruamel.yaml.YAML instance to
    control the behaviors such like default_flow_style and allow_duplicate_keys
    as keyword options to load and dump functions.

  - See also: https://yaml.readthedocs.io

Changelog:

.. versionchanged:: 0.9.8

   - Split from the common yaml backend and start to support ruamel.yaml
     specific features.
"""
from __future__ import absolute_import

import ruamel.yaml as ryaml
import anyconfig.backend.base
import anyconfig.utils

from . import common


try:
    ryaml.YAML  # flake8: noqa
except AttributeError:
    raise ImportError("ruamel.yaml may be too old to use!")

_YAML_INIT_KWARGS = ["typ", "pure", "plug_ins"]  # kwargs for ruamel.yaml.YAML
_YAML_INSTANCE_MEMBERS = ['allow_duplicate_keys', 'allow_unicode',
                          'block_seq_indent', 'canonical', 'composer',
                          'constructor', 'default_flow_style', 'default_style',
                          'dump', 'dump_all', 'emitter', 'encoding',
                          'explicit_end', 'explicit_start',
                          'get_constructor_parser',
                          'get_serializer_representer_emitter', 'indent',
                          'line_break', 'load', 'load_all', 'map',
                          'map_indent', 'official_plug_ins', 'old_indent',
                          'parser', 'prefix_colon', 'preserve_quotes',
                          'reader', 'register_class', 'representer',
                          'resolver', 'scanner', 'seq', 'sequence_dash_offset',
                          'sequence_indent', 'serializer', 'stream', 'tags',
                          'top_level_block_style_scalar_no_indent_error_1_1',
                          'top_level_colon_align', 'version', 'width']

_YAML_OPTS = _YAML_INIT_KWARGS + _YAML_INSTANCE_MEMBERS


def yml_fnc(fname, *args, **options):
    """
    :param fname:
        "load" or "dump", not checked but it should be OK.
        see also :func:`yml_load` and :func:`yml_dump`
    :param args: [stream] for load or [cnf, stream] for dump
    :param options: keyword args may contain "ac_safe" to load/dump safely
    """
    options = common.filter_from_options("ac_dict", options)

    if "ac_safe" in options:
        options["typ"] = "safe"  # Override it.

    iopts = anyconfig.utils.filter_options(_YAML_INIT_KWARGS, options)
    oopts = anyconfig.utils.filter_options(_YAML_INSTANCE_MEMBERS, options)

    yml = ryaml.YAML(**iopts)
    for attr, val in oopts.items():
        setattr(yml, attr, val)  # e.g. yml.preserve_quotes = True

    return getattr(yml, fname)(*args)


def yml_load(stream, container, **options):
    """.. seealso:: :func:`anyconfig.backend.yaml.pyyaml.yml_load`
    """
    ret = yml_fnc("load", stream, **options)
    if ret is None:
        return container()

    return ret


def yml_dump(data, stream, **options):
    """.. seealso:: :func:`anyconfig.backend.yaml.pyyaml.yml_dump`
    """
    # .. todo:: Needed?
    # if anyconfig.utils.is_dict_like(data):
    #     if options.get("ac_ordered"):
    #         factory = collections.OrderedDict
    #     else:
    #         factory = dict
    #     data = anyconfig.dicts.convert_to(data, ac_dict=factory)
    return yml_fnc("dump", data, stream, **options)


class Parser(common.Parser):
    """Parser for YAML files.
    """
    _cid = "ruamel.yaml"
    _load_opts = _YAML_OPTS
    _dump_opts = _YAML_OPTS

    load_from_stream = anyconfig.backend.base.to_method(yml_load)
    dump_to_stream = anyconfig.backend.base.to_method(yml_dump)

# vim:sw=4:ts=4:et:
