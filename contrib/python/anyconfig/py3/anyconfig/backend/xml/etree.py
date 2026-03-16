#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# Some XML modules may be missing and Base.{load,dumps}_impl are not
# overridden:
# pylint: disable=import-error, duplicate-except
# len(elem) is necessary to check that ElementTree.Element object has children.
# pylint: disable=len-as-condition
r"""A backend module to load and dump XML files.

- Format to support: XML, e.g. http://www.w3.org/TR/xml11/
- Requirements: one of the followings

  - xml.etree.cElementTree in standard lib if python >= 2.5
  - xml.etree.ElementTree in standard lib if python >= 2.5
  - elementtree.ElementTree (otherwise)

- Development Status :: 4 - Beta
- Limitations:

  - special node '@attrs', '@text' and '@children' are used to keep XML
    structure of original data. You have to cusomize them with 'tags' keyword
    option to avoid any config parameters conflict with some of them.

  - Some data or structures of original XML file may be lost if make it backed
    to XML file; XML file - (anyconfig.load) -> config - (anyconfig.dump) ->
    XML file

  - XML specific features (namespace, etc.) may not be processed correctly.

- Special Options:

  - ac_parse_value: Try to parse values, elements' text and attributes.

  - merge_attrs: Merge attributes and mix with children nodes. Please note that
    information of attributes are lost after load if this option is used.

  - tags: A dict provide special parameter names to distinguish between
    attributes, text and children nodes. Default is {"attrs": "@attrs", "text":
    "@text", "children": "@children"}.

Changelog:

.. versionchanged:: 0.8.2

   - Add special options, tags, merge_attrs and ac_parse_value
   - Remove special option, pprefix which conflicts with another option tags

.. versionchanged:: 0.8.0

   - Try to make a nested dict w/o extra dict having keys of attrs, text and
     children from XML string/file as much as possible.
   - Support namespaces partially.

.. versionchanged:: 0.1.0

   - Added XML dump support.
"""
from __future__ import annotations

import io
import itertools
import operator
import re
import typing

from xml.etree import ElementTree

from .. import base
from ... import dicts
from ...parser import parse_single
from ...utils import (
    filter_options, get_path_from_stream,
    is_dict_like, is_iterable, noop,
)

if typing.TYPE_CHECKING:
    import collections.abc


_TAGS = {"attrs": "@attrs", "text": "@text", "children": "@children"}
_ET_NS_RE = re.compile(r"^{(\S+)}(\S+)$")

_ENCODING: str = "utf-8"


if typing.TYPE_CHECKING:
    DicType = dict[str, typing.Any]
    DicsType = collections.abc.Iterable[DicType]
    GenDicType = collections.abc.Callable[..., DicType]


def _namespaces_from_file(
    xmlfile: base.PathOrStrT | typing.IO,
) -> dict[str, tuple[str, str]]:
    """Get the namespace str from file.

    :param xmlfile: XML file or file-like object
    :return: {namespace_uri: namespace_prefix} or {}
    """
    return {
        typing.cast("str", url): typing.cast("tuple[str, str]", prefix)
        for _, (prefix, url)
        in ElementTree.iterparse(xmlfile, events=("start-ns", ))
    }


def _tweak_ns(tag: str, **options: dict[str, str]) -> str:
    """Tweak the namespace.

    :param tag: XML tag element
    :param nspaces: A namespaces dict, {uri: prefix}
    :param options: Extra keyword options may contain 'nspaces' keyword option
        provide a namespace dict, {uri: prefix}

    >>> _tweak_ns("a", nspaces={})
    'a'
    >>> _tweak_ns("a", nspaces={"http://example.com/ns/val/": "val"})
    'a'
    >>> _tweak_ns("{http://example.com/ns/val/}a",
    ...           nspaces={"http://example.com/ns/val/": "val"})
    'val:a'
    """
    nspaces = options.get("nspaces")
    if nspaces is not None:
        matched = _ET_NS_RE.match(tag)
        if matched:
            (uri, tag) = matched.groups()
            prefix = nspaces.get(uri, False)
            if prefix:
                return f"{prefix}:{tag}"

    return tag


def _dicts_have_unique_keys(dics: DicsType) -> bool:
    """Test if given dicts don't have same keys.

    :param dics: [<dict or dict-like object>], must not be [] or [{...}]
    :return: True if all keys of each dict of 'dics' are unique

    # Enable the followings if to allow dics is [], [{...}]:
    # >>> all(_dicts_have_unique_keys([d]) for [d]
    # ...     in ({}, {'a': 0}, {'a': 1, 'b': 0}))
    # True
    # >>> _dicts_have_unique_keys([{}, {'a': 1}, {'b': 2, 'c': 0}])
    # True

    >>> _dicts_have_unique_keys([{}, {"a": 1}, {"a": 2}])
    False
    >>> _dicts_have_unique_keys([{}, {"a": 1}, {"b": 2}, {"b": 3, "c": 0}])
    False
    >>> _dicts_have_unique_keys([{}, {}])
    True
    """
    key_itr = itertools.chain.from_iterable(d.keys() for d in dics)
    return len(set(key_itr)) == sum(len(d) for d in dics)


def _parse_text(val: str, **options: typing.Any) -> typing.Any:
    """Parse ``val`` and interpret its data to some value.

    :return: Parsed value or value itself depends on 'ac_parse_value'
    """
    if val and options.get("ac_parse_value", False):
        return parse_single(val)

    return val


def _process_elem_text(
    elem: ElementTree.Element, dic: DicType, subdic: DicType,
    text: str = "@text", **options: typing.Any,
) -> None:
    """Process the text in the element ``elem``.

    :param elem: ElementTree.Element object which has elem.text
    :param dic: <container> (dict[-like]) object converted from elem
    :param subdic: Sub <container> object converted from elem
    :param options:
        Keyword options, see the description of :func:`elem_to_container` for
        more details.

    :return: None but updating elem.text, dic and subdic as side effects
    """
    if elem.text:
        elem.text = elem.text.strip()

    if elem.text:
        etext = _parse_text(elem.text, **options)
        if len(elem) or elem.attrib:
            subdic[text] = etext
        else:
            dic[elem.tag] = etext  # Only text, e.g. <a>text</a>


def _parse_attrs(
    elem: ElementTree.Element, container: GenDicType = dict,
    **options: typing.Any,
) -> DicType:
    """Parse the attributes of the element ``elem``.

    :param elem: ElementTree.Element object has attributes (elem.attrib)
    :param container: callble to make a container object
    :return: Parsed value or value itself depends on 'ac_parse_value'
    """
    adic = {_tweak_ns(a, **options): v for a, v in elem.attrib.items()}
    if options.get("ac_parse_value", False):
        return container({k: parse_single(v) for k, v in adic.items()})

    return container(adic)


def _process_elem_attrs(
    elem: ElementTree.Element, dic: DicType, subdic: DicType,
    container: GenDicType = dict, attrs: str = "@attrs",
    **options: typing.Any,
) -> None:
    """Process attributes in the element ``elem``.

    :param elem: ElementTree.Element object or None
    :param dic: <container> (dict[-like]) object converted from elem
    :param subdic: Sub <container> object converted from elem
    :param options:
        Keyword options, see the description of :func:`elem_to_container` for
        more details.

    :return: None but updating dic and subdic as side effects
    """
    adic = _parse_attrs(elem, container=container, **options)
    if not elem.text and not len(elem) and options.get("merge_attrs"):
        dic[elem.tag] = adic
    else:
        subdic[attrs] = adic


def _process_children_elems(
    elem: ElementTree.Element, dic: DicType, subdic: DicType,
    container: GenDicType = dict, children: str = "@children",
    **options: typing.Any,
) -> None:
    """Process children of the element ``elem``.

    :param elem: ElementTree.Element object or None
    :param dic: <container> (dict[-like]) object converted from elem
    :param subdic: Sub <container> object converted from elem
    :param container: callble to make a container object
    :param children: Tag for children nodes
    :param options:
        Keyword options, see the description of :func:`elem_to_container` for
        more details.

    :return: None but updating dic and subdic as side effects
    """
    cdics = [elem_to_container(c, container=container, **options)
             for c in elem]
    merge_attrs = options.get("merge_attrs", False)
    if merge_attrs or subdic:
        sdics = [container(elem.attrib) if merge_attrs else subdic, *cdics]
    else:
        sdics = cdics

    if _dicts_have_unique_keys(sdics):  # ex. <a><b>1</b><c>c</c></a>
        (sdic, udicts) = (sdics[0], sdics[1:])
        for udic in udicts:
            dicts.merge(sdic, udic, **options)

        dic[elem.tag] = dicts.convert_to(sdic, ac_dict=container)

    elif not subdic:  # There are no attrs nor text and only these children.
        dic[elem.tag] = cdics
    else:
        subdic[children] = cdics


def elem_to_container(
    elem: ElementTree.Element | None,
    container: GenDicType = dict,
    **options: typing.Any,
) -> DicType:
    """Convert XML ElementTree Element to a collection of container objects.

    Elements are transformed to a node under special tagged nodes, attrs, text
    and children, to store the type of these elements basically, however, in
    some special cases like the followings, these nodes are attached to the
    parent node directly for later convenience.

    - There is only text element
    - There are only children elements each has unique keys among all

    :param elem: ElementTree.Element object or None
    :param container: callble to make a container object
    :param options: Keyword options

        - nspaces: A namespaces dict, {uri: prefix} or None
        - attrs, text, children: Tags for special nodes to keep XML info
        - merge_attrs: Merge attributes and mix with children nodes, and the
          information of attributes are lost after its transformation.
    """
    dic = container()
    if elem is None:
        return dic

    elem.tag = _tweak_ns(elem.tag, **options)  # {ns}tag -> ns_prefix:tag
    subdic = dic[elem.tag] = container()
    options["container"] = container

    if elem.text:
        _process_elem_text(elem, dic, subdic, **options)

    if elem.attrib:
        _process_elem_attrs(elem, dic, subdic, **options)

    if len(elem):
        _process_children_elems(elem, dic, subdic, **options)
    elif not elem.text and not elem.attrib:  # ex. <tag/>.
        dic[elem.tag] = None

    return dic


def _complement_tag_options(options: DicType) -> DicType:
    """Complement tag options.

    :param options: Keyword options :: dict

    >>> ref = _TAGS.copy()
    >>> ref["text"] = "#text"
    >>> opts = _complement_tag_options({"tags": {"text": ref["text"]}})
    >>> del opts["tags"]  # To simplify comparison.
    >>> sorted(opts.items())
    [('attrs', '@attrs'), ('children', '@children'), ('text', '#text')]
    """
    if not all(nt in options for nt in _TAGS):
        tags = options.get("tags", {})
        for ntype, tag in _TAGS.items():
            options[ntype] = tags.get(ntype, tag)

    return options


def root_to_container(
    root: ElementTree.Element,
    container: GenDicType = dict,
    nspaces: DicType | None = None,
    **options: typing.Any,
) -> DicType:
    """Convert XML ElementTree Root Element to container objects.

    :param root: etree root object or None
    :param container: callble to make a container object
    :param nspaces: A namespaces dict, {uri: prefix} or None
    :param options: Keyword options,

        - tags: Dict of tags for special nodes to keep XML info, attributes,
          text and children nodes, e.g. {"attrs": "@attrs", "text": "#text"}
    """
    tree = container()
    if root is None:
        return tree

    if nspaces is not None:
        for uri, prefix in nspaces.items():
            root.attrib[f"xmlns:{prefix}" if prefix else "xmlns"] = uri

    return elem_to_container(root, container=container, nspaces=nspaces,
                             **_complement_tag_options(options))


def _to_str_fn(
    **options: typing.Any,
) -> collections.abc.Callable[..., str]:
    """Convert any objects to a str.

    :param options: Keyword options might have 'ac_parse_value' key
    :param to_str: Callable to convert value to string
    """
    return (
        str if options.get("ac_parse_value")  # type: ignore[return-value]
        else noop
    )


def _elem_set_attrs(
    obj: DicType, parent: ElementTree.Element,
    to_str: collections.abc.Callable[..., str],
) -> None:
    """Set attributes of the element ``parent``.

    :param obj: Container instance gives attributes of XML Element
    :param parent: XML ElementTree parent node object
    :param to_str: Callable to convert value to string or None
    :param options: Keyword options, see :func:`container_to_elem`

    :return: None but parent will be modified
    """
    for attr, val in obj.items():
        parent.set(attr, to_str(val))


def _elem_from_descendants(
    children_nodes: collections.abc.Iterable[DicType],
    **options: typing.Any,
) -> collections.abc.Iterator[ElementTree.Element]:
    """Get the elements from the descendants ``children_nodes``.

    :param children_nodes: A list of child dict objects
    :param options: Keyword options, see :func:`container_to_elem`
    """
    for child in children_nodes:  # child should be a dict-like object.
        for ckey, cval in child.items():
            celem = ElementTree.Element(ckey)
            container_to_elem(cval, parent=celem, **options)
            yield celem


def _get_or_update_parent(
    key: str, val: typing.Any,
    to_str: collections.abc.Callable[..., str],
    parent: ElementTree.Element | None = None,
    **options: typing.Any,
) -> ElementTree.Element:
    """Get or update the parent element ``parent``.

    :param key: Key of current child (dict{,-like} object)
    :param val: Value of current child (dict{,-like} object or [dict{,...}])
    :param to_str: Callable to convert value to string
    :param parent: XML ElementTree parent node object or None
    :param options: Keyword options, see :func:`container_to_elem`
    """
    elem = ElementTree.Element(key)

    vals = val if is_iterable(val) else [val]
    for val_ in vals:
        container_to_elem(val_, parent=elem, to_str=to_str, **options)

    if parent is None:  # 'elem' is the top level etree.
        return elem

    parent.append(elem)
    return parent


_ATC = ("attrs", "text", "children")


def _assert_if_invalid_node(
    obj: typing.Any,
    parent: ElementTree.Element | None = None,
) -> None:
    """Make sure the ``obj`` or ``parent`` is not invalid."""
    if parent is None and (obj is None or not obj):
        raise ValueError

    if parent is not None and not isinstance(parent, ElementTree.Element):
        raise ValueError


def container_to_elem(
    obj: typing.Any,
    parent: ElementTree.Element | None = None,
    to_str: collections.abc.Callable[..., str] | None = None,
    **options: typing.Any,
) -> ElementTree.Element:
    """Convert a dict-like object to XML ElementTree.

    :param obj: Container instance to convert to
    :param parent: XML ElementTree parent node object or None
    :param to_str: Callable to convert value to string or None
    :param options: Keyword options,

        - tags: Dict of tags for special nodes to keep XML info, attributes,
          text and children nodes, e.g. {"attrs": "@attrs", "text": "#text"}
    """
    _assert_if_invalid_node(obj, parent=parent)

    if to_str is None:
        to_str = _to_str_fn(**options)

    if not is_dict_like(obj):
        if parent is not None and obj:
            parent.text = to_str(obj)  # Parent is a leaf text node.
        # All attributes and text should be set already.
        return parent  # type: ignore[return-value]

    options = _complement_tag_options(options)
    (attrs, text, children) = operator.itemgetter(*_ATC)(options)

    for key, val in obj.items():
        if parent is None:
            parent = _get_or_update_parent(
                key, val, to_str, parent=parent, **options,
            )
            continue

        if key == attrs:
            _elem_set_attrs(val, parent, to_str)
        elif key == text:
            parent.text = to_str(val)
        elif key == children:
            for celem in _elem_from_descendants(val, **options):
                parent.append(celem)
        else:
            parent = _get_or_update_parent(
                key, val, to_str, parent=parent, **options,
            )

    return parent  # type: ignore[return-value]


def etree_write(
    elem: ElementTree.Element, stream: typing.IO,
    **options: typing.Any,
) -> None:
    """Write XML ElementTree 'root' content into 'stream'.

    :param tree: XML ElementTree object
    :param stream: File or file-like object can write to
    """
    opts = {"xml_declaration": True, "encoding": "unicode"}
    opts.update(
        **filter_options(
            ("method", "xml_declaration", "default_namespace",
             "short_empty_elements"),
            options,
        ),
    )
    content: bytes = ElementTree.tostring(  # type: ignore[call-overload]
        elem, **opts,
    ).encode("utf-8")
    stream.write(content)


class Parser(base.Parser, base.ToStreamDumperMixin):
    """Parser for XML files."""

    _cid: typing.ClassVar[str] = "xml.etree"
    _type: typing.ClassVar[str] = "xml"
    _extensions: tuple[str, ...] = ("xml", )
    _load_opts: tuple[str, ...] = (
        "tags", "merge_attrs", "ac_parse_value",
    )
    # .. seealso:: xml.etree.ElementTree.tostring
    _dump_opts = (
        *_load_opts,
        "encoding", "method", "xml_declaration", "default_namespace",
        "short_empty_elements",
    )

    _ordered: typing.ClassVar[bool] = True
    _dict_opts: tuple[str, ...] = ("ac_dict", )
    _open_read_mode: typing.ClassVar[str] = "rb"
    _open_write_mode: typing.ClassVar[str] = "wb"

    def load_from_string(
        self, content: str | bytes, container: GenDicType,
        **opts: typing.Any,
    ) -> DicType:
        """Load config from XML snippet (a string 'content').

        :param content:
            XML snippet string of str (python 2) or bytes (python 3) type
        :param container: callble to make a container object
        :param opts: optional keyword parameters passed to

        :return: Dict-like object holding config parameters
        """
        if isinstance(content, str):
            elem = ElementTree.fromstring(content)
            stream = io.BytesIO(content.encode(_ENCODING))
        else:
            elem = ElementTree.fromstring(content.decode(_ENCODING))
            stream = io.BytesIO(content)

        nspaces = _namespaces_from_file(stream)
        return root_to_container(
            elem, container=container, nspaces=nspaces, **opts,
        )

    def load_from_path(
        self, filepath: base.PathOrStrT, container: GenDicType,
        **opts: typing.Any,
    ) -> DicType:
        """Load data from path ``filepath``.

        :param filepath: XML file path
        :param container: callble to make a container object
        :param opts: optional keyword parameters to be sanitized

        :return: Dict-like object holding config parameters
        """
        elem = ElementTree.parse(filepath).getroot()
        nspaces = _namespaces_from_file(filepath)
        return root_to_container(
            elem, container=container, nspaces=nspaces, **opts,
        )

    def load_from_stream(
        self, stream: typing.IO, container: GenDicType,
        **opts: typing.Any,
    ) -> DicType:
        """Load data from IO stream ``stream``.

        :param stream: XML file or file-like object
        :param container: callble to make a container object
        :param opts: optional keyword parameters to be sanitized

        :return: Dict-like object holding config parameters
        """
        elem = ElementTree.parse(stream).getroot()
        path = get_path_from_stream(stream)
        nspaces = _namespaces_from_file(path)
        return root_to_container(
            elem, container=container, nspaces=nspaces, **opts,
        )

    def dump_to_string(  # type: ignore[override]
        self, cnf: base.InDataExT, **opts: typing.Any,
    ) -> bytes:
        """Dump data ``cnf`` as a str.

        :param cnf: Configuration data to dump
        :param opts: optional keyword parameters

        :return: string represents the configuration
        """
        if cnf is None or not cnf or not is_dict_like(cnf):
            return b""

        elem = container_to_elem(cnf, **opts)
        bio = io.BytesIO()
        etree_write(elem, bio, **opts)
        return bio.getvalue()

    def dump_to_stream(
        self, cnf: base.InDataExT, stream: typing.IO,
        **opts: typing.Any,
    ) -> None:
        """Dump data ``cnf`` to the IO stream ``stream``.

        :param cnf: Configuration data to dump
        :param stream: Config file or file like object write to
        :param opts: optional keyword parameters
        """
        if cnf is None or not cnf or not is_dict_like(cnf):
            return

        elem = container_to_elem(cnf, **opts)
        etree_write(elem, stream, **opts)
