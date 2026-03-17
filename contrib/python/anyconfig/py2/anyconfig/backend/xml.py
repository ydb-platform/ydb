#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# Some XML modules may be missing and Base.{load,dumps}_impl are not overriden:
# pylint: disable=import-error, duplicate-except
# len(elem) is necessary to check that ET.Element object has children.
# pylint: disable=len-as-condition
r"""XML backend:

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
from __future__ import absolute_import
from io import BytesIO

import operator
import re
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
except ImportError:
    import elementtree.ElementTree as ET

import anyconfig.backend.base
import anyconfig.compat
import anyconfig.utils
import anyconfig.parser


_TAGS = dict(attrs="@attrs", text="@text", children="@children")
_ET_NS_RE = re.compile(r"^{(\S+)}(\S+)$")


def _iterparse(xmlfile):
    """
    Avoid bug in python 3.{2,3}. See http://bugs.python.org/issue9257.

    :param xmlfile: XML file or file-like object
    """
    try:
        return ET.iterparse(xmlfile, events=("start-ns", ))
    except TypeError:
        return ET.iterparse(xmlfile, events=(b"start-ns", ))


def flip(tpl):
    """
    >>> flip((1, 2))
    (2, 1)
    """
    return (tpl[1], tpl[0])


def _namespaces_from_file(xmlfile):
    """
    :param xmlfile: XML file or file-like object
    :return: {namespace_uri: namespace_prefix} or {}
    """
    return dict(flip(t) for _, t in _iterparse(xmlfile))


def _tweak_ns(tag, **options):
    """
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
    nspaces = options.get("nspaces", None)
    if nspaces is not None:
        matched = _ET_NS_RE.match(tag)
        if matched:
            (uri, tag) = matched.groups()
            prefix = nspaces.get(uri, False)
            if prefix:
                return "%s:%s" % (prefix, tag)

    return tag


def _dicts_have_unique_keys(dics):
    """
    :param dics: [<dict or dict-like object>], must not be [] or [{...}]
    :return: True if all keys of each dict of 'dics' are unique

    # Enable the followings if to allow dics is [], [{...}]:
    # >>> all(_dicts_have_unique_keys([d]) for [d]
    # ...     in ({}, {'a': 0}, {'a': 1, 'b': 0}))
    # True
    # >>> _dicts_have_unique_keys([{}, {'a': 1}, {'b': 2, 'c': 0}])
    # True

    >>> _dicts_have_unique_keys([{}, {'a': 1}, {'a': 2}])
    False
    >>> _dicts_have_unique_keys([{}, {'a': 1}, {'b': 2}, {'b': 3, 'c': 0}])
    False
    >>> _dicts_have_unique_keys([{}, {}])
    True
    """
    key_itr = anyconfig.compat.from_iterable(d.keys() for d in dics)
    return len(set(key_itr)) == sum(len(d) for d in dics)


def _merge_dicts(dics, container=dict):
    """
    :param dics: [<dict/-like object must not have same keys each other>]
    :param container: callble to make a container object
    :return: <container> object

    >>> _merge_dicts(({}, ))
    {}
    >>> _merge_dicts(({'a': 1}, ))
    {'a': 1}
    >>> sorted(kv for kv in _merge_dicts(({'a': 1}, {'b': 2})).items())
    [('a', 1), ('b', 2)]
    """
    dic_itr = anyconfig.compat.from_iterable(d.items() for d in dics)
    return container(anyconfig.compat.OrderedDict(dic_itr))


def _parse_text(val, **options):
    """
    :return: Parsed value or value itself depends on 'ac_parse_value'
    """
    if val and options.get("ac_parse_value", False):
        return anyconfig.parser.parse_single(val)

    return val


def _process_elem_text(elem, dic, subdic, text="@text", **options):
    """
    :param elem: ET Element object which has elem.text
    :param dic: <container> (dict[-like]) object converted from elem
    :param subdic: Sub <container> object converted from elem
    :param options:
        Keyword options, see the description of :func:`elem_to_container` for
        more details.

    :return: None but updating elem.text, dic and subdic as side effects
    """
    elem.text = elem.text.strip()
    if elem.text:
        etext = _parse_text(elem.text, **options)
        if len(elem) or elem.attrib:
            subdic[text] = etext
        else:
            dic[elem.tag] = etext  # Only text, e.g. <a>text</a>


def _parse_attrs(elem, container=dict, **options):
    """
    :param elem: ET Element object has attributes (elem.attrib)
    :param container: callble to make a container object
    :return: Parsed value or value itself depends on 'ac_parse_value'
    """
    adic = dict((_tweak_ns(a, **options), v) for a, v in elem.attrib.items())
    if options.get("ac_parse_value", False):
        return container(dict((k, anyconfig.parser.parse_single(v))
                              for k, v in adic.items()))

    return container(adic)


def _process_elem_attrs(elem, dic, subdic, container=dict, attrs="@attrs",
                        **options):
    """
    :param elem: ET Element object or None
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


def _process_children_elems(elem, dic, subdic, container=dict,
                            children="@children", **options):
    """
    :param elem: ET Element object or None
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
    sdics = [container(elem.attrib) if merge_attrs else subdic] + cdics

    if _dicts_have_unique_keys(sdics):  # ex. <a><b>1</b><c>c</c></a>
        dic[elem.tag] = _merge_dicts(sdics, container)
    elif not subdic:  # There are no attrs nor text and only these children.
        dic[elem.tag] = cdics
    else:
        subdic[children] = cdics


def elem_to_container(elem, container=dict, **options):
    """
    Convert XML ElementTree Element to a collection of container objects.

    Elements are transformed to a node under special tagged nodes, attrs, text
    and children, to store the type of these elements basically, however, in
    some special cases like the followings, these nodes are attached to the
    parent node directly for later convenience.

    - There is only text element
    - There are only children elements each has unique keys among all

    :param elem: ET Element object or None
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


def _complement_tag_options(options):
    """
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


def root_to_container(root, container=dict, nspaces=None, **options):
    """
    Convert XML ElementTree Root Element to a collection of container objects.

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
            root.attrib["xmlns:" + prefix if prefix else "xmlns"] = uri

    return elem_to_container(root, container=container, nspaces=nspaces,
                             **_complement_tag_options(options))


def _to_str_fn(**options):
    """
    :param options: Keyword options might have 'ac_parse_value' key
    :param to_str: Callable to convert value to string
    """
    return str if options.get("ac_parse_value") else anyconfig.utils.noop


def _elem_set_attrs(obj, parent, to_str):
    """
    :param obj: Container instance gives attributes of XML Element
    :param parent: XML ElementTree parent node object
    :param to_str: Callable to convert value to string or None
    :param options: Keyword options, see :func:`container_to_etree`

    :return: None but parent will be modified
    """
    for attr, val in anyconfig.compat.iteritems(obj):
        parent.set(attr, to_str(val))


def _elem_from_descendants(children_nodes, **options):
    """
    :param children_nodes: A list of child dict objects
    :param options: Keyword options, see :func:`container_to_etree`
    """
    for child in children_nodes:  # child should be a dict-like object.
        for ckey, cval in anyconfig.compat.iteritems(child):
            celem = ET.Element(ckey)
            container_to_etree(cval, parent=celem, **options)
            yield celem


def _get_or_update_parent(key, val, to_str, parent=None, **options):
    """
    :param key: Key of current child (dict{,-like} object)
    :param val: Value of current child (dict{,-like} object or [dict{,...}])
    :param to_str: Callable to convert value to string
    :param parent: XML ElementTree parent node object or None
    :param options: Keyword options, see :func:`container_to_etree`
    """
    elem = ET.Element(key)

    vals = val if anyconfig.utils.is_iterable(val) else [val]
    for val_ in vals:
        container_to_etree(val_, parent=elem, to_str=to_str, **options)

    if parent is None:  # 'elem' is the top level etree.
        return elem

    parent.append(elem)
    return parent


_ATC = ("attrs", "text", "children")


def container_to_etree(obj, parent=None, to_str=None, **options):
    """
    Convert a dict-like object to XML ElementTree.

    :param obj: Container instance to convert to
    :param parent: XML ElementTree parent node object or None
    :param to_str: Callable to convert value to string or None
    :param options: Keyword options,

        - tags: Dict of tags for special nodes to keep XML info, attributes,
          text and children nodes, e.g. {"attrs": "@attrs", "text": "#text"}
    """
    if to_str is None:
        to_str = _to_str_fn(**options)

    if not anyconfig.utils.is_dict_like(obj):
        if parent is not None and obj:
            parent.text = to_str(obj)  # Parent is a leaf text node.
        return parent  # All attributes and text should be set already.

    options = _complement_tag_options(options)
    (attrs, text, children) = operator.itemgetter(*_ATC)(options)

    for key, val in anyconfig.compat.iteritems(obj):
        if key == attrs:
            _elem_set_attrs(val, parent, to_str)
        elif key == text:
            parent.text = to_str(val)
        elif key == children:
            for celem in _elem_from_descendants(val, **options):
                parent.append(celem)
        else:
            parent = _get_or_update_parent(key, val, to_str, parent=parent,
                                           **options)

    return ET.ElementTree(parent)


def etree_write(tree, stream):
    """
    Write XML ElementTree 'root' content into 'stream'.

    :param tree: XML ElementTree object
    :param stream: File or file-like object can write to
    """
    try:
        tree.write(stream, encoding="utf-8", xml_declaration=True)
    except TypeError:
        tree.write(stream, encoding="unicode", xml_declaration=True)


class Parser(anyconfig.backend.base.Parser,
             anyconfig.backend.base.ToStreamDumperMixin,
             anyconfig.backend.base.BinaryFilesMixin):
    """
    Parser for XML files.
    """
    _cid = "xml"
    _type = "xml"
    _extensions = ["xml"]
    _load_opts = _dump_opts = ["tags", "merge_attrs", "ac_parse_value"]
    _ordered = True
    _dict_opts = ["ac_dict"]

    def load_from_string(self, content, container, **opts):
        """
        Load config from XML snippet (a string 'content').

        :param content:
            XML snippet string of str (python 2) or bytes (python 3) type
        :param container: callble to make a container object
        :param opts: optional keyword parameters passed to

        :return: Dict-like object holding config parameters
        """
        root = ET.fromstring(content)
        if anyconfig.compat.IS_PYTHON_3:
            stream = BytesIO(content)
        else:
            stream = anyconfig.compat.StringIO(content)
        nspaces = _namespaces_from_file(stream)
        return root_to_container(root, container=container,
                                 nspaces=nspaces, **opts)

    def load_from_path(self, filepath, container, **opts):
        """
        :param filepath: XML file path
        :param container: callble to make a container object
        :param opts: optional keyword parameters to be sanitized

        :return: Dict-like object holding config parameters
        """
        root = ET.parse(filepath).getroot()
        nspaces = _namespaces_from_file(filepath)
        return root_to_container(root, container=container,
                                 nspaces=nspaces, **opts)

    def load_from_stream(self, stream, container, **opts):
        """
        :param stream: XML file or file-like object
        :param container: callble to make a container object
        :param opts: optional keyword parameters to be sanitized

        :return: Dict-like object holding config parameters
        """
        root = ET.parse(stream).getroot()
        path = anyconfig.utils.get_path_from_stream(stream)
        nspaces = _namespaces_from_file(path)
        return root_to_container(root, container=container,
                                 nspaces=nspaces, **opts)

    def dump_to_string(self, cnf, **opts):
        """
        :param cnf: Configuration data to dump
        :param opts: optional keyword parameters

        :return: string represents the configuration
        """
        tree = container_to_etree(cnf, **opts)
        buf = BytesIO()
        etree_write(tree, buf)
        return buf.getvalue()

    def dump_to_stream(self, cnf, stream, **opts):
        """
        :param cnf: Configuration data to dump
        :param stream: Config file or file like object write to
        :param opts: optional keyword parameters
        """
        tree = container_to_etree(cnf, **opts)
        etree_write(tree, stream)

# vim:sw=4:ts=4:et:
