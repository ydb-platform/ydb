"""XPath and JMESPath selectors based on the lxml and jmespath Python
packages."""

from __future__ import annotations

import json
import typing
import warnings
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    SupportsIndex,
    TypeAlias,
    TypedDict,
    TypeVar,
)

import jmespath
from lxml import etree, html
from packaging.version import Version

from .csstranslator import GenericTranslator, HTMLTranslator
from .utils import extract_regex, flatten, iflatten, shorten

if TYPE_CHECKING:
    from collections.abc import Mapping
    from re import Pattern

    # typing.Self requires Python 3.11
    from typing_extensions import Self


_SelectorType = TypeVar("_SelectorType", bound="Selector")
_ParserType: TypeAlias = etree.XMLParser | etree.HTMLParser
# simplified _OutputMethodArg from types-lxml
_TostringMethodType = Literal[
    "html",
    "xml",
]

lxml_version = Version(etree.__version__)
lxml_huge_tree_version = Version("4.2")
LXML_SUPPORTS_HUGE_TREE = lxml_version >= lxml_huge_tree_version


class CannotRemoveElementWithoutRoot(Exception):
    pass


class CannotRemoveElementWithoutParent(Exception):
    pass


class CannotDropElementWithoutParent(CannotRemoveElementWithoutParent):
    pass


class SafeXMLParser(etree.XMLParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("resolve_entities", False)
        super().__init__(*args, **kwargs)


class CTGroupValue(TypedDict):
    _parser: type[etree.XMLParser | html.HTMLParser]
    _csstranslator: GenericTranslator | HTMLTranslator
    _tostring_method: _TostringMethodType


_ctgroup: dict[str, CTGroupValue] = {
    "html": {
        "_parser": html.HTMLParser,
        "_csstranslator": HTMLTranslator(),
        "_tostring_method": "html",
    },
    "xml": {
        "_parser": SafeXMLParser,
        "_csstranslator": GenericTranslator(),
        "_tostring_method": "xml",
    },
}


def _xml_or_html(type_: str | None) -> str:
    return "xml" if type_ == "xml" else "html"


def create_root_node(
    text: str,
    parser_cls: type[_ParserType],
    base_url: str | None = None,
    huge_tree: bool = LXML_SUPPORTS_HUGE_TREE,
    body: bytes = b"",
    encoding: str = "utf-8",
) -> etree._Element:
    """Create root node for text using given parser class."""
    if not text:
        body = body.replace(b"\x00", b"").strip()
    else:
        body = text.strip().replace("\x00", "").encode(encoding) or b"<html/>"

    if huge_tree and LXML_SUPPORTS_HUGE_TREE:
        parser = parser_cls(recover=True, encoding=encoding, huge_tree=True)
        root = etree.fromstring(body, parser=parser, base_url=base_url)
    else:
        parser = parser_cls(recover=True, encoding=encoding)
        root = etree.fromstring(body, parser=parser, base_url=base_url)
        for error in parser.error_log:
            if "use XML_PARSE_HUGE option" in error.message:
                warnings.warn(
                    f"Input data is too big. Upgrade to lxml "
                    f"{lxml_huge_tree_version} or later for huge_tree support.",
                    stacklevel=2,
                )
    if root is None:
        root = etree.fromstring(b"<html/>", parser=parser, base_url=base_url)
    return root


class SelectorList(list[_SelectorType]):
    """
    The :class:`SelectorList` class is a subclass of the builtin ``list``
    class, which provides a few additional methods.
    """

    @typing.overload
    def __getitem__(self, pos: SupportsIndex) -> _SelectorType:
        pass

    @typing.overload
    def __getitem__(self, pos: slice) -> SelectorList[_SelectorType]:
        pass

    def __getitem__(
        self, pos: SupportsIndex | slice
    ) -> _SelectorType | SelectorList[_SelectorType]:
        o = super().__getitem__(pos)
        if isinstance(pos, slice):
            return self.__class__(typing.cast("SelectorList[_SelectorType]", o))
        return typing.cast("_SelectorType", o)

    def __getstate__(self) -> None:
        raise TypeError("can't pickle SelectorList objects")

    def jmespath(self, query: str, **kwargs: Any) -> SelectorList[_SelectorType]:
        """
        Call the ``.jmespath()`` method for each element in this list and return
        their results flattened as another :class:`SelectorList`.

        ``query`` is the same argument as the one in :meth:`Selector.jmespath`.

        Any additional named arguments are passed to the underlying
        ``jmespath.search`` call, e.g.::

            selector.jmespath('author.name', options=jmespath.Options(dict_cls=collections.OrderedDict))
        """
        return self.__class__(flatten([x.jmespath(query, **kwargs) for x in self]))

    def xpath(
        self,
        xpath: str,
        namespaces: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> SelectorList[_SelectorType]:
        """
        Call the ``.xpath()`` method for each element in this list and return
        their results flattened as another :class:`SelectorList`.

        ``xpath`` is the same argument as the one in :meth:`Selector.xpath`

        ``namespaces`` is an optional ``prefix: namespace-uri`` mapping (dict)
        for additional prefixes to those registered with ``register_namespace(prefix, uri)``.
        Contrary to ``register_namespace()``, these prefixes are not
        saved for future calls.

        Any additional named arguments can be used to pass values for XPath
        variables in the XPath expression, e.g.::

            selector.xpath('//a[href=$url]', url="http://www.example.com")
        """
        return self.__class__(
            flatten([x.xpath(xpath, namespaces=namespaces, **kwargs) for x in self])
        )

    def css(self, query: str) -> SelectorList[_SelectorType]:
        """
        Call the ``.css()`` method for each element in this list and return
        their results flattened as another :class:`SelectorList`.

        ``query`` is the same argument as the one in :meth:`Selector.css`
        """
        return self.__class__(flatten([x.css(query) for x in self]))

    def re(self, regex: str | Pattern[str], replace_entities: bool = True) -> list[str]:
        """
        Call the ``.re()`` method for each element in this list and return
        their results flattened, as a list of strings.

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``.
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        return flatten([x.re(regex, replace_entities=replace_entities) for x in self])

    @typing.overload
    def re_first(
        self,
        regex: str | Pattern[str],
        default: None = None,
        replace_entities: bool = True,
    ) -> str | None:
        pass

    @typing.overload
    def re_first(
        self,
        regex: str | Pattern[str],
        default: str,
        replace_entities: bool = True,
    ) -> str:
        pass

    def re_first(
        self,
        regex: str | Pattern[str],
        default: str | None = None,
        replace_entities: bool = True,
    ) -> str | None:
        """
        Call the ``.re()`` method for the first element in this list and
        return the result in an string. If the list is empty or the
        regex doesn't match anything, return the default value (``None`` if
        the argument is not provided).

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``.
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        for el in iflatten(
            x.re(regex, replace_entities=replace_entities) for x in self
        ):
            return typing.cast("str", el)
        return default

    def getall(self) -> list[str]:
        """
        Call the ``.get()`` method for each element is this list and return
        their results flattened, as a list of strings.
        """
        return [x.get() for x in self]

    extract = getall

    @typing.overload
    def get(self, default: None = None) -> str | None:
        pass

    @typing.overload
    def get(self, default: str) -> str:
        pass

    def get(self, default: str | None = None) -> Any:
        """
        Return the result of ``.get()`` for the first element in this list.
        If the list is empty, return the default value.
        """
        for x in self:
            return x.get()
        return default

    extract_first = get

    @property
    def attrib(self) -> Mapping[str, str]:
        """Return the attributes dictionary for the first element.
        If the list is empty, return an empty dict.
        """
        for x in self:
            return x.attrib
        return {}

    def drop(self) -> None:
        """
        Drop matched nodes from the parent for each element in this list.
        """
        for x in self:
            x.drop()


_NOT_SET = object()


def _get_root_from_text(text: str, *, type_: str, **lxml_kwargs: Any) -> etree._Element:
    return create_root_node(text, _ctgroup[type_]["_parser"], **lxml_kwargs)


def _get_root_and_type_from_bytes(
    body: bytes,
    encoding: str,
    *,
    input_type: str | None,
    **lxml_kwargs: Any,
) -> tuple[Any, str]:
    if input_type == "text":
        return body.decode(encoding), input_type
    if encoding == "utf-8":
        try:
            data = json.load(BytesIO(body))
        except ValueError:
            data = _NOT_SET
        if data is not _NOT_SET:
            return data, "json"
    if input_type == "json":
        return None, "json"
    assert input_type in ("html", "xml", None)  # nosec
    type_ = _xml_or_html(input_type)
    root = create_root_node(
        text="",
        body=body,
        encoding=encoding,
        parser_cls=_ctgroup[type_]["_parser"],
        **lxml_kwargs,
    )
    return root, type_


def _get_root_and_type_from_text(
    text: str, *, input_type: str | None, **lxml_kwargs: Any
) -> tuple[Any, str]:
    if input_type == "text":
        return text, input_type
    try:
        data = json.loads(text)
    except ValueError:
        data = _NOT_SET
    if data is not _NOT_SET:
        return data, "json"
    if input_type == "json":
        return None, "json"
    assert input_type in ("html", "xml", None)  # nosec
    type_ = _xml_or_html(input_type)
    root = _get_root_from_text(text, type_=type_, **lxml_kwargs)
    return root, type_


def _get_root_type(root: Any, *, input_type: str | None) -> str:
    if isinstance(root, etree._Element):
        if input_type in {"json", "text"}:
            raise ValueError(
                f"Selector got an lxml.etree._Element object as root, "
                f"and {input_type!r} as type."
            )
        return _xml_or_html(input_type)
    if isinstance(root, (dict, list)) or _is_valid_json(root):
        return "json"
    return input_type or "json"


def _is_valid_json(text: str) -> bool:
    try:
        json.loads(text)
    except (TypeError, ValueError):
        return False
    return True


def _load_json_or_none(text: str) -> Any:
    if isinstance(text, (str, bytes, bytearray)):
        try:
            return json.loads(text)
        except ValueError:
            return None
    return None


class Selector:
    """Wrapper for input data in HTML, JSON, or XML format, that allows
    selecting parts of it using selection expressions.

    You can write selection expressions in CSS or XPath for HTML and XML
    inputs, or in JMESPath for JSON inputs.

    ``text`` is an ``str`` object.

    ``body`` is a ``bytes`` object. It can be used together with the
    ``encoding`` argument instead of the ``text`` argument.

    ``type`` defines the selector type. It can be ``"html"`` (default),
    ``"json"``, or ``"xml"``.

    ``base_url`` allows setting a URL for the document. This is needed when looking up external entities with relative paths.
    See the documentation for :func:`lxml.etree.fromstring` for more information.

    ``huge_tree`` controls the lxml/libxml2 feature that forbids parsing
    certain large documents to protect from possible memory exhaustion. The
    argument is ``True`` by default if the installed lxml version supports it,
    which disables the protection to allow parsing such documents. Set it to
    ``False`` if you want to enable the protection.
    See `this lxml FAQ entry <https://lxml.de/FAQ.html#is-lxml-vulnerable-to-xml-bombs>`_
    for more information.
    """

    __slots__ = [
        "__weakref__",
        "_expr",
        "_huge_tree",
        "_text",
        "body",
        "namespaces",
        "root",
        "type",
    ]

    _default_namespaces = {
        "re": "http://exslt.org/regular-expressions",
        # supported in libxslt:
        # set:difference
        # set:has-same-node
        # set:intersection
        # set:leading
        # set:trailing
        "set": "http://exslt.org/sets",
    }
    _lxml_smart_strings = False
    selectorlist_cls = SelectorList["Selector"]

    def __init__(
        self,
        text: str | None = None,
        type: str | None = None,  # noqa: A002
        body: bytes | bytearray = b"",
        encoding: str = "utf-8",
        namespaces: Mapping[str, str] | None = None,
        root: Any | None = _NOT_SET,
        base_url: str | None = None,
        _expr: str | None = None,
        huge_tree: bool = LXML_SUPPORTS_HUGE_TREE,
    ) -> None:
        self.root: Any
        if type not in ("html", "json", "text", "xml", None):
            raise ValueError(f"Invalid type: {type}")

        if text is None and not body and root is _NOT_SET:
            raise ValueError("Selector needs text, body, or root arguments")

        if text is not None and not isinstance(text, str):
            msg = f"text argument should be of type str, got {text.__class__}"
            raise TypeError(msg)

        if text is not None:
            if root is not _NOT_SET:
                warnings.warn(
                    "Selector got both text and root, root is being ignored.",
                    stacklevel=2,
                )
            if not isinstance(text, str):
                msg = f"text argument should be of type str, got {text.__class__}"
                raise TypeError(msg)

            root, type = _get_root_and_type_from_text(  # noqa: A001
                text,
                input_type=type,
                base_url=base_url,
                huge_tree=huge_tree,
            )
            self.root = root
            self.type = type
        elif body:
            if not isinstance(body, (bytes, bytearray)):
                msg = f"body argument should be of type bytes or bytearray, got {body.__class__}"
                raise TypeError(msg)
            root, type = _get_root_and_type_from_bytes(  # noqa: A001
                body=bytes(body),
                encoding=encoding,
                input_type=type,
                base_url=base_url,
                huge_tree=huge_tree,
            )
            self.root = root
            self.type = type
        elif root is _NOT_SET:
            raise ValueError("Selector needs text, body, or root arguments")
        else:
            self.root = root
            self.type = _get_root_type(root, input_type=type)

        self.namespaces = dict(self._default_namespaces)
        if namespaces is not None:
            self.namespaces.update(namespaces)

        self._expr = _expr
        self._huge_tree = huge_tree
        self._text = text

    def __getstate__(self) -> Any:
        raise TypeError("can't pickle Selector objects")

    def _get_root(
        self,
        text: str = "",
        base_url: str | None = None,
        huge_tree: bool = LXML_SUPPORTS_HUGE_TREE,
        type_: str | None = None,
        body: bytes = b"",
        encoding: str = "utf-8",
    ) -> etree._Element:
        return create_root_node(
            text,
            body=body,
            encoding=encoding,
            parser_cls=_ctgroup[type_ or self.type]["_parser"],
            base_url=base_url,
            huge_tree=huge_tree,
        )

    def jmespath(
        self,
        query: str,
        **kwargs: Any,
    ) -> SelectorList[Self]:
        """
        Find objects matching the JMESPath ``query`` and return the result as a
        :class:`SelectorList` instance with all elements flattened. List
        elements implement :class:`Selector` interface too.

        ``query`` is a string containing the `JMESPath
        <https://jmespath.org/>`_ query to apply.

        Any additional named arguments are passed to the underlying
        ``jmespath.search`` call, e.g.::

            selector.jmespath('author.name', options=jmespath.Options(dict_cls=collections.OrderedDict))
        """
        if self.type == "json":
            if isinstance(self.root, str):
                # Selector received a JSON string as root.
                data = _load_json_or_none(self.root)
            else:
                data = self.root
        else:
            assert self.type in {"html", "xml"}  # nosec
            data = _load_json_or_none(self.root.text)

        result = jmespath.search(query, data, **kwargs)
        if result is None:
            result = []
        elif not isinstance(result, list):
            result = [result]

        def make_selector(x: Any) -> Selector:  # closure function
            if isinstance(x, str):
                return self.__class__(text=x, _expr=query, type="text")
            return self.__class__(root=x, _expr=query)

        result = [make_selector(x) for x in result]
        return typing.cast("SelectorList[Self]", self.selectorlist_cls(result))

    def xpath(
        self,
        query: str,
        namespaces: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> SelectorList[Self]:
        """
        Find nodes matching the xpath ``query`` and return the result as a
        :class:`SelectorList` instance with all elements flattened. List
        elements implement :class:`Selector` interface too.

        ``query`` is a string containing the XPATH query to apply.

        ``namespaces`` is an optional ``prefix: namespace-uri`` mapping (dict)
        for additional prefixes to those registered with ``register_namespace(prefix, uri)``.
        Contrary to ``register_namespace()``, these prefixes are not
        saved for future calls.

        Any additional named arguments can be used to pass values for XPath
        variables in the XPath expression, e.g.::

            selector.xpath('//a[href=$url]', url="http://www.example.com")
        """
        if self.type not in ("html", "xml", "text"):
            raise ValueError(f"Cannot use xpath on a Selector of type {self.type!r}")
        if self.type in ("html", "xml"):
            try:
                xpathev = self.root.xpath
            except AttributeError:
                return typing.cast("SelectorList[Self]", self.selectorlist_cls([]))
        else:
            try:
                xpathev = self._get_root(self._text or "", type_="html").xpath
            except AttributeError:
                return typing.cast("SelectorList[Self]", self.selectorlist_cls([]))

        nsp = dict(self.namespaces)
        if namespaces is not None:
            nsp.update(namespaces)
        try:
            result = xpathev(
                query,
                namespaces=nsp,
                smart_strings=self._lxml_smart_strings,
                **kwargs,
            )
        except etree.XPathError as exc:
            raise ValueError(f"XPath error: {exc} in {query}")

        if not isinstance(result, list):
            result = [result]

        result = [
            self.__class__(
                root=x,
                _expr=query,
                namespaces=self.namespaces,
                type=_xml_or_html(self.type),
            )
            for x in result
        ]
        return typing.cast("SelectorList[Self]", self.selectorlist_cls(result))

    def css(self, query: str) -> SelectorList[Self]:
        """
        Apply the given CSS selector and return a :class:`SelectorList` instance.

        ``query`` is a string containing the CSS selector to apply.

        In the background, CSS queries are translated into XPath queries using
        `cssselect`_ library and run ``.xpath()`` method.

        .. _cssselect: https://pypi.python.org/pypi/cssselect/
        """
        if self.type not in ("html", "xml", "text"):
            raise ValueError(f"Cannot use css on a Selector of type {self.type!r}")
        return self.xpath(self._css2xpath(query))

    def _css2xpath(self, query: str) -> str:
        type_ = _xml_or_html(self.type)
        return _ctgroup[type_]["_csstranslator"].css_to_xpath(query)

    def re(self, regex: str | Pattern[str], replace_entities: bool = True) -> list[str]:
        """
        Apply the given regex and return a list of strings with the
        matches.

        ``regex`` can be either a compiled regular expression or a string which
        will be compiled to a regular expression using ``re.compile(regex)``.

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``).
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        data = self.get()
        return extract_regex(regex, data, replace_entities=replace_entities)

    @typing.overload
    def re_first(
        self,
        regex: str | Pattern[str],
        default: None = None,
        replace_entities: bool = True,
    ) -> str | None:
        pass

    @typing.overload
    def re_first(
        self,
        regex: str | Pattern[str],
        default: str,
        replace_entities: bool = True,
    ) -> str:
        pass

    def re_first(
        self,
        regex: str | Pattern[str],
        default: str | None = None,
        replace_entities: bool = True,
    ) -> str | None:
        """
        Apply the given regex and return the first string which matches. If
        there is no match, return the default value (``None`` if the argument
        is not provided).

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``).
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        return next(
            iflatten(self.re(regex, replace_entities=replace_entities)),
            default,
        )

    def get(self) -> Any:
        """
        Serialize and return the matched nodes.

        For HTML and XML, the result is always a string, and percent-encoded
        content is unquoted.
        """
        if self.type in ("text", "json"):
            return self.root
        try:
            return etree.tostring(
                self.root,
                method=_ctgroup[self.type]["_tostring_method"],
                encoding="unicode",
                with_tail=False,
            )
        except (AttributeError, TypeError):
            if self.root is True:
                return "1"
            if self.root is False:
                return "0"
            return str(self.root)

    extract = get

    def getall(self) -> list[str]:
        """
        Serialize and return the matched node in a 1-element list of strings.
        """
        return [self.get()]

    def register_namespace(self, prefix: str, uri: str) -> None:
        """
        Register the given namespace to be used in this :class:`Selector`.
        Without registering namespaces you can't select or extract data from
        non-standard namespaces. See :ref:`selector-examples-xml`.
        """
        self.namespaces[prefix] = uri

    def remove_namespaces(self) -> None:
        """
        Remove all namespaces, allowing to traverse the document using
        namespace-less xpaths. See :ref:`removing-namespaces`.
        For JSON selectors, this method does nothing.
        """
        if self.type == "json":
            return

        for el in self.root.iter("*"):
            if el.tag.startswith("{"):
                el.tag = el.tag.split("}", 1)[1]
            # loop on element attributes also
            for an in el.attrib:
                if an.startswith("{"):
                    el.attrib[an.split("}", 1)[1]] = el.attrib.pop(an)
        # remove namespace declarations
        etree.cleanup_namespaces(self.root)

    def drop(self) -> None:
        """
        Drop matched nodes from the parent element.
        """
        try:
            parent = self.root.getparent()
        except AttributeError:
            # 'str' object has no attribute 'getparent'
            raise CannotRemoveElementWithoutRoot(
                "The node you're trying to drop has no root, "
                "are you trying to drop a pseudo-element? "
                "Try to use 'li' as a selector instead of 'li::text' or "
                "'//li' instead of '//li/text()', for example."
            )

        try:
            if self.type == "xml":
                if parent is None:
                    raise ValueError("This node has no parent")
                parent.remove(self.root)
            else:
                typing.cast("html.HtmlElement", self.root).drop_tree()
        except (AttributeError, AssertionError):
            # 'NoneType' object has no attribute 'drop'
            raise CannotDropElementWithoutParent(
                "The node you're trying to remove has no parent, "
                "are you trying to remove a root element?"
            )

    @property
    def attrib(self) -> dict[str, str]:
        """
        Return the attributes dictionary for underlying element.
        For JSON selectors, return an empty dict.
        """
        if self.type == "json":
            return {}

        return dict(self.root.attrib)

    def __bool__(self) -> bool:
        """
        Return ``True`` if there is any real content selected or ``False``
        otherwise.  In other words, the boolean value of a :class:`Selector` is
        given by the contents it selects.
        """
        return bool(self.get())

    __nonzero__ = __bool__

    def __str__(self) -> str:
        return str(self.get())

    def __repr__(self) -> str:
        data = repr(shorten(str(self.get()), width=40))
        return f"<{type(self).__name__} query={self._expr!r} data={data}>"
