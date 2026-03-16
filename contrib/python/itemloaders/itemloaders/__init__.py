"""
Item Loader

See documentation in :ref:`declaring-loaders`.
"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any

from itemadapter import ItemAdapter
from parsel import Selector  # noqa: TC002  # for sphinx
from parsel.utils import extract_regex, flatten

from itemloaders.common import wrap_loader_context
from itemloaders.processors import Identity
from itemloaders.utils import arg_to_iter

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, MutableMapping
    from re import Pattern

    # typing.Self requires Python 3.11
    from typing_extensions import Self


def unbound_method(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    Allow to use single-argument functions as input or output processors
    (no need to define an unused first 'self' argument)
    """
    with suppress(AttributeError):
        if "." not in method.__qualname__:
            return method.__func__  # type: ignore[attr-defined, no-any-return]
    return method


class ItemLoader:
    """
    Return a new Item Loader for populating the given item. If no item is
    given, one is instantiated automatically using the class in
    :attr:`default_item_class`.

    When instantiated with a :param ``selector`` parameter the :class:`ItemLoader` class
    provides convenient mechanisms for extracting data from web pages
    using parsel_ selectors.

    :param item: The item instance to populate using subsequent calls to
        :meth:`~ItemLoader.add_xpath`, :meth:`~ItemLoader.add_css`,
        :meth:`~ItemLoader.add_jmes` or :meth:`~ItemLoader.add_value`.
    :type item: :class:`dict` object

    :param selector: The selector to extract data from, when using the
        :meth:`add_xpath` (resp. :meth:`add_css`, :meth:`add_jmes`) or :meth:`replace_xpath`
        (resp. :meth:`replace_css`, :meth:`replace_jmes`) method.
    :type selector: :class:`~parsel.selector.Selector` object

    The item, selector and the remaining keyword arguments are
    assigned to the Loader context (accessible through the :attr:`context` attribute).

    .. attribute:: item

        The item object being parsed by this Item Loader.
        This is mostly used as a property so when attempting to override this
        value, you may want to check out :attr:`default_item_class` first.

    .. attribute:: context

        The currently active :ref:`Context <loaders-context>` of this Item Loader.
        Refer to <loaders-context> for more information about the Loader Context.

    .. attribute:: default_item_class

        An Item class (or factory), used to instantiate items when not given in
        the ``__init__`` method.

        .. warning:: Currently, this factory/class needs to be
            callable/instantiated without any arguments.
            If you are using ``dataclasses``, please consider the following
            alternative::

                from dataclasses import dataclass, field
                from typing import Optional

                @dataclass
                class Product:
                    name: Optional[str] = field(default=None)
                    price: Optional[float] = field(default=None)

    .. attribute:: default_input_processor

        The default input processor to use for those fields which don't specify
        one.

    .. attribute:: default_output_processor

        The default output processor to use for those fields which don't specify
        one.

    .. attribute:: selector

        The :class:`~parsel.selector.Selector` object to extract data from.
        It's the selector given in the ``__init__`` method.
        This attribute is meant to be read-only.

    .. _parsel: https://parsel.readthedocs.io/en/latest/
    """

    default_item_class: type = dict
    default_input_processor: Callable[..., Any] = Identity()
    default_output_processor: Callable[..., Any] = Identity()

    def __init__(
        self,
        item: Any = None,
        selector: Selector | None = None,
        parent: ItemLoader | None = None,
        **context: Any,
    ):
        self.selector: Selector | None = selector
        context.update(selector=selector)
        if item is None:
            item = self.default_item_class()
        self._local_item = item
        context["item"] = item
        self.context: MutableMapping[str, Any] = context
        self.parent: ItemLoader | None = parent
        self._local_values: dict[str, list[Any]] = {}
        # values from initial item
        for field_name, value in ItemAdapter(item).items():
            self._values.setdefault(field_name, [])
            self._values[field_name] += arg_to_iter(value)

    @property
    def _values(self) -> dict[str, list[Any]]:
        if self.parent is not None:
            return self.parent._values
        return self._local_values

    @property
    def item(self) -> Any:
        if self.parent is not None:
            return self.parent.item
        return self._local_item

    def nested_xpath(self, xpath: str, **context: Any) -> Self:
        """
        Create a nested loader with an xpath selector.
        The supplied selector is applied relative to selector associated
        with this :class:`ItemLoader`. The nested loader shares the item
        with the parent :class:`ItemLoader` so calls to :meth:`add_xpath`,
        :meth:`add_value`, :meth:`replace_value`, etc. will behave as expected.
        """
        self._check_selector_method()
        assert self.selector is not None
        selector = self.selector.xpath(xpath)
        context.update(selector=selector)
        return self.__class__(item=self.item, parent=self, **context)

    def nested_css(self, css: str, **context: Any) -> Self:
        """
        Create a nested loader with a css selector.
        The supplied selector is applied relative to selector associated
        with this :class:`ItemLoader`. The nested loader shares the item
        with the parent :class:`ItemLoader` so calls to :meth:`add_xpath`,
        :meth:`add_value`, :meth:`replace_value`, etc. will behave as expected.
        """
        self._check_selector_method()
        assert self.selector is not None
        selector = self.selector.css(css)
        context.update(selector=selector)
        return self.__class__(item=self.item, parent=self, **context)

    def add_value(
        self,
        field_name: str | None,
        value: Any,
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Process and then add the given ``value`` for the given field.

        The value is first passed through :meth:`get_value` by giving the
        ``processors`` and ``kwargs``, and then passed through the
        :ref:`field input processor <processors>` and its result
        appended to the data collected for that field. If the field already
        contains collected data, the new data is added.

        The given ``field_name`` can be ``None``, in which case values for
        multiple fields may be added. And the processed value should be a dict
        with field_name mapped to values.

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader

        Examples::

            loader.add_value('name', 'Color TV')
            loader.add_value('colours', ['white', 'blue'])
            loader.add_value('length', '100')
            loader.add_value('name', 'name: foo', TakeFirst(), re='name: (.+)')
            loader.add_value(None, {'name': 'foo', 'sex': 'male'})

        """
        value = self.get_value(value, *processors, re=re, **kw)
        if value is None:
            return self
        if not field_name:
            for k, v in value.items():
                self._add_value(k, v)
        else:
            self._add_value(field_name, value)
        return self

    def replace_value(
        self,
        field_name: str | None,
        value: Any,
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`add_value` but replaces the collected data with the
        new value instead of adding it.

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader
        """
        value = self.get_value(value, *processors, re=re, **kw)
        if value is None:
            return self
        if not field_name:
            for k, v in value.items():
                self._replace_value(k, v)
        else:
            self._replace_value(field_name, value)
        return self

    def _add_value(self, field_name: str, value: Any) -> None:
        value = arg_to_iter(value)
        processed_value = self._process_input_value(field_name, value)
        if processed_value:
            self._values.setdefault(field_name, [])
            self._values[field_name] += arg_to_iter(processed_value)

    def _replace_value(self, field_name: str, value: Any) -> None:
        self._values.pop(field_name, None)
        self._add_value(field_name, value)

    def get_value(
        self,
        value: Any,
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Any:
        """
        Process the given ``value`` by the given ``processors`` and keyword
        arguments.

        Available keyword arguments:

        :param re: a regular expression to use for extracting data from the
            given value using :func:`~parsel.utils.extract_regex` method,
            applied before processors
        :type re: str or typing.Pattern[str]

        Examples:

        >>> from itemloaders import ItemLoader
        >>> from itemloaders.processors import TakeFirst
        >>> loader = ItemLoader()
        >>> loader.get_value('name: foo', TakeFirst(), str.upper, re='name: (.+)')
        'FOO'
        """
        if re:
            value = arg_to_iter(value)
            value = flatten(extract_regex(re, x) for x in value)

        for proc in processors:
            if value is None:
                break
            _proc = proc
            proc = wrap_loader_context(proc, self.context)  # noqa: PLW2901
            try:
                value = proc(value)
            except Exception as e:
                raise ValueError(
                    f"Error with processor {_proc.__class__.__name__} "
                    f"value={value!r} error='{type(e).__name__}: {e!s}'"
                ) from e
        return value

    def load_item(self) -> Any:
        """
        Populate the item with the data collected so far, and return it. The
        data collected is first passed through the :ref:`output processors
        <processors>` to get the final value to assign to each item field.
        """
        adapter = ItemAdapter(self.item)
        for field_name in tuple(self._values):
            value = self.get_output_value(field_name)
            if value is not None:
                adapter[field_name] = value

        return adapter.item

    def get_output_value(self, field_name: str) -> Any:
        """
        Return the collected values parsed using the output processor, for the
        given field. This method doesn't populate or modify the item at all.
        """
        proc = self.get_output_processor(field_name)
        proc = wrap_loader_context(proc, self.context)
        value = self._values.get(field_name, [])
        try:
            return proc(value)
        except Exception as e:
            raise ValueError(
                f"Error with output processor: field={field_name!r} "
                f"value={value!r} error='{type(e).__name__}: {e!s}'"
            ) from e

    def get_collected_values(self, field_name: str) -> list[Any]:
        """Return the collected values for the given field."""
        return self._values.get(field_name, [])

    def get_input_processor(self, field_name: str) -> Callable[..., Any]:
        proc = getattr(self, f"{field_name}_in", None)
        if not proc:
            proc = self._get_item_field_attr(
                field_name, "input_processor", self.default_input_processor
            )
        return unbound_method(proc)

    def get_output_processor(self, field_name: str) -> Callable[..., Any]:
        proc = getattr(self, f"{field_name}_out", None)
        if not proc:
            proc = self._get_item_field_attr(
                field_name, "output_processor", self.default_output_processor
            )
        return unbound_method(proc)

    def _get_item_field_attr(
        self, field_name: str, key: Any, default: Any = None
    ) -> Any:
        field_meta = ItemAdapter(self.item).get_field_meta(field_name)
        return field_meta.get(key, default)

    def _process_input_value(self, field_name: str, value: Any) -> Any:
        proc = self.get_input_processor(field_name)
        _proc = proc
        proc = wrap_loader_context(proc, self.context)
        try:
            return proc(value)
        except Exception as e:
            raise ValueError(
                f"Error with input processor {_proc.__class__.__name__}:"
                f" field={field_name!r} value={value!r} error='{type(e).__name__}: {e!s}'"
            ) from e

    def _check_selector_method(self) -> None:
        if self.selector is None:
            raise RuntimeError(
                f"To use XPath or CSS selectors, {self.__class__.__name__} "
                f"must be instantiated with a selector"
            )

    def add_xpath(
        self,
        field_name: str | None,
        xpath: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`ItemLoader.add_value` but receives an XPath instead of a
        value, which is used to extract a list of strings from the
        selector associated with this :class:`ItemLoader`.

        See :meth:`get_xpath` for ``kwargs``.

        :param xpath: the XPath to extract data from
        :type xpath: str

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader

        Examples::

            # HTML snippet: <p class="product-name">Color TV</p>
            loader.add_xpath('name', '//p[@class="product-name"]')
            # HTML snippet: <p id="price">the price is $1200</p>
            loader.add_xpath('price', '//p[@id="price"]', re='the price is (.*)')

        """
        values = self._get_xpathvalues(xpath, **kw)
        return self.add_value(field_name, values, *processors, re=re, **kw)

    def replace_xpath(
        self,
        field_name: str | None,
        xpath: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`add_xpath` but replaces collected data instead of adding it.

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader

        """
        values = self._get_xpathvalues(xpath, **kw)
        return self.replace_value(field_name, values, *processors, re=re, **kw)

    def get_xpath(
        self,
        xpath: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Any:
        """
        Similar to :meth:`ItemLoader.get_value` but receives an XPath instead of a
        value, which is used to extract a list of unicode strings from the
        selector associated with this :class:`ItemLoader`.

        :param xpath: the XPath to extract data from
        :type xpath: str

        :param re: a regular expression to use for extracting data from the
            selected XPath region
        :type re: str or typing.Pattern[str]

        Examples::

            # HTML snippet: <p class="product-name">Color TV</p>
            loader.get_xpath('//p[@class="product-name"]')
            # HTML snippet: <p id="price">the price is $1200</p>
            loader.get_xpath('//p[@id="price"]', TakeFirst(), re='the price is (.*)')

        """
        values = self._get_xpathvalues(xpath, **kw)
        return self.get_value(values, *processors, re=re, **kw)

    def _get_xpathvalues(self, xpaths: str | Iterable[str], **kw: Any) -> list[Any]:
        self._check_selector_method()
        assert self.selector is not None
        xpaths = arg_to_iter(xpaths)
        return flatten(self.selector.xpath(xpath, **kw).getall() for xpath in xpaths)

    def add_css(
        self,
        field_name: str | None,
        css: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`ItemLoader.add_value` but receives a CSS selector
        instead of a value, which is used to extract a list of unicode strings
        from the selector associated with this :class:`ItemLoader`.

        See :meth:`get_css` for ``kwargs``.

        :param css: the CSS selector to extract data from
        :type css: str

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader

        Examples::

            # HTML snippet: <p class="product-name">Color TV</p>
            loader.add_css('name', 'p.product-name')
            # HTML snippet: <p id="price">the price is $1200</p>
            loader.add_css('price', 'p#price', re='the price is (.*)')

        """
        values = self._get_cssvalues(css)
        return self.add_value(field_name, values, *processors, re=re, **kw)

    def replace_css(
        self,
        field_name: str | None,
        css: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`add_css` but replaces collected data instead of adding it.

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader

        """
        values = self._get_cssvalues(css)
        return self.replace_value(field_name, values, *processors, re=re, **kw)

    def get_css(
        self,
        css: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Any:
        """
        Similar to :meth:`ItemLoader.get_value` but receives a CSS selector
        instead of a value, which is used to extract a list of unicode strings
        from the selector associated with this :class:`ItemLoader`.

        :param css: the CSS selector to extract data from
        :type css: str

        :param re: a regular expression to use for extracting data from the
            selected CSS region
        :type re: str or typing.Pattern[str]

        Examples::

            # HTML snippet: <p class="product-name">Color TV</p>
            loader.get_css('p.product-name')
            # HTML snippet: <p id="price">the price is $1200</p>
            loader.get_css('p#price', TakeFirst(), re='the price is (.*)')
        """
        values = self._get_cssvalues(css)
        return self.get_value(values, *processors, re=re, **kw)

    def _get_cssvalues(self, csss: str | Iterable[str]) -> list[Any]:
        self._check_selector_method()
        assert self.selector is not None
        csss = arg_to_iter(csss)
        return flatten(self.selector.css(css).getall() for css in csss)

    def add_jmes(
        self,
        field_name: str | None,
        jmes: str,
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`ItemLoader.add_value` but receives a JMESPath selector
        instead of a value, which is used to extract a list of unicode strings
        from the selector associated with this :class:`ItemLoader`.

        See :meth:`get_jmes` for ``kwargs``.

        :param jmes: the JMESPath selector to extract data from
        :type jmes: str

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader

        Examples::

            # HTML snippet: {"name": "Color TV"}
            loader.add_jmes('name')
            # HTML snippet: {"price": the price is $1200"}
            loader.add_jmes('price', TakeFirst(), re='the price is (.*)')
        """
        values = self._get_jmesvalues(jmes)
        return self.add_value(field_name, values, *processors, re=re, **kw)

    def replace_jmes(
        self,
        field_name: str | None,
        jmes: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Self:
        """
        Similar to :meth:`add_jmes` but replaces collected data instead of adding it.

        :returns: The current ItemLoader instance for method chaining.
        :rtype: ItemLoader
        """
        values = self._get_jmesvalues(jmes)
        return self.replace_value(field_name, values, *processors, re=re, **kw)

    def get_jmes(
        self,
        jmes: str | Iterable[str],
        *processors: Callable[..., Any],
        re: str | Pattern[str] | None = None,
        **kw: Any,
    ) -> Any:
        """
        Similar to :meth:`ItemLoader.get_value` but receives a JMESPath selector
        instead of a value, which is used to extract a list of unicode strings
        from the selector associated with this :class:`ItemLoader`.

        :param jmes: the JMESPath selector to extract data from
        :type jmes: str

        :param re: a regular expression to use for extracting data from the
            selected JMESPath
        :type re: str or typing.Pattern

        Examples::

            # HTML snippet: {"name": "Color TV"}
            loader.get_jmes('name')
            # HTML snippet: {"price": the price is $1200"}
            loader.get_jmes('price', TakeFirst(), re='the price is (.*)')
        """
        values = self._get_jmesvalues(jmes)
        return self.get_value(values, *processors, re=re, **kw)

    def _get_jmesvalues(self, jmess: str | Iterable[str]) -> list[Any]:
        self._check_selector_method()
        assert self.selector is not None
        jmess = arg_to_iter(jmess)
        if not hasattr(self.selector, "jmespath"):
            raise AttributeError(
                "Please install parsel >= 1.8.1 to get JMESPath support"
            )
        return flatten(self.selector.jmespath(jmes).getall() for jmes in jmess)
