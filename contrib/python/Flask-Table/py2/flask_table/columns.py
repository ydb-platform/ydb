from __future__ import unicode_literals

from flask import Markup, url_for
from babel.dates import format_date, format_datetime
from flask_babel import gettext as _

from .html import element


def _single_get(item, key):
    # First, try to lookup the key as if the item were a dict. If
    # that fails, lookup the key as an atrribute of an item.
    try:
        val = item[key]
    except (KeyError, TypeError):
        val = getattr(item, key)

    # once we have the value, try calling it as a function. If
    # that fails, the just return it.
    try:
        return val()
    except TypeError:
        return val


def _recursive_getattr(item, keys):
    # See if keys is as string, if so, we need to split on the dots.
    try:
        keys = keys.split('.')
    except AttributeError:
        pass

    if item is None:
        return None
    if len(keys) == 1:
        return _single_get(item, keys[0])
    else:
        return _recursive_getattr(_single_get(item, keys[0]), keys[1:])


class Col(object):
    """The subclass for all Columns, and the class that just gets some
    data from each item an outputs it.

    We use this hack with _counter to make sure that the columns end
    up in the same order as when declared. Each column must set a
    name, which is used in as the heading for that column, and can
    optionally set an attr or an attr_list. attr='foo' is equivalent
    to attr_list=['foo'] and attr_list=['foo', 'bar', 'baz'] will
    attempt to get item.foo.bar.baz for each item in the iterable
    given to the table. If item.foo.bar is None, then this process
    will terminate and will not error. However, if item.foo.bar is an
    object without an attribute 'baz', then this will currently error.

    """

    _counter = 0

    def __init__(self, name, attr=None, attr_list=None,
                 allow_sort=True, show=True,
                 th_html_attrs=None, td_html_attrs=None,
                 column_html_attrs=None):
        self.name = name
        self.allow_sort = allow_sort
        self._counter_val = Col._counter
        self.attr_list = attr_list
        if attr:
            self.attr_list = attr.split('.')
        self.show = show

        column_html_attrs = column_html_attrs or {}
        self.td_html_attrs = column_html_attrs.copy()
        self.td_html_attrs.update(td_html_attrs or {})
        self.th_html_attrs = column_html_attrs.copy()
        self.th_html_attrs.update(th_html_attrs or {})

        Col._counter += 1

    def get_attr_list(self, attr):
        if self.attr_list:
            return self.attr_list
        elif attr:
            return attr.split('.')
        else:
            return None

    def from_attr_list(self, item, attr_list):
        out = _recursive_getattr(item, attr_list)
        if out is None:
            return ''
        else:
            return out

    def td(self, item, attr):
        content = self.td_contents(item, self.get_attr_list(attr))
        return element(
            'td',
            content=content,
            escape_content=False,
            attrs=self.td_html_attrs)

    def td_contents(self, item, attr_list):
        """Given an item and an attr, return the contents of the td.

        This method is a likely candidate to override when extending
        the Col class, which is done in LinkCol and
        ButtonCol. Override this method if you need to get some extra
        data from the item.

        Note that the output of this function is NOT escaped.

        """
        return self.td_format(self.from_attr_list(item, attr_list))

    def td_format(self, content):
        """Given just the value extracted from the item, return what should
        appear within the td.

        This method is also a good choice to override when extending,
        which is done in the BoolCol, DateCol and DatetimeCol
        classes. Override this method when you just need the standard
        data that attr_list gets from the item, but need to adjust how
        it is represented.

        Note that the output of this function is escaped.

        """
        return Markup.escape(content)


class OptCol(Col):
    """Translate the contents according to a dictionary of choices.

    """

    def __init__(self, name, choices=None, default_key=None, default_value='',
                 coerce_fn=None, **kwargs):
        super(OptCol, self).__init__(name, **kwargs)
        if choices is None:
            self.choices = {}
        else:
            self.choices = choices
        self.default_value = self.choices.get(default_key, default_value)
        self.coerce_fn = coerce_fn

    def from_attr_list(self, item, attr_list):
        # Don't convert None to empty string here.
        return _recursive_getattr(item, attr_list)

    def coerce_content(self, content):
        if self.coerce_fn:
            return self.coerce_fn(content)
        else:
            return content

    def td_format(self, content):
        return self.choices.get(
            self.coerce_content(content), self.default_value)


class BoolCol(OptCol):
    """Output Yes/No values for truthy or falsey values.

    """

    yes_display = _('Yes')
    no_display = _('No')

    def __init__(self, name, yes_display=None, no_display=None, **kwargs):
        if yes_display is None:
            yes_display = self.yes_display
        if no_display is None:
            no_display = self.no_display
        super(BoolCol, self).__init__(
            name,
            choices={True: yes_display, False: no_display},
            coerce_fn=bool,
            **kwargs)


class BoolNaCol(OptCol):
    """Output Yes/No values for truthy or falsey values, or N/A for None.

    """

    yes_display = _('Yes')
    no_display = _('No')
    na_display = _('N/A')

    def __init__(self, name, yes_display=None, no_display=None,
                 na_display=None, **kwargs):
        if yes_display is None:
            yes_display = self.yes_display
        if no_display is None:
            no_display = self.no_display
        if na_display is None:
            na_display = self.na_display

        def bool_or_none(value):
            if value is None:
                return None
            return bool(value)

        super(BoolNaCol, self).__init__(
            name,
            choices={True: yes_display, False: no_display, None: na_display},
            coerce_fn=bool_or_none,
            **kwargs)


class DateCol(Col):
    """Format the content as a date, unless it is None, in which case,
    output empty.

    """
    def __init__(self, name, date_format='short', **kwargs):
        super(DateCol, self).__init__(name, **kwargs)
        self.date_format = date_format

    def td_format(self, content):
        if content:
            return format_date(content, self.date_format)
        else:
            return ''


class DatetimeCol(Col):
    """Format the content as a datetime, unless it is None, in which case,
    output empty.

    """
    def __init__(self, name, datetime_format='short', **kwargs):
        super(DatetimeCol, self).__init__(name, **kwargs)
        self.datetime_format = datetime_format

    def td_format(self, content):
        if content:
            return format_datetime(content, self.datetime_format)
        else:
            return ''


class LinkCol(Col):
    """Format the content as a link. Requires a endpoint to use to find
    the url and can also take a dict of url_kwargs which is expected
    to have values that are strings which are used to get data from
    the item.

    Eg:

    view = LinkCol('View', 'view_fn', url_kwargs=dict(id='id'))

    This will create a link to the address given by url_for('view_fn',
    id=item.id) for each item in the iterable.

    """
    def __init__(self, name, endpoint, attr=None, attr_list=None,
                 url_kwargs=None, url_kwargs_extra=None,
                 anchor_attrs=None, text_fallback=None, **kwargs):
        super(LinkCol, self).__init__(
            name,
            attr=attr,
            attr_list=attr_list,
            **kwargs)
        self.endpoint = endpoint
        self._url_kwargs = url_kwargs or {}
        self._url_kwargs_extra = url_kwargs_extra or {}
        self.text_fallback = text_fallback
        self.anchor_attrs = anchor_attrs or {}

    def url_kwargs(self, item):
        # We give preference to the item kwargs, rather than the extra
        # kwargs.
        kwargs = self._url_kwargs_extra.copy()
        item_kwargs = {k: _recursive_getattr(item, v)
                       for k, v in self._url_kwargs.items()}
        kwargs.update(item_kwargs)
        return kwargs

    def get_attr_list(self, attr):
        return super(LinkCol, self).get_attr_list(None)

    def text(self, item, attr_list):
        if attr_list:
            return self.from_attr_list(item, attr_list)
        elif self.text_fallback:
            return self.text_fallback
        else:
            return self.name

    def url(self, item):
        return url_for(self.endpoint, **self.url_kwargs(item))

    def td_contents(self, item, attr_list):
        attrs = dict(href=self.url(item))
        attrs.update(self.anchor_attrs)
        text = self.td_format(self.text(item, attr_list))
        return element('a', attrs=attrs, content=text, escape_content=False)


class ButtonCol(LinkCol):
    """Just the same a LinkCol, but creates an empty form which gets
    posted to the specified url.

    Eg:

    delete = ButtonCol('Delete', 'delete_fn', url_kwargs=dict(id='id'))

    When clicked, this will post to url_for('delete_fn', id=item.id).

    Can pass button_attrs to pass extra attributes to the button
    element.

    """

    def __init__(self, name, endpoint, attr=None, attr_list=None,
                 url_kwargs=None, button_attrs=None, form_attrs=None,
                 form_hidden_fields=None, **kwargs):
        super(ButtonCol, self).__init__(
            name,
            endpoint,
            attr=attr,
            attr_list=attr_list,
            url_kwargs=url_kwargs, **kwargs)
        self.button_attrs = button_attrs or {}
        self.form_attrs = form_attrs or {}
        self.form_hidden_fields = form_hidden_fields or {}

    def td_contents(self, item, attr_list):
        button_attrs = dict(self.button_attrs)
        button_attrs['type'] = 'submit'
        button = element(
            'button',
            attrs=button_attrs,
            content=self.text(item, attr_list),
        )
        form_attrs = dict(self.form_attrs)
        form_attrs.update(dict(
            method='post',
            action=self.url(item),
        ))
        form_hidden_fields_elements = [
            element(
                'input',
                attrs=dict(
                    type='hidden',
                    name=name,
                    value=value))
            for name, value in sorted(self.form_hidden_fields.items())]
        return element(
            'form',
            attrs=form_attrs,
            content=[
                ''.join(form_hidden_fields_elements),
                button
            ],
            escape_content=False,
        )


class NestedTableCol(Col):
    """This column type allows for nesting tables into a column.  The
    nested table is defined as a sub-class of Table as usual. Then in
    the main table, a column is defined using NestedTableCol with the
    second argument being the name of the Table sub-class object
    defined for the nested table.

    Eg:

    class MySubTable(Table):
        a = Col('1st nested table col')
        b = Col('2nd nested table col')

    class MainTable(Table):
        id = Col('id')
        objects = NestedTableCol('objects', MySubTable)

    """

    def __init__(self, name, table_class, **kwargs):
        super(NestedTableCol, self).__init__(name, **kwargs)
        self.table_class = table_class

    def td_format(self, content):
        t = self.table_class(content).__html__()
        return t
