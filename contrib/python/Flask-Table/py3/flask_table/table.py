# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from collections import OrderedDict

from flask import Markup
from flask_babel import gettext as _

from .columns import Col
from .compat import with_metaclass
from .html import element


class TableMeta(type):
    """The metaclass for the Table class. We use the metaclass to sort of
    the columns defined in the table declaration.

    """

    def __new__(meta, name, bases, attrs):
        """Create the class as normal, but also iterate over the attributes
        set and gather up any that are Cols, and store them, so they
        can be iterated over later.

        """
        cls = type.__new__(meta, name, bases, attrs)
        cls._cols = OrderedDict()
        # If there are any base classes with a `_cols` attribute, add
        # them to the columns for this table.
        for parent in bases:
            try:
                parent_cols = parent._cols
            except AttributeError:
                continue
            else:
                cls._cols.update(parent_cols)
        # Then add the columns from this class.
        this_cls_cols = sorted(
            ((k, v) for k, v in attrs.items() if isinstance(v, Col)),
            key=lambda x: x[1]._counter_val)
        cls._cols.update(OrderedDict(this_cls_cols))
        return cls


class Table(with_metaclass(TableMeta)):
    """The main table class that should be subclassed when to create a
    table. Initialise with an iterable of objects. Then either use the
    __html__ method, or just output in a template to output the table
    as html. Can also set a list of classes, either when declaring the
    table, or when initialising. Can also set the text to display if
    there are no items to display.

    """

    # For setting attributes on the <table> element.
    html_attrs = None
    classes = []
    table_id = None
    border = False

    thead_attrs = None
    thead_classes = []
    allow_sort = False
    no_items = _('No Items')
    allow_empty = False

    def __init__(self, items, classes=None, thead_classes=None,
                 sort_by=None, sort_reverse=False, no_items=None,
                 table_id=None, border=None, html_attrs=None):
        self.items = items
        self.sort_by = sort_by
        self.sort_reverse = sort_reverse
        if classes is not None:
            self.classes = classes
        if thead_classes is not None:
            self.thead_classes = thead_classes
        if no_items is not None:
            self.no_items = no_items
        if table_id is not None:
            self.table_id = table_id
        if html_attrs is not None:
            self.html_attrs = html_attrs
        if border is not None:
            self.border = border

    def get_html_attrs(self):
        attrs = dict(self.html_attrs) if self.html_attrs else {}
        if self.table_id:
            attrs['id'] = self.table_id
        if self.classes:
            attrs['class'] = ' '.join(self.classes)
        if self.border:
            attrs['border'] = 1
        return attrs

    def get_thead_attrs(self):
        attrs = dict(self.thead_attrs) if self.thead_attrs else {}
        if self.thead_classes:
            attrs['class'] = ' '.join(self.thead_classes)
        return attrs

    def __html__(self):
        tbody = self.tbody()
        if tbody or self.allow_empty:
            content = '\n{thead}\n{tbody}\n'.format(
                thead=self.thead(),
                tbody=tbody,
            )
            return element(
                'table',
                attrs=self.get_html_attrs(),
                content=content,
                escape_content=False)
        else:
            return element('p', content=self.no_items)

    def thead(self):
        ths = ''.join(
            self.th(col_key, col)
            for col_key, col in self._cols.items()
            if col.show)
        content = element('tr', content=ths, escape_content=False)
        return element(
            'thead',
            attrs=self.get_thead_attrs(),
            content=content,
            escape_content=False,
        )

    def tbody(self):
        out = [self.tr(item) for item in self.items]
        if not out:
            return ''
        content = '\n{}\n'.format('\n'.join(out))
        return element('tbody', content=content, escape_content=False)

    def get_tr_attrs(self, item):
        return {}

    def tr(self, item):
        content = ''.join(c.td(item, attr) for attr, c in self._cols.items()
                          if c.show)
        return element(
            'tr',
            attrs=self.get_tr_attrs(item),
            content=content,
            escape_content=False)

    def th_contents(self, col_key, col):
        if not (col.allow_sort and self.allow_sort):
            return Markup.escape(col.name)

        if self.sort_by == col_key:
            if self.sort_reverse:
                href = self.sort_url(col_key)
                label_prefix = '↑'
            else:
                href = self.sort_url(col_key, reverse=True)
                label_prefix = '↓'
        else:
            href = self.sort_url(col_key)
            label_prefix = ''
        label = '{prefix}{label}'.format(prefix=label_prefix, label=col.name)
        return element('a', attrs=dict(href=href), content=label)

    def th(self, col_key, col):
        return element(
            'th',
            content=self.th_contents(col_key, col),
            escape_content=False,
            attrs=col.th_html_attrs,
        )

    def sort_url(self, col_id, reverse=False):
        raise NotImplementedError('sort_url not implemented')

    @classmethod
    def add_column(cls, name, col):
        cls._cols[name] = col
        return cls


def create_table(name=str('_Table'), base=Table, options=None):
    """Creates and returns a new table class. You can specify a name for
    you class if you wish. You can also set the base class (or
    classes) that should be used when creating the class.

    """
    try:
        base = tuple(base)
    except TypeError:
        # Then assume that what we have is a single class, so make it
        # into a 1-tuple.
        base = (base,)

    return TableMeta(name, base, options or {})
