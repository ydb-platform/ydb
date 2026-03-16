import math

from flask import abort
from mongoengine.queryset import QuerySet

__all__ = ("Pagination", "ListFieldPagination")


class Pagination(object):
    def __init__(self, iterable, page, per_page):

        if page < 1:
            abort(404)

        self.iterable = iterable
        self.page = page
        self.per_page = per_page

        if isinstance(iterable, QuerySet):
            self.total = iterable.count()
        else:
            self.total = len(iterable)

        start_index = (page - 1) * per_page
        end_index = page * per_page

        self.items = iterable[start_index:end_index]
        if isinstance(self.items, QuerySet):
            self.items = self.items.select_related()
        if not self.items and page != 1:
            abort(404)

    @property
    def pages(self):
        """The total number of pages"""
        return int(math.ceil(self.total / float(self.per_page)))

    def prev(self, error_out=False):
        """Returns a :class:`Pagination` object for the previous page."""
        assert self.iterable is not None, (
            "an object is required " "for this method to work"
        )
        iterable = self.iterable
        if isinstance(iterable, QuerySet):
            iterable._skip = None
            iterable._limit = None
        return self.__class__(iterable, self.page - 1, self.per_page)

    @property
    def prev_num(self):
        """Number of the previous page."""
        return self.page - 1

    @property
    def has_prev(self):
        """True if a previous page exists"""
        return self.page > 1

    def next(self, error_out=False):
        """Returns a :class:`Pagination` object for the next page."""
        assert self.iterable is not None, (
            "an object is required " "for this method to work"
        )
        iterable = self.iterable
        if isinstance(iterable, QuerySet):
            iterable._skip = None
            iterable._limit = None
        return self.__class__(iterable, self.page + 1, self.per_page)

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self):
        """Number of the next page"""
        return self.page + 1

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination.  The four
        parameters control the thresholds how many numbers should be produced
        from the sides.  Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:

        .. sourcecode:: html+jinja

            {% macro render_pagination(pagination, endpoint) %}
              <div class=pagination>
              {%- for page in pagination.iter_pages() %}
                {% if page %}
                  {% if page != pagination.page %}
                    <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                  {% else %}
                    <strong>{{ page }}</strong>
                  {% endif %}
                {% else %}
                  <span class=ellipsis>…</span>
                {% endif %}
              {%- endfor %}
              </div>
            {% endmacro %}
        """
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or num > self.pages - right_edge
                or (
                    num >= self.page - left_current and num <= self.page + right_current
                )
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num
        if last != self.pages:
            yield None


class ListFieldPagination(Pagination):
    def __init__(self, queryset, doc_id, field_name, page, per_page, total=None):
        """Allows an array within a document to be paginated.

        Queryset must contain the document which has the array we're
        paginating, and doc_id should be it's _id.
        Field name is the name of the array we're paginating.
        Page and per_page work just like in Pagination.
        Total is an argument because it can be computed more efficiently
        elsewhere, but we still use array.length as a fallback.
        """
        if page < 1:
            abort(404)

        self.page = page
        self.per_page = per_page

        self.queryset = queryset
        self.doc_id = doc_id
        self.field_name = field_name

        start_index = (page - 1) * per_page

        field_attrs = {field_name: {"$slice": [start_index, per_page]}}

        qs = queryset(pk=doc_id)
        self.items = getattr(qs.fields(**field_attrs).first(), field_name)
        self.total = total or len(
            getattr(qs.fields(**{field_name: 1}).first(), field_name)
        )

        if not self.items and page != 1:
            abort(404)

    def prev(self, error_out=False):
        """Returns a :class:`Pagination` object for the previous page."""
        assert self.items is not None, (
            "a query object is required " "for this method to work"
        )
        return self.__class__(
            self.queryset,
            self.doc_id,
            self.field_name,
            self.page - 1,
            self.per_page,
            self.total,
        )

    def next(self, error_out=False):
        """Returns a :class:`Pagination` object for the next page."""
        assert self.items is not None, (
            "a query object is required " "for this method to work"
        )
        return self.__class__(
            self.queryset,
            self.doc_id,
            self.field_name,
            self.page + 1,
            self.per_page,
            self.total,
        )
