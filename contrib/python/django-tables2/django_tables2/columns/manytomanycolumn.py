from django.db import models
from django.utils.encoding import force_str
from django.utils.html import conditional_escape
from django.utils.safestring import mark_safe

from .base import Column, LinkTransform, library


@library.register
class ManyToManyColumn(Column):
    """
    Display the list of objects from a `ManyRelatedManager`.

    Ordering is disabled for this column.

    Arguments:
        transform: callable to transform each item to text, it gets an item as argument
            and must return a string-like representation of the item.
            By default, it calls `~django.utils.force_str` on each item.
        filter: callable to filter, limit or order the QuerySet, it gets the
            `ManyRelatedManager` as first argument and must return a filtered QuerySet.
            By default, it returns `all()`
        separator: separator string to join the items with. default: ``", "``
        linkify_item: callable, arguments to reverse() or `True` to wrap items in a ``<a>`` tag.
            For a detailed explanation, see ``linkify`` argument to ``Column``.

    For example, when displaying a list of friends with their full name::

        # models.py
        class Person(models.Model):
            first_name = models.CharField(max_length=200)
            last_name = models.CharField(max_length=200)
            friends = models.ManyToManyField(Person)
            is_active = models.BooleanField(default=True)

            @property
            def name(self):
                return f"{self.first_name} {self.last_name}"

        # tables.py
        class PersonTable(tables.Table):
            name = tables.Column(order_by=("last_name", "first_name"))
            friends = tables.ManyToManyColumn(transform=lambda user: user.name)

    If only the active friends should be displayed, you can use the `filter` argument::

        friends = tables.ManyToManyColumn(filter=lambda qs: qs.filter(is_active=True))

    """

    def __init__(
        self, transform=None, filter=None, separator=", ", linkify_item=None, *args, **kwargs
    ):
        kwargs.setdefault("orderable", False)
        super().__init__(*args, **kwargs)

        if transform is not None:
            self.transform = transform
        if filter is not None:
            self.filter = filter
        self.separator = separator

        link_kwargs = None
        if callable(linkify_item):
            link_kwargs = dict(url=linkify_item)
        elif isinstance(linkify_item, (dict, tuple)):
            link_kwargs = dict(reverse_args=linkify_item)
        elif linkify_item is True:
            link_kwargs = dict()

        if link_kwargs is not None:
            self.linkify_item = LinkTransform(attrs=self.attrs.get("a", {}), **link_kwargs)

    def transform(self, obj):
        """Apply to each item of the list of objects from the ManyToMany relation."""
        return force_str(obj)

    def filter(self, qs):
        """Call on the ManyRelatedManager to allow ordering, filtering or limiting on the set of related objects."""
        return qs.all()

    def render(self, value):
        items = []
        for item in self.filter(value):
            content = conditional_escape(self.transform(item))
            if hasattr(self, "linkify_item"):
                content = self.linkify_item(content=content, record=item)

            items.append(content)

        return mark_safe(conditional_escape(self.separator).join(items))

    @classmethod
    def from_field(cls, field, **kwargs):
        if isinstance(field, models.ManyToManyField):
            return cls(**kwargs)
