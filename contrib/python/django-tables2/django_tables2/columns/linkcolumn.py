from .base import Column, library


class BaseLinkColumn(Column):
    """
    The base for other columns that render links.

    Arguments:
        text (str or callable): If set, this value will be used to render the
            text inside link instead of value. The callable gets the record
            being rendered as argument.
        attrs (dict): In addition to ``attrs`` keys supported by `~.Column`, the
            following are available:

             - `a` -- ``<a>`` in ``<td>`` elements.
    """

    def __init__(self, text=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text = text

    def text_value(self, record, value):
        if self.text is None:
            return value
        return self.text(record) if callable(self.text) else self.text

    def value(self, record, value):
        """Return the content for a specific cell similarly to `.render` without any HTML content."""
        return self.text_value(record, value)

    def render(self, record, value):
        return self.text_value(record, value)


@library.register
class LinkColumn(BaseLinkColumn):
    """
    Render a normal value as an internal hyperlink to another page.

    .. note ::

        This column should not be used anymore, the `linkify` keyword argument to
        regular columns can be used to achieve the same results.

    It's common to have the primary value in a row hyperlinked to the page
    dedicated to that record.

    The first arguments are identical to that of
    `~django.urls.reverse` and allows an internal URL to be
    described. If this argument is `None`, then `get_absolute_url`.
    (see Django references) will be used.
    The last argument *attrs* allows custom HTML attributes to be added to the
    rendered ``<a href="...">`` tag.

    Arguments:
        viewname (str or None): See `~django.urls.reverse`, or use `None`
            to use the model's `get_absolute_url`
        urlconf (str): See `~django.urls.reverse`.
        args (list): See `~django.urls.reverse`. [2]_
        kwargs (dict): See `~django.urls.reverse`. [2]_
        current_app (str): See `~django.urls.reverse`.
        attrs (dict): HTML attributes that are added to the rendered
            ``<a ...>...</a>`` tag.
        text (str or callable): Either static text, or a callable. If set, this
            will be used to render the text inside link instead of value (default).
            The callable gets the record being rendered as argument.

    .. [2] In order to create a link to a URL that relies on information in the
        current row, `.Accessor` objects can be used in the *args* or *kwargs*
        arguments. The accessor will be resolved using the row's record before
        `~django.urls.reverse` is called.

    Example:

    .. code-block:: python

        # models.py
        class Person(models.Model):
            name = models.CharField(max_length=200)

        # urls.py
        urlpatterns = patterns('',
            url("people/([0-9]+)/", views.people_detail, name="people_detail")
        )

        # tables.py
        from django_tables2.utils import A  # alias for Accessor

        class PeopleTable(tables.Table):
            name = tables.LinkColumn("people_detail", args=[A("pk")])

    In order to override the text value (i.e. ``<a ... >text</a>``) consider
    the following example:

    .. code-block:: python

        # tables.py
        from django_tables2.utils import A  # alias for Accessor

        class PeopleTable(tables.Table):
            name = tables.LinkColumn("people_detail", text="static text", args=[A("pk")])
            age  = tables.LinkColumn("people_detail", text=lambda record: record.name, args=[A("pk")])

    In the first example, a static text would be rendered (``"static text"``)
    In the second example, you can specify a callable which accepts a record object (and thus
    can return anything from it)

    In addition to *attrs* keys supported by `.Column`, the following are
    available:

    - `a` -- ``<a>`` elements in ``<td>``.

    Adding attributes to the ``<a>``-tag looks like this::

        class PeopleTable(tables.Table):
            first_name = tables.LinkColumn(attrs={
                "a": {"style": "color: red;"}
            })

    """

    def __init__(
        self,
        viewname=None,
        urlconf=None,
        args=None,
        kwargs=None,
        current_app=None,
        attrs=None,
        **extra,
    ):
        super().__init__(
            attrs=attrs,
            linkify=dict(
                viewname=viewname,
                urlconf=urlconf,
                args=args,
                kwargs=kwargs,
                current_app=current_app,
            ),
            **extra,
        )


@library.register
class RelatedLinkColumn(LinkColumn):
    """
    Render a link to a related object using related object's ``get_absolute_url``,
    same parameters as ``~.LinkColumn``.

    .. note ::

        This column should not be used anymore, the `linkify` keyword argument to
        regular columns can be used achieve the same results.

    If the related object does not have a method called ``get_absolute_url``,
    or if it is not callable, the link will be rendered as '#'.

    Traversing relations is also supported, suppose a Person has a foreign key to
    Country which in turn has a foreign key to Continent::

        class PersonTable(tables.Table):
            name = tables.Column()
            country = tables.RelatedLinkColumn()
            continent = tables.RelatedLinkColumn(accessor="country.continent")

    will render:

     - in column 'country', link to ``person.country.get_absolute_url()`` with the output of
       ``str(person.country)`` as ``<a>`` contents.
     - in column 'continent', a link to ``person.country.continent.get_absolute_url()`` with
       the output of ``str(person.country.continent)`` as ``<a>`` contents.

    Alternative contents of ``<a>`` can be supplied using the ``text`` keyword argument as
    documented for `~.columns.LinkColumn`.
    """
