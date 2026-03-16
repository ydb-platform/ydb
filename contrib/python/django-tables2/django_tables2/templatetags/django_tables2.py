import re
from collections import OrderedDict

from django import template
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.template import Node, TemplateSyntaxError
from django.template.loader import get_template, select_template
from django.templatetags.l10n import register as l10n_register
from django.utils.html import escape
from django.utils.http import urlencode

import django_tables2 as tables
from django_tables2.paginators import LazyPaginator
from django_tables2.utils import AttributeDict

register = template.Library()
kwarg_re = re.compile(r"(?:(.+)=)?(.+)")
context_processor_error_msg = (
    "Tag {%% %s %%} requires django.template.context_processors.request to be "
    "in the template configuration in "
    "settings.TEMPLATES[]OPTIONS.context_processors) in order for the included "
    "template tags to function correctly."
)


def token_kwargs(bits, parser):
    """
    Based on Django's `~django.template.defaulttags.token_kwargs`, but with a few changes.

    - No legacy mode.
    - Both keys and values are compiled as a filter
    """
    if not bits:
        return {}
    kwargs = OrderedDict()
    while bits:
        match = kwarg_re.match(bits[0])
        if not match or not match.group(1):
            return kwargs
        key, value = match.groups()
        del bits[:1]
        kwargs[parser.compile_filter(key)] = parser.compile_filter(value)
    return kwargs


class QuerystringNode(Node):
    def __init__(self, updates, removals, asvar=None):
        super().__init__()
        self.updates = updates
        self.removals = removals
        self.asvar = asvar

    def render(self, context):
        if "request" not in context:
            raise ImproperlyConfigured(context_processor_error_msg % "querystring")

        params = dict(context["request"].GET)
        for key, value in self.updates.items():
            if isinstance(key, str):
                params[key] = value
                continue
            key = key.resolve(context)
            value = value.resolve(context)
            if key not in ("", None):
                params[key] = value
        for removal in self.removals:
            params.pop(removal.resolve(context), None)

        value = escape("?" + urlencode(params, doseq=True))

        if self.asvar:
            context[str(self.asvar)] = value
            return ""
        else:
            return value


# {% querystring "name"="abc" "age"=15 as=qs %}
@register.tag
def querystring(parser, token):
    """
    Create an URL (containing only the query string [including "?"]) derivedfrom the current URL's query string.

    By updating it with the provided keyword arguments.

    Example (imagine URL is ``/abc/?gender=male&name=Brad``)::

        # {% querystring "name"="abc" "age"=15 %}
        ?name=abc&gender=male&age=15
        {% querystring "name"="Ayers" "age"=20 %}
        ?name=Ayers&gender=male&age=20
        {% querystring "name"="Ayers" without "gender" %}
        ?name=Ayers
    """
    bits = token.split_contents()
    tag = bits.pop(0)
    updates = token_kwargs(bits, parser)

    asvar_key = None
    for key in updates:
        if str(key) == "as":
            asvar_key = key

    if asvar_key is not None:
        asvar = updates[asvar_key]
        del updates[asvar_key]
    else:
        asvar = None

    # ``bits`` should now be empty of a=b pairs, it should either be empty, or
    # have ``without`` arguments.
    if bits and bits.pop(0) != "without":
        raise TemplateSyntaxError(f"Malformed arguments to '{tag}'")
    removals = [parser.compile_filter(bit) for bit in bits]
    return QuerystringNode(updates, removals, asvar=asvar)


class RenderTableNode(Node):
    """
    Node to render a table.

    Parameters:
        table (~.Table): the table to render
        template (str or list): Name[s] of template to render

    """

    def __init__(self, table, template_name=None):
        super().__init__()
        self.table = table
        self.template_name = template_name

    def render(self, context):
        table = self.table.resolve(context)

        request = context.get("request")

        if isinstance(table, tables.Table):
            pass
        elif hasattr(table, "model"):
            queryset = table

            table = tables.table_factory(model=queryset.model)(queryset, request=request)
        else:
            raise ValueError(f"Expected table or queryset, not {type(table).__name__}")

        if self.template_name:
            template_name = self.template_name.resolve(context)
        else:
            template_name = table.template_name

        if isinstance(template_name, str):
            template = get_template(template_name)
        else:
            # assume some iterable was given
            template = select_template(template_name)

        try:
            # HACK:
            # TemplateColumn benefits from being able to use the context
            # that the table is rendered in. The current way this is
            # achieved is to temporarily attach the context to the table,
            # which TemplateColumn then looks for and uses.
            table.context = context
            table.before_render(request)

            return template.render(context={"table": table}, request=request)
        finally:
            del table.context


@register.tag
def render_table(parser, token):
    """
    Render a HTML table.

    The tag can be given either a `.Table` object, or a queryset. An optional
    second argument can specify the template to use.

    Example::

        {% render_table table %}
        {% render_table table "custom.html" %}
        {% render_table user_queryset %}

    When given a queryset, a `.Table` class is generated dynamically as
    follows::

        class OnTheFlyTable(tables.Table):
            class Meta:
                model = queryset.model
                attrs = {'class': 'paleblue'}

    For configuration beyond this, a `.Table` class must be manually defined,
    instantiated, and passed to this tag.

    The context should include a *request* variable containing the current
    request. This allows pagination URLs to be created without clobbering the
    existing querystring.
    """
    bits = token.split_contents()
    bits.pop(0)

    table = parser.compile_filter(bits.pop(0))
    template = parser.compile_filter(bits.pop(0)) if bits else None

    return RenderTableNode(table, template)


register.filter("localize", l10n_register.filters["localize"])
register.filter("unlocalize", l10n_register.filters["unlocalize"])


@register.simple_tag(takes_context=True)
def export_url(context, export_format, export_trigger_param=None):
    """
    Return an export URL for the given file `export_format`, preserving current query string parameters.

    Example for a page requested with querystring ``?q=blue``::

        {% export_url "csv" %}

    It will return::

        ?q=blue&amp;_export=csv
    """
    if export_trigger_param is None and "view" in context:
        export_trigger_param = getattr(context["view"], "export_trigger_param", None)

    export_trigger_param = export_trigger_param or "_export"

    return QuerystringNode(updates={export_trigger_param: export_format}, removals=[]).render(
        context
    )


@register.filter
def table_page_range(page, paginator):
    """
    Given an page and paginator, return a list of max 10 (by default) page numbers.

     - always containing the first, last and current page.
     - containing one or two '...' to skip ranges between first/last and current.

    Example:
        {% for p in table.page|table_page_range:table.paginator %}
            {{ p }}
        {% endfor %}
    """
    page_range = getattr(settings, "DJANGO_TABLES2_PAGE_RANGE", 10)

    num_pages = paginator.num_pages
    if num_pages <= page_range:
        return range(1, num_pages + 1)

    range_start = page.number - int(page_range / 2)
    if range_start < 1:
        range_start = 1
    range_end = range_start + page_range
    if range_end > num_pages:
        range_start = num_pages - page_range + 1
        range_end = num_pages + 1

    ret = range(range_start, range_end)
    if 1 not in ret:
        ret = [1, "..."] + list(ret)[2:]
    if num_pages not in ret:
        ret = list(ret)[:-2] + ["...", num_pages]
    if isinstance(paginator, LazyPaginator) and not paginator.is_last_page(page.number):
        ret.append("...")
    return ret


@register.simple_tag
def render_attrs(attrs, **kwargs):
    ret = AttributeDict(kwargs)

    if attrs is not None:
        ret.update(attrs)

    return ret.as_html()
