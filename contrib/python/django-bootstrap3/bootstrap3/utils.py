import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from django import get_version
from django.forms.utils import flatatt
from django.template import Variable, VariableDoesNotExist
from django.template.base import FilterExpression, TemplateSyntaxError, kwarg_re
from django.template.loader import get_template
from django.utils.encoding import force_str
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from bootstrap3.exceptions import BootstrapError

from .text import text_value

DJANGO_VERSION = get_version()
IS_DJANGO5 = DJANGO_VERSION >= "5"

# RegEx for quoted string
QUOTED_STRING = re.compile(r'^["\'](?P<noquotes>.+)["\']$')


def handle_var(value, context):
    """Handle template tag variable."""
    # Resolve FilterExpression and Variable immediately
    if isinstance(value, FilterExpression) or isinstance(value, Variable):
        return value.resolve(context)
    # Return quoted strings unquoted
    # http://djangosnippets.org/snippets/886
    stringval = QUOTED_STRING.search(value)
    if stringval:
        return stringval.group("noquotes")
    # Resolve variable or return string value
    try:
        return Variable(value).resolve(context)
    except VariableDoesNotExist:
        return value


def parse_token_contents(parser, token):
    """Parse template tag contents."""
    bits = token.split_contents()
    tag = bits.pop(0)
    args = []
    kwargs = {}
    asvar = None
    if len(bits) >= 2 and bits[-2] == "as":
        asvar = bits[-1]
        bits = bits[:-2]
    for bit in bits:
        match = kwarg_re.match(bit)
        if not match:
            raise TemplateSyntaxError(f'Malformed arguments to tag "{tag}"')
        name, value = match.groups()
        if name:
            kwargs[name] = parser.compile_filter(value)
        else:
            args.append(parser.compile_filter(value))
    return {"tag": tag, "args": args, "kwargs": kwargs, "asvar": asvar}


def split_css_classes(css_classes):
    """Turn string into a list of CSS classes."""
    classes_list = text_value(css_classes).split(" ")
    return [c for c in classes_list if c]


def add_css_class(css_classes, css_class, prepend=False):
    """Add a CSS class to a string of CSS classes."""
    classes_list = split_css_classes(css_classes)
    classes_to_add = [c for c in split_css_classes(css_class) if c not in classes_list]
    if prepend:
        classes_list = classes_to_add + classes_list
    else:
        classes_list += classes_to_add
    return " ".join(classes_list)


def remove_css_class(css_classes, css_class):
    """Remove a CSS class from a string of CSS classes."""
    remove = set(split_css_classes(css_class))
    classes_list = [c for c in split_css_classes(css_classes) if c not in remove]
    return " ".join(classes_list)


def render_script_tag(url):
    """Build a script tag."""
    url_dict = url_to_attrs_dict(url, url_attr="src")
    return render_tag("script", url_dict)


def render_link_tag(url, rel="stylesheet", media=None):
    """Build a link tag."""
    url_dict = url_to_attrs_dict(url, url_attr="href")
    url_dict.setdefault("href", url_dict.pop("url", None))
    url_dict["rel"] = rel
    if media:
        url_dict["media"] = media
    return render_tag("link", attrs=url_dict, close=False)


def render_tag(tag, attrs=None, content=None, close=True):
    """Render a HTML tag."""
    builder = "<{tag}{attrs}>{content}"
    if content or close:
        builder += "</{tag}>"
    return format_html(
        builder,
        tag=tag,
        attrs=mark_safe(flatatt(attrs)) if attrs else "",
        content=text_value(content),
    )


def render_template_file(template, context=None):
    """Render a Template to unicode."""
    template = get_template(template)
    return template.render(context)


def url_replace_param(url, name, value):
    """Replace a GET parameter in an URL."""
    url_components = urlparse(force_str(url))
    query_params = parse_qs(url_components.query)
    query_params[name] = value
    query = urlencode(query_params, doseq=True)
    return force_str(
        urlunparse(
            [
                url_components.scheme,
                url_components.netloc,
                url_components.path,
                url_components.params,
                query,
                url_components.fragment,
            ]
        )
    )


def url_to_attrs_dict(url, url_attr):
    """Sanitize url dict as used in django-bootstrap3 settings."""
    result = dict()
    # If url is not a string, it should be a dict
    if isinstance(url, str):
        url_value = url
    else:
        try:
            url_value = url["url"]
        except TypeError:
            raise BootstrapError('Function "url_to_attrs_dict" expects a string or a dict with key "url".')
        crossorigin = url.get("crossorigin", None)
        integrity = url.get("integrity", None)
        if crossorigin:
            result["crossorigin"] = crossorigin
        if integrity:
            result["integrity"] = integrity
    result[url_attr] = url_value
    return result
