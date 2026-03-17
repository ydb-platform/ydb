import re

from django import template
from django.conf import settings
from django.template import TemplateSyntaxError
from django.utils import six
from django.template.base import FilterExpression
from django.template.defaulttags import URLNode
from django.utils.encoding import iri_to_uri, smart_str
from django.urls import set_urlconf, get_urlconf

from ..resolvers import reverse_host, get_host
from ..utils import normalize_scheme, normalize_port

register = template.Library()

kwarg_re = re.compile(r"(?:(\w+)=)?(.+)")


class HostURLNode(URLNode):

    def __init__(self, *args, **kwargs):
        self.host = kwargs.pop('host')
        self.host_args = kwargs.pop('host_args')
        self.host_kwargs = kwargs.pop('host_kwargs')
        self.scheme = kwargs.pop('scheme')
        self.port = kwargs.pop('port')
        super(HostURLNode, self).__init__(*args, **kwargs)

    def maybe_resolve(self, var, context):
        """
        Variable may have already been resolved
        in e.g. a LoopNode, so we only resolve()
        if needed.
        """
        if isinstance(var, FilterExpression):
            return var.resolve(context)
        return var

    def render(self, context):
        host = get_host(self.maybe_resolve(self.host, context))
        current_urlconf = get_urlconf()
        try:
            set_urlconf(host.urlconf)
            path = super(HostURLNode, self).render(context)
            if self.asvar:
                path = context[self.asvar]
        finally:
            set_urlconf(current_urlconf)

        host_args = [self.maybe_resolve(x, context) for x in self.host_args]

        host_kwargs = dict((smart_str(k, 'ascii'),
                            self.maybe_resolve(v, context))
                           for k, v in six.iteritems(self.host_kwargs))

        if self.scheme:
            scheme = normalize_scheme(self.maybe_resolve(self.scheme, context))
        else:
            scheme = host.scheme

        if self.port:
            port = normalize_port(self.maybe_resolve(self.port, context))
        else:
            port = host.port

        hostname = reverse_host(host, args=host_args, kwargs=host_kwargs)

        uri = iri_to_uri('%s%s%s%s' % (scheme, hostname, port, path))

        if self.asvar:
            context[self.asvar] = uri
            return ''
        else:
            return uri


def parse_params(name, parser, bits):
    args = []
    kwargs = {}
    for bit in bits:
        match = kwarg_re.match(bit)
        if not match:
            raise TemplateSyntaxError("Malformed arguments to %s tag" % name)
        name, value = match.groups()
        if name:
            kwargs[name] = parser.compile_filter(value)
        else:
            args.append(parser.compile_filter(value))
    return args, kwargs


def fetch_arg(name, arg, bits, consume=True):
    try:
        pivot = bits.index(arg)
        try:
            value = bits[pivot + 1]
        except IndexError:
            raise TemplateSyntaxError("'%s' arguments must include "
                                      "a variable name after '%s'" %
                                      (name, arg))
        else:
            if consume:
                del bits[pivot:pivot + 2]
            return value, pivot, bits
    except ValueError:
        return None, None, bits


@register.tag
def host_url(parser, token):
    """
    Simple tag to reverse the URL inclusing a host.

    {% host_url 'view-name' host 'host-name'  %}
    {% host_url 'view-name' host 'host-name' 'spam' %}
    {% host_url 'view-name' host 'host-name' scheme 'https' %}
    {% host_url 'view-name' host 'host-name' as url_on_host_variable %}
    {% host_url 'view-name' varg1=vvalue1 host 'host-name' 'spam' 'hvalue1' %}
    {% host_url 'view-name' vvalue2 host 'host-name' 'spam' harg2=hvalue2 %}
    """
    bits = token.split_contents()
    name = bits[0]
    if len(bits) < 2:
        raise TemplateSyntaxError("'%s' takes at least one argument"
                                  " (path to a view)" % name)

    view_name = parser.compile_filter(bits[1])
    asvar, pivot, bits = fetch_arg(name, 'as', bits[1:])  # Strip off viewname
    scheme, pivot, bits = fetch_arg(name, 'scheme', bits)
    if scheme:
        scheme = parser.compile_filter(scheme)
    port, pivot, bits = fetch_arg(name, 'port', bits)
    if port:
        port = parser.compile_filter(port)

    host, pivot, bits = fetch_arg(name, 'host', bits, consume=False)

    if host:
        host = parser.compile_filter(host)
        view_args, view_kwargs = parse_params(name, parser, bits[1:pivot])
        host_args, host_kwargs = parse_params(name, parser, bits[pivot + 2:])
    else:
        # No host was given so use the default host
        host = settings.DEFAULT_HOST
        view_args, view_kwargs = parse_params(name, parser, bits[1:])
        host_args, host_kwargs = (), {}

    return HostURLNode(view_name=view_name, args=view_args, kwargs=view_kwargs,
                       asvar=asvar, host=host, host_args=host_args,
                       host_kwargs=host_kwargs, scheme=scheme, port=port)
