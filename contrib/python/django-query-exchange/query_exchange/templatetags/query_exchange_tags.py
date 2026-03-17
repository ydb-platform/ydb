import re
from cgi import parse_qs

from django import template
from django.utils.datastructures import MultiValueDict
from django.template.defaulttags import URLNode
from django.utils.html import escape

from query_exchange import process_query

register = template.Library()

# Regex for URL arguments including filters
url_arg_re = re.compile(
    r"(?:(%(name)s)=)?(%(value)s(?:\|%(name)s(?::%(value)s)?)*)" % {
        'name':'\w+',
        'value':'''(?:(?:'[^']*')|(?:"[^"]*")|(?:[\w\.-]+))'''},
    re.VERBOSE)


class BaseQueryNode(object):
    def render(self, context):
        url, params = self.get_url(context)

        if 'request' not in context:
            raise ValueError('`request` needed in context for GET query processing')

        query = process_query(
            params,
            self.keep and [v.resolve(context) for v in self.keep],
            self.exclude and [v.resolve(context) for v in self.exclude],
            self.add and dict([(k, v.resolve(context)) for k, v in self.add.iteritems()]),
            self.remove and dict([(k, v.resolve(context)) for k, v in self.remove.iteritems()]),
        )
        if query:
            url += '?' + query

        url = escape(url)

        if self._asvar:
            context[self._asvar] = url
            return ''
        else:
            return url

class URLWithQueryNode(BaseQueryNode, URLNode):
    def __init__(self, view_name, args, kwargs, asvar, keep, exclude, add, remove):
        super(URLWithQueryNode, self).__init__(view_name, args, kwargs, None)
        self._asvar = asvar

        self.keep = keep
        self.exclude = exclude
        self.add = add
        self.remove = remove

    def get_url(self, context):
        try:
            self.view_name = self.view_name.resolve(context)
        except AttributeError:
            pass

        return URLNode.render(self, context), context['request'].GET.copy()

class WithQueryNode(BaseQueryNode, template.Node):
    def __init__(self, url, asvar, keep, exclude, add, remove):
        self.url = url
        self._asvar = asvar

        self.keep = keep
        self.exclude = exclude
        self.add = add
        self.remove = remove

    def get_url(self, context):
        params =  context['request'].GET.copy()

        url = self.url.resolve(context)

        if '?' in url:
            url, exctra_params = url.split('?', 1)

            params.update(MultiValueDict(parse_qs(exctra_params.encode('utf-8'))))

        return url, params


class QueryNode(BaseQueryNode, template.Node):
    def __init__(self, asvar, keep, exclude, add, remove):
        self._asvar = asvar

        self.keep = keep
        self.exclude = exclude
        self.add = add
        self.remove = remove

    def get_url(self, context):
        url = self.url.resolve(context)
        if '?' in url:
            url, params = url.split('?', 1)

            return url, MultiValueDict(parse_qs(params.encode('utf-8')))

        return url, MultiValueDict()


def parse_args(parser, bit):
    end = 0
    args = []
    kwargs = {}

    for i, match in enumerate(url_arg_re.finditer(bit)):
        if (i == 0 and match.start() != 0) or \
              (i > 0 and (bit[end:match.start()] != ',')):
            raise template.TemplateSyntaxError("Malformed arguments to url tag")

        end = match.end()
        name, value = match.group(1), match.group(2)

        if name:
            kwargs[name] = parser.compile_filter(value)
        else:
            args.append(parser.compile_filter(value))

    if end != len(bit):
        raise template.TemplateSyntaxError("Malformed arguments to url tag")

    return args, kwargs

@register.tag
def url_with_query(parser, token):
    bits = token.split_contents()
    if len(bits) < 2:
        raise template.TemplateSyntaxError("'%s' takes at least one argument"
                                           " (path to a view)" % bits[0])
    viewname = parser.compile_filter(bits[1])
    args = []
    kwargs = {}
    asvar = None

    keep = None
    exclude = None
    add = None
    remove = None

    if len(bits) > 2:
        bits = iter(bits[2:])
        for bit in bits:
            if bit == 'as':
                asvar = bits.next()
                break
            elif bit == 'keep':
                keep, _ = parse_args(parser, bits.next())
            elif bit == 'exclude':
                exclude, _ = parse_args(parser, bits.next())
            elif bit == 'add':
                _, add = parse_args(parser, bits.next())
            elif bit == 'remove':
                _, remove = parse_args(parser, bits.next())
            else:
                args, kwargs = parse_args(parser, bit)

    return URLWithQueryNode(viewname, args, kwargs, asvar, keep, exclude, add, remove)


@register.tag
def with_query(parser, token):
    bits = token.split_contents()
    if len(bits) < 2:
        raise template.TemplateSyntaxError("'%s' takes at least one argument"
                                           " (url)" % bits[0])
    url = parser.compile_filter(bits[1])
    asvar = None

    keep = None
    exclude = None
    add = None
    remove = None

    if len(bits) > 2:
        bits = iter(bits[2:])
        for bit in bits:
            if bit == 'as':
                asvar = bits.next()
                break
            elif bit == 'keep':
                keep, _ = parse_args(parser, bits.next())
            elif bit == 'exclude':
                exclude, _ = parse_args(parser, bits.next())
            elif bit == 'add':
                _, add = parse_args(parser, bits.next())
            elif bit == 'remove':
                _, remove = parse_args(parser, bits.next())

    return WithQueryNode(url, asvar, keep, exclude, add, remove)


@register.tag
def query(parser, token):
    bits = token.split_contents()
    if len(bits) < 2:
        raise template.TemplateSyntaxError("'%s' takes at least some arguments" % bits[0])

    asvar = None

    keep = None
    exclude = None
    add = None
    remove = None

    bits = iter(bits[2:])

    for bit in bits:
        if bit == 'as':
            asvar = bits.next()
            break
        elif bit == 'keep':
            keep, _ = parse_args(parser, bits.next())
        elif bit == 'exclude':
            exclude, _ = parse_args(parser, bits.next())
        elif bit == 'remove':
            remove, _ = parse_args(parser, bits.next())
        elif bit == 'add':
            _, add = parse_args(parser, bits.next())

    return QueryNode(asvar, keep, exclude, add, remove)
