import logging
import pprint

from django.conf import settings
from django.contrib.staticfiles.storage import staticfiles_storage
from django.core.cache import cache
from django.core.cache.utils import make_template_fragment_key
from django.urls import NoReverseMatch
from django.urls import reverse
from django.utils.encoding import force_str
from jinja2.nodes import ContextReference
from jinja2 import TemplateSyntaxError
from jinja2 import pass_context
from jinja2 import nodes
from jinja2.ext import Extension
from markupsafe import Markup


JINJA2_MUTE_URLRESOLVE_EXCEPTIONS = getattr(settings, "JINJA2_MUTE_URLRESOLVE_EXCEPTIONS", False)
logger = logging.getLogger(__name__)


class CsrfExtension(Extension):
    tags = {'csrf_token'}

    def __init__(self, environment):
        self.environment = environment

    def parse(self, parser):
        lineno = parser.stream.expect('name:csrf_token').lineno
        call = self.call_method(
            '_render',
            [nodes.Name('csrf_token', 'load', lineno=lineno)],
            lineno=lineno
        )
        return nodes.Output([nodes.MarkSafe(call)])

    def _render(self, csrf_token):
        if csrf_token:
            if csrf_token == 'NOTPROVIDED':
                return Markup("")

            return Markup(f"<input type='hidden' name='csrfmiddlewaretoken' value='{csrf_token}' />")

        if settings.DEBUG:
            import warnings
            warnings.warn("A {% csrf_token %} was used in a template, but the context "
                          "did not provide the value.  This is usually caused by not "
                          "using RequestContext.")
        return ''


class CacheExtension(Extension):
    """
    Exactly like Django's own tag, but supports full Jinja2
    expressiveness for all arguments.

        {% cache gettimeout()*2 "foo"+options.cachename  %}
            ...
        {% endcache %}

    General Syntax:

        {% cache [expire_time] [fragment_name] [var1] [var2] .. %}
            .. some expensive processing ..
        {% endcache %}

    Available by default (does not need to be loaded).

    Partly based on the ``FragmentCacheExtension`` from the Jinja2 docs.
    """

    tags = {'cache'}

    def parse(self, parser):
        lineno = next(parser.stream).lineno

        expire_time = parser.parse_expression()
        fragment_name = parser.parse_expression()
        vary_on = []

        while not parser.stream.current.test('block_end'):
            vary_on.append(parser.parse_expression())

        body = parser.parse_statements(['name:endcache'], drop_needle=True)

        return nodes.CallBlock(
            self.call_method('_cache_support',
                             [expire_time, fragment_name,
                              nodes.List(vary_on), nodes.Const(lineno)]),
            [], [], body).set_lineno(lineno)

    def _cache_support(self, expire_time, fragm_name, vary_on, lineno, caller):
        try:
            if expire_time is not None:
                expire_time = int(expire_time)
        except (ValueError, TypeError):
            raise TemplateSyntaxError(
                f'"{list(self.tags)[0]}" tag got a non-integer timeout value: {expire_time!r}',
                lineno,
            )

        cache_key = make_template_fragment_key(fragm_name, vary_on)

        value = cache.get(cache_key)
        if value is None:
            value = caller()
            cache.set(cache_key, force_str(value), expire_time)
        else:
            value = force_str(value)

        return value


class DebugExtension(Extension):
    """
    A ``{% debug %}`` tag that dumps the available variables, filters and tests.
    Typical usage like this:

    .. codeblock:: html+jinja
        <pre>{% debug %}</pre>

    produces output like this:

    ::
        {'context': {'_': <function _gettext_alias at 0x7f9ceabde488>,
                 'csrf_token': <SimpleLazyObject: 'lfPE7al...q3bykS4txKfb3'>,
                 'cycler': <class 'jinja2.utils.Cycler'>,
                 ...
                 'view': <polls.views_auth.Login object at 0x7f9cea2cbe48>},
        'filters': ['abs', 'add', 'addslashes', 'attr', 'batch', 'bootstrap',
                 'bootstrap_classes', 'bootstrap_horizontal',
                 'bootstrap_inline', ... 'yesno'],
        'tests': ['callable', 'checkbox_field', 'defined', 'divisibleby',
               'escaped', 'even', 'iterable', 'lower', 'mapping',
               'multiple_checkbox_field', ... 'string', 'undefined', 'upper']}

    """
    tags = {'debug'}

    def __init__(self, environment):
        super().__init__(environment)

    def parse(self, parser):
        lineno = parser.stream.expect('name:debug').lineno
        context = ContextReference()
        call = self.call_method('_render', [context], lineno=lineno)
        return nodes.Output([nodes.MarkSafe(call)])

    def _render(self, context):
        result = {
            'filters': sorted(self.environment.filters.keys()),
            'tests': sorted(self.environment.tests.keys()),
            'context': context.get_all()
        }
        #
        # We set the depth since the intent is basically to show the top few
        # names. TODO: provide user control over this?
        #
        text = pprint.pformat(result, depth=3, compact=True)
        return Markup.escape(text)


class StaticFilesExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.globals["static"] = self._static

    def _static(self, path):
        return staticfiles_storage.url(path)


class UrlsExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.globals["url"] = self._url_reverse

    @pass_context
    def _url_reverse(self, context, name, *args, **kwargs):
        try:
            current_app = context["request"].current_app
        except AttributeError:
            try:
                current_app = context["request"].resolver_match.namespace
            except AttributeError:
                current_app = None
        except KeyError:
            current_app = None
        try:
            return reverse(name, args=args, kwargs=kwargs, current_app=current_app)
        except NoReverseMatch as exc:
            logger.error('Error: %s', exc)
            if not JINJA2_MUTE_URLRESOLVE_EXCEPTIONS:
                raise
            return ''
        return reverse(name, args=args, kwargs=kwargs)


from . import filters

class TimezoneExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.globals["utc"] = filters.utc
        environment.globals["timezone"] = filters.timezone
        environment.globals["localtime"] = filters.localtime


class DjangoFiltersExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["static"] = filters.static
        environment.filters["reverseurl"] = filters.reverse
        environment.filters["addslashes"] = filters.addslashes
        environment.filters["capfirst"] = filters.capfirst
        environment.filters["escapejs"] = filters.escapejs_filter
        environment.filters["floatformat"] = filters.floatformat
        environment.filters["iriencode"] = filters.iriencode
        environment.filters["linenumbers"] = filters.linenumbers
        environment.filters["make_list"] = filters.make_list
        environment.filters["slugify"] = filters.slugify
        environment.filters["stringformat"] = filters.stringformat
        environment.filters["truncatechars"] = filters.truncatechars
        environment.filters["truncatechars_html"] = filters.truncatechars_html
        environment.filters["truncatewords"] = filters.truncatewords
        environment.filters["truncatewords_html"] = filters.truncatewords_html
        environment.filters["urlizetrunc"] = filters.urlizetrunc
        environment.filters["ljust"] = filters.ljust
        environment.filters["rjust"] = filters.rjust
        environment.filters["cut"] = filters.cut
        environment.filters["linebreaksbr"] = filters.linebreaksbr
        environment.filters["linebreaks"] = filters.linebreaks_filter
        environment.filters["striptags"] = filters.striptags
        environment.filters["add"] = filters.add
        environment.filters["date"] = filters.date
        environment.filters["time"] = filters.time
        environment.filters["timesince"] = filters.timesince_filter
        environment.filters["timeuntil"] = filters.timeuntil_filter
        environment.filters["default_if_none"] = filters.default_if_none
        environment.filters["divisibleby"] = filters.divisibleby
        environment.filters["yesno"] = filters.yesno
        environment.filters["pluralize"] = filters.pluralize
        environment.filters["localtime"] = filters.localtime
        environment.filters["utc"] = filters.utc
        environment.filters["timezone"] = filters.timezone
        environment.filters["json_script"] = filters.json_script


class DjangoExtraFiltersExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["title"] = filters.title
        environment.filters["upper"] = filters.upper
        environment.filters["lower"] = filters.lower
        environment.filters["urlencode"] = filters.urlencode
        environment.filters["urlize"] = filters.urlize
        environment.filters["wordcount"] = filters.wordcount
        environment.filters["wordwrap"] = filters.wordwrap
        environment.filters["center"] = filters.center
        environment.filters["join"] = filters.join
        environment.filters["length"] = filters.length
        environment.filters["random"] = filters.random
        environment.filters["default"] = filters.default
        environment.filters["filesizeformat"] = filters.filesizeformat
        environment.filters["pprint"] = filters.pprint
