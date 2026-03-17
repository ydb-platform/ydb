from django.core.cache import caches, DEFAULT_CACHE_ALIAS
from django.core.cache.utils import make_template_fragment_key
from jinja2.nodes import Keyword, Const, CallBlock
from jinja2.ext import Extension

from .api import get_last_invalidation


class CachalotExtension(Extension):
    tags = {'cache'}
    allowed_kwargs = ('cache_key', 'timeout', 'cache_alias')

    def __init__(self, environment):
        super(CachalotExtension, self).__init__(environment)

        self.environment.globals.update(
            get_last_invalidation=get_last_invalidation)

    def parse_args(self, parser):
        args = []
        kwargs = []

        stream = parser.stream

        while stream.current.type != 'block_end':
            if stream.current.type == 'name' \
                    and stream.look().type == 'assign':
                key = stream.current.value
                if key not in self.allowed_kwargs:
                    parser.fail(
                        "'%s' is not a valid keyword argument "
                        "for {%% cache %%}" % key,
                        stream.current.lineno)
                stream.skip(2)
                value = parser.parse_expression()
                kwargs.append(Keyword(key, value, lineno=value.lineno))
            else:
                args.append(parser.parse_expression())

            if stream.current.type == 'block_end':
                break

            parser.stream.expect('comma')

        return args, kwargs

    def parse(self, parser):
        tag = parser.stream.current.value
        lineno = next(parser.stream).lineno
        args, kwargs = self.parse_args(parser)
        default_cache_key = (None if parser.filename is None
                             else '%s:%d' % (parser.filename, lineno))
        kwargs.append(Keyword('default_cache_key', Const(default_cache_key),
                              lineno=lineno))
        body = parser.parse_statements(['name:end' + tag], drop_needle=True)

        return CallBlock(self.call_method('cache', args, kwargs),
                         [], [], body).set_lineno(lineno)

    def cache(self, *args, **kwargs):
        cache_alias = kwargs.get('cache_alias', DEFAULT_CACHE_ALIAS)
        cache_key = kwargs.get('cache_key', kwargs['default_cache_key'])
        if cache_key is None:
            raise ValueError(
                'You must set `cache_key` when the template is not a file.')
        cache_key = make_template_fragment_key(cache_key, args)

        out = caches[cache_alias].get(cache_key)
        if out is None:
            out = kwargs['caller']()
            caches[cache_alias].set(cache_key, out, kwargs.get('timeout'))
        return out


cachalot = CachalotExtension
