# -*- coding: utf-8 -*-
"""
    flask_caching.jinja2ext
    ~~~~~~~~~~~~~~~~~~~~~~~

    Jinja2 extension that adds support for caching template fragments.

    Usage::

        {% cache timeout key1[, [key2, ...]] %}
        ...
        {% endcache %}

    By default, the value of "path to template file" + "block start line"
    is used as the cache key. Also, the key name can be set manually.
    Keys are concatenated together into a single string, that can be used
    to avoid the same block evaluating in different templates.

    Set the timeout to ``None`` for no timeout, but with custom keys::

        {% cache None "key" %}
        ...
        {% endcache %}

    Set timeout to ``del`` to delete cached value::

        {% cache 'del' key1 %}
        ...
        {% endcache %}

    Considering we have ``render_form_field`` and ``render_submit`` macros::

        {% cache 60*5 'myform' %}
        <div>
            <form>
            {% render_form_field(form.username) %}
            {% render_submit() %}
            </form>
        </div>
        {% endcache %}

    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""
from jinja2 import nodes
from jinja2.ext import Extension

from flask_caching import make_template_fragment_key

JINJA_CACHE_ATTR_NAME = "_template_fragment_cache"


class CacheExtension(Extension):
    tags = set(["cache"])

    def parse(self, parser):
        lineno = next(parser.stream).lineno

        #: Parse timeout
        args = [parser.parse_expression()]

        #: Parse fragment name
        #: Grab the fragment name if it exists
        #: otherwise, default to the old method of using the templates
        #: lineno to maintain backwards compatibility.
        if parser.stream.skip_if("comma"):
            args.append(parser.parse_expression())
        else:
            args.append(nodes.Const("%s%s" % (parser.filename, lineno)))

        #: Parse vary_on parameters
        vary_on = []
        while parser.stream.skip_if("comma"):
            vary_on.append(parser.parse_expression())

        if vary_on:
            args.append(nodes.List(vary_on))
        else:
            args.append(nodes.Const([]))

        body = parser.parse_statements(["name:endcache"], drop_needle=True)
        return nodes.CallBlock(
            self.call_method("_cache", args), [], [], body
        ).set_lineno(lineno)

    def _cache(self, timeout, fragment_name, vary_on, caller):
        try:
            cache = getattr(self.environment, JINJA_CACHE_ATTR_NAME)
        except AttributeError as e:
            raise e

        key = make_template_fragment_key(fragment_name, vary_on=vary_on)

        #: Delete key if timeout is 'del'
        if timeout == "del":
            cache.delete(key)
            return caller()

        rv = cache.get(key)
        if rv is None:
            rv = caller()
            cache.set(key, rv, timeout)
        return rv
