#!/usr/bin/env python
# coding=utf8

import re

from flask import Blueprint, current_app, url_for

try:
    from wtforms.fields import HiddenField
except ImportError:

    def is_hidden_field_filter(field):
        raise RuntimeError('WTForms is not installed.')
else:

    def is_hidden_field_filter(field):
        return isinstance(field, HiddenField)


from .forms import render_form

__version__ = '3.3.7.1.dev1'
BOOTSTRAP_VERSION = re.sub(r'^(\d+\.\d+\.\d+).*', r'\1', __version__)
JQUERY_VERSION = '1.12.4'
HTML5SHIV_VERSION = '3.7.3'
RESPONDJS_VERSION = '1.4.2'


class CDN(object):
    """Base class for CDN objects."""

    def get_resource_url(self, filename):
        """Return resource url for filename."""
        raise NotImplementedError


class StaticCDN(object):
    """A CDN that serves content from the local application.

    :param static_endpoint: Endpoint to use.
    :param rev: If ``True``, honor ``BOOTSTRAP_QUERYSTRING_REVVING``.
    """

    def __init__(self, static_endpoint='static', rev=False):
        self.static_endpoint = static_endpoint
        self.rev = rev

    def get_resource_url(self, filename):
        extra_args = {}

        if self.rev and current_app.config['BOOTSTRAP_QUERYSTRING_REVVING']:
            extra_args['bootstrap'] = __version__

        return url_for(self.static_endpoint, filename=filename, **extra_args)


class WebCDN(object):
    """Serves files from the Web.

    :param baseurl: The baseurl. Filenames are simply appended to this URL.
    """

    def __init__(self, baseurl):
        self.baseurl = baseurl

    def get_resource_url(self, filename):
        return self.baseurl + filename


class ConditionalCDN(object):
    """Serves files from one CDN or another, depending on whether a
    configuration value is set.

    :param confvar: Configuration variable to use.
    :param primary: CDN to use if the configuration variable is ``True``.
    :param fallback: CDN to use otherwise.
    """

    def __init__(self, confvar, primary, fallback):
        self.confvar = confvar
        self.primary = primary
        self.fallback = fallback

    def get_resource_url(self, filename):
        if current_app.config[self.confvar]:
            return self.primary.get_resource_url(filename)
        return self.fallback.get_resource_url(filename)


def bootstrap_find_resource(filename, cdn, use_minified=None, local=True):
    """Resource finding function, also available in templates.

    Tries to find a resource, will force SSL depending on
    ``BOOTSTRAP_CDN_FORCE_SSL`` settings.

    :param filename: File to find a URL for.
    :param cdn: Name of the CDN to use.
    :param use_minified': If set to ``True``/``False``, use/don't use
                          minified. If ``None``, honors
                          ``BOOTSTRAP_USE_MINIFIED``.
    :param local: If ``True``, uses the ``local``-CDN when
                  ``BOOTSTRAP_SERVE_LOCAL`` is enabled. If ``False``, uses
                  the ``static``-CDN instead.
    :return: A URL.
    """
    config = current_app.config

    if None == use_minified:
        use_minified = config['BOOTSTRAP_USE_MINIFIED']

    if use_minified:
        filename = '%s.min.%s' % tuple(filename.rsplit('.', 1))

    cdns = current_app.extensions['bootstrap']['cdns']
    resource_url = cdns[cdn].get_resource_url(filename)

    if resource_url.startswith('//') and config['BOOTSTRAP_CDN_FORCE_SSL']:
        resource_url = 'https:%s' % resource_url

    return resource_url


class Bootstrap(object):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('BOOTSTRAP_USE_MINIFIED', True)
        app.config.setdefault('BOOTSTRAP_CDN_FORCE_SSL', False)

        app.config.setdefault('BOOTSTRAP_QUERYSTRING_REVVING', True)
        app.config.setdefault('BOOTSTRAP_SERVE_LOCAL', False)

        app.config.setdefault('BOOTSTRAP_LOCAL_SUBDOMAIN', None)

        blueprint = Blueprint(
            'bootstrap',
            __name__,
            template_folder='templates',
            static_folder='static',
            static_url_path=app.static_url_path + '/bootstrap',
            subdomain=app.config['BOOTSTRAP_LOCAL_SUBDOMAIN'])

        # add the form rendering template filter
        blueprint.add_app_template_filter(render_form)

        app.register_blueprint(blueprint)

        app.jinja_env.globals['bootstrap_is_hidden_field'] =\
            is_hidden_field_filter
        app.jinja_env.globals['bootstrap_find_resource'] =\
            bootstrap_find_resource
        app.jinja_env.add_extension('jinja2.ext.do')

        if not hasattr(app, 'extensions'):
            app.extensions = {}

        local = StaticCDN('bootstrap.static', rev=True)
        static = StaticCDN()

        def lwrap(cdn, primary=static):
            return ConditionalCDN('BOOTSTRAP_SERVE_LOCAL', primary, cdn)

        bootstrap = lwrap(
            WebCDN('//cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/%s/' %
                   BOOTSTRAP_VERSION), local)

        jquery = lwrap(
            WebCDN('//cdnjs.cloudflare.com/ajax/libs/jquery/%s/' %
                   JQUERY_VERSION), local)

        html5shiv = lwrap(
            WebCDN('//cdnjs.cloudflare.com/ajax/libs/html5shiv/%s/' %
                   HTML5SHIV_VERSION))

        respondjs = lwrap(
            WebCDN('//cdnjs.cloudflare.com/ajax/libs/respond.js/%s/' %
                   RESPONDJS_VERSION))

        app.extensions['bootstrap'] = {
            'cdns': {
                'local': local,
                'static': static,
                'bootstrap': bootstrap,
                'jquery': jquery,
                'html5shiv': html5shiv,
                'respond.js': respondjs,
            },
        }

        # setup support for flask-nav
        renderers = app.extensions.setdefault('nav_renderers', {})
        renderer_name = (__name__ + '.nav', 'BootstrapRenderer')
        renderers['bootstrap'] = renderer_name

        # make bootstrap the default renderer
        renderers[None] = renderer_name
