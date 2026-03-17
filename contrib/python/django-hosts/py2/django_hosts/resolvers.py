"""
If you want to reverse the hostname or the full URL or a view including the
scheme, hostname and port you'll need to use the ``reverse`` and
``reverse_host`` helper functions (or its lazy cousins).
"""
import re

from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.urls import NoReverseMatch, reverse as reverse_path
from django.utils import six
from django.utils.encoding import iri_to_uri, force_text
from django.utils.functional import lazy
from django.utils.lru_cache import lru_cache
from django.utils.regex_helper import normalize

from .defaults import host as host_cls
from .utils import normalize_scheme, normalize_port


@lru_cache()
def get_hostconf():
    try:
        return settings.ROOT_HOSTCONF
    except AttributeError:
        raise ImproperlyConfigured("Missing ROOT_HOSTCONF setting")


@lru_cache()
def get_hostconf_module(hostconf=None):
    if hostconf is None:
        hostconf = get_hostconf()
    return import_module(hostconf)


@lru_cache()
def get_host(name=None):
    if name is None:
        try:
            name = settings.DEFAULT_HOST
        except AttributeError:
            raise ImproperlyConfigured("Missing DEFAULT_HOST setting")
    for host in get_host_patterns():
        if host.name == name:
            return host
    raise NoReverseMatch("No host called '%s' exists" % name)


@lru_cache()
def get_host_patterns():
    hostconf = get_hostconf()
    module = get_hostconf_module(hostconf)
    try:
        return module.host_patterns
    except AttributeError:
        raise ImproperlyConfigured("Missing host_patterns in '%s'" % hostconf)


def clear_host_caches():
    get_hostconf.cache_clear()
    get_hostconf_module.cache_clear()
    get_host.cache_clear()
    get_host_patterns.cache_clear()


def setting_changed_receiver(setting, enter, **kwargs):
    if setting in {'ROOT_HOSTCONF', 'DEFAULT_HOST'}:
        clear_host_caches()


setting_changed.connect(setting_changed_receiver)


def reverse_host(host, args=None, kwargs=None):
    """
    Given the host name and the appropriate parameters,
    reverses the host, e.g.::

        >>> from django.conf import settings
        >>> settings.ROOT_HOSTCONF = 'mysite.hosts'
        >>> settings.PARENT_HOST = 'example.com'
        >>> from django_hosts.resolvers import reverse_host
        >>> reverse_host('with_username', args=('jezdez',))
        'jezdez.example.com'

    :param name: the name of the host as specified in the hostconf
    :param args: the host arguments to use to find a matching entry in the
                 hostconf
    :param kwargs: similar to args but key value arguments
    :raises django.core.urlresolvers.NoReverseMatch: if no host matches
    :rtype: reversed hostname
    """
    if args and kwargs:
        raise ValueError("Don't mix *args and **kwargs in call to reverse()!")

    args = args or ()
    kwargs = kwargs or {}

    if not isinstance(host, host_cls):
        host = get_host(host)

    unicode_args = [force_text(x) for x in args]
    unicode_kwargs = dict(((k, force_text(v))
                          for (k, v) in six.iteritems(kwargs)))

    for result, params in normalize(host.regex):
        if args:
            if len(args) != len(params):
                continue
            candidate = result % dict(zip(params, unicode_args))
        else:
            if set(kwargs.keys()) != set(params):
                continue
            candidate = result % unicode_kwargs

        if re.match(host.regex, candidate, re.UNICODE):  # pragma: no cover
            parent_host = getattr(settings, 'PARENT_HOST', '').lstrip('.')
            if parent_host:
                # only add the parent host when needed (aka www-less domain)
                if candidate and candidate != parent_host:
                    candidate = '%s.%s' % (candidate, parent_host)
                else:
                    candidate = parent_host
            return candidate

    raise NoReverseMatch("Reverse host for '%s' with arguments '%s' "
                         "and keyword arguments '%s' not found." %
                         (host.name, args, kwargs))


#: The lazy version of the :func:`~django_hosts.resolvers.reverse_host`
#: function to be used in class based views and other module level situations
reverse_host_lazy = lazy(reverse_host, str)


def reverse(viewname, args=None, kwargs=None, prefix=None, current_app=None,
            host=None, host_args=None, host_kwargs=None,
            scheme=None, port=None):
    """
    Given the host and view name and the appropriate parameters,
    reverses the fully qualified URL, e.g.::

        >>> from django.conf import settings
        >>> settings.ROOT_HOSTCONF = 'mysite.hosts'
        >>> settings.PARENT_HOST = 'example.com'
        >>> from django_hosts.resolvers import reverse
        >>> reverse('about')
        '//www.example.com/about/'
        >>> reverse('about', host='www')
        '//www.example.com/about/'
        >>> reverse('repo', args=('jezdez',), host='www', scheme='git', port=1337)
        'git://jezdez.example.com:1337/repo/'

    You can set the used port and scheme in the host object or override with
    the paramater named accordingly.

    The host name can be left empty to automatically fall back to the default
    hostname as defined in the :attr:`~django.conf.settings.DEFAULT_HOST`
    setting.

    :param viewname: the name of the view
    :param args: the arguments of the view
    :param kwargs: the keyed arguments of the view
    :param prefix: the prefix of the view urlconf
    :param current_app: the current_app argument
    :param scheme: the scheme to use
    :param port: the port to use
    :param host: the name of the host
    :param host_args: the host arguments
    :param host_kwargs: the host keyed arguments
    :raises django.core.urlresolvers.NoReverseMatch: if no host or path matches
    :rtype: the fully qualified URL with path
    """
    host = get_host(host)
    hostname = reverse_host(host,
                            args=host_args,
                            kwargs=host_kwargs)
    path = reverse_path(
        viewname,
        urlconf=host.urlconf,
        args=args or (),
        kwargs=kwargs or {},
        current_app=current_app,
    )
    if scheme is None:
        scheme = host.scheme
    else:
        scheme = normalize_scheme(scheme)

    if port is None:
        port = host.port
    else:
        port = normalize_port(port)

    return iri_to_uri('%s%s%s%s' % (scheme, hostname, port, path))


#: The lazy version of the :func:`~django_hosts.resolvers.reverse`
#: function to be used in class based views and other module level situations
reverse_lazy = lazy(reverse, str)
