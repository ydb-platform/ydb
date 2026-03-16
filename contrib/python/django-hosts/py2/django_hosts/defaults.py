"""
When defining hostconfs you need to use the ``patterns`` and ``host`` helpers
"""
import re
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ViewDoesNotExist
from django.urls import (
    get_callable as actual_get_callable, get_mod_func,
)
from django.utils.encoding import smart_str
from django.utils.functional import cached_property

from .utils import normalize_scheme, normalize_port


def get_callable(lookup_view):
    """
    Convert a string version of a function name to the callable object.

    If the lookup_view is not an import path, it is assumed to be a URL pattern
    label and the original string is returned.
    """
    try:
        return actual_get_callable(lookup_view)
    except ViewDoesNotExist as exc:
        raise ImproperlyConfigured(exc.args[0].replace('View', 'Callable'))


def patterns(prefix, *args):
    """
    The function to define the list of hosts (aka hostconfs), e.g.::

        from django_hosts import patterns

        host_patterns = patterns('path.to',
            (r'www', 'urls.default', 'default'),
            (r'api', 'urls.api', 'api'),
        )

    :param prefix: the URLconf prefix to pass to the host object
    :type prefix: str
    :param \*args: a list of :class:`~django_hosts.defaults.hosts` instances
                   or an iterable thereof
    """
    hosts = []
    for arg in args:
        if isinstance(arg, (list, tuple)):
            arg = host(prefix=prefix, *arg)
        else:
            arg.add_prefix(prefix)
        name = arg.name
        if name in [h.name for h in hosts]:
            raise ImproperlyConfigured("Duplicate host name: %s" % name)
        hosts.append(arg)
    return hosts


class host(object):
    """
    The host object used in host conf together with the
    :func:`django_hosts.defaults.patterns` function, e.g.::

        from django_hosts import patterns, host

        host_patterns = patterns('path.to',
            host(r'www', 'urls.default', name='default'),
            host(r'api', 'urls.api', name='api'),
            host(r'admin', 'urls.admin', name='admin', scheme='https://'),
        )

    :param regex: a regular expression to be used to match the request's
                  host.
    :type regex: str
    :param urlconf: the dotted path of a URLconf module of the host
    :type urlconf: str
    :param callback: a callable or the dotted path of a callable to be used
                     when matching has happened
    :type callback: callable or str
    :param prefix: the prefix to apply to the ``urlconf`` parameter
    :type prefix: str
    :param scheme: the scheme to prepend host names with during reversing,
                   e.g. when using the host_url() template tag. Defaults to
                   :attr:`~django.conf.settings.HOST_SCHEME`.
    :param port: the port to append to host names during reversing,
                 e.g. when using the host_url() template tag. Defaults to
                 :attr:`~django.conf.settings.HOST_PORT`.
    :type scheme: str
    """
    def __init__(self, regex, urlconf, name, callback=None, prefix='',
                 scheme=None, port=None):
        """
        Compile hosts. We add a literal fullstop to the end of every
        pattern to avoid rather unwieldy escaping in every definition.
        """
        self.regex = regex
        self.compiled_regex = re.compile(r'%s(\.|$)' % regex)
        self.urlconf = urlconf
        self.name = name
        self._scheme = scheme
        self._port = port
        if callable(callback):
            self._callback = callback
        else:
            self._callback, self._callback_str = None, callback
        self.add_prefix(prefix)

    def __repr__(self):
        return smart_str('<%s %s: regex=%r urlconf=%r scheme=%r port=%r>' %
                         (self.__class__.__name__, self.name, self.regex,
                          self.urlconf, self.scheme, self.port))

    @cached_property
    def scheme(self):
        if self._scheme is None:
            self._scheme = getattr(settings, 'HOST_SCHEME', '//')
        return normalize_scheme(self._scheme)

    @cached_property
    def port(self):
        if self._port is None:
            self._port = getattr(settings, 'HOST_PORT', '')
        return normalize_port(self._port)

    @property
    def callback(self):
        if self._callback is not None:
            return self._callback
        elif self._callback_str is None:
            return lambda *args, **kwargs: None
        try:
            self._callback = get_callable(self._callback_str)
        except ImportError as exc:
            mod_name, _ = get_mod_func(self._callback_str)
            raise ImproperlyConfigured("Could not import '%s'. "
                                       "Error was: %s" %
                                       (mod_name, str(exc)))
        except AttributeError as exc:
            mod_name, func_name = get_mod_func(self._callback_str)
            raise ImproperlyConfigured("Tried importing '%s' from module "
                                       "'%s' but failed. Error was: %s" %
                                       (func_name, mod_name, str(exc)))
        return self._callback

    def add_prefix(self, prefix=''):
        """
        Adds the prefix string to a string-based urlconf.
        """
        if prefix:
            self.urlconf = prefix.rstrip('.') + '.' + self.urlconf
