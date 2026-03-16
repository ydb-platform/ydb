from django.conf import settings
from django.shortcuts import get_object_or_404
from django.utils.functional import LazyObject

from .resolvers import reverse_host

HOST_SITE_TIMEOUT = getattr(settings, 'HOST_SITE_TIMEOUT', 3600)


class LazySite(LazyObject):

    def __init__(self, request, *args, **kwargs):
        super(LazySite, self).__init__()
        self.__dict__.update({
            'name': request.host.name,
            'args': args,
            'kwargs': kwargs,
        })

    def _setup(self):
        host = reverse_host(self.name, args=self.args, kwargs=self.kwargs)
        from django.contrib.sites.models import Site
        site = get_object_or_404(Site, domain__iexact=host)
        self._wrapped = site


class CachedLazySite(LazySite):

    def _setup(self):
        host = reverse_host(self.name, args=self.args, kwargs=self.kwargs)
        cache_key = "hosts:%s" % host
        from django.core.cache import cache
        site = cache.get(cache_key, None)
        if site is not None:
            self._wrapped = site
            return
        from django.contrib.sites.models import Site
        site = get_object_or_404(Site, domain__iexact=host)
        cache.set(cache_key, site, HOST_SITE_TIMEOUT)
        self._wrapped = site


def host_site(request, *args, **kwargs):
    """
    A callback function which uses the :mod:`django.contrib.sites` contrib
    app included in Django to match a host to a
    :class:`~django.contrib.sites.models.Site` instance, setting a
    ``request.site`` attribute on success.

    :param request: the request object passed from the middleware
    :param \*args: the parameters as matched by the host patterns
    :param \*\*kwargs: the keyed parameters as matched by the host patterns

    It's important to note that this uses
    :func:`~django_hosts.resolvers.reverse_host` behind the scenes to
    reverse the host with the given arguments and keyed arguments to
    enable a flexible configuration of what will be used to retrieve
    the :class:`~django.contrib.sites.models.Site` instance -- in the end
    the callback will use a ``domain__iexact`` lookup to get it.

    For example, imagine a host conf with a username parameter::

        from django.conf import settings
        from django_hosts import patterns, host

        settings.PARENT_HOST = 'example.com'

        host_patterns = patterns('',
            host(r'www', settings.ROOT_URLCONF, name='www'),
            host(r'(?P<username>\w+)', 'path.to.custom_urls',
                 callback='django_hosts.callbacks.host_site',
                 name='user-sites'),
        )

    When requesting this website with the host ``jezdez.example.com``,
    the callback will act as if you'd do::

        request.site = Site.objects.get(domain__iexact='jezdez.example.com')

    ..since the result of calling :func:`~django_hosts.resolvers.reverse_host`
    with the username ``'jezdez'`` is ``'jezdez.example.com'``.

    Later, in your views, you can nicely refer to the current site
    as ``request.site`` for further site-specific functionality.
    """
    request.site = LazySite(request, *args, **kwargs)


def cached_host_site(request, *args, **kwargs):
    """
    A callback function similar to :func:`~django_hosts.callbacks.host_site`
    which caches the resulting :class:`~django.contrib.sites.models.Site`
    instance in the default cache backend for the time specfified as
    :attr:`~django.conf.settings.HOST_SITE_TIMEOUT`.

    :param request: the request object passed from the middleware
    :param \*args: the parameters as matched by the host patterns
    :param \*\*kwargs: the keyed parameters as matched by the host patterns
    """
    request.site = CachedLazySite(request, *args, **kwargs)
