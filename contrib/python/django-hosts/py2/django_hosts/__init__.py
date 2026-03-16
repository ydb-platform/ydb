# flake8: noqa
import pkg_resources

try:  # pragma: no cover
    from django_hosts.defaults import patterns, host
    from django_hosts.resolvers import (reverse, reverse_lazy,
                                        reverse_host, reverse_host_lazy)
except ImportError:  # pragma: no cover
    pass

__version__ = pkg_resources.get_distribution('django-hosts').version
__author__ = 'Jazzband members (https://jazzband.co/)'

default_app_config = 'django_hosts.apps.HostsConfig'
