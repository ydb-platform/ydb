from django.core.exceptions import ImproperlyConfigured
from django_hosts.defaults import host, patterns
from django_hosts.resolvers import get_host_patterns

from .base import HostsTestCase


class PatternsTests(HostsTestCase):

    def test_pattern(self):
        host_patterns = patterns('',
            host(r'api', 'api.urls', name='api'),
        )
        self.assertEqual(len(host_patterns), 1)
        self.assertTrue(isinstance(host_patterns[0], host))
        self.assertEqual(repr(host_patterns[0]),
                         "<host api: regex='api' urlconf='api.urls' "
                         "scheme='//' port=''>")

    def test_pattern_as_tuple(self):
        host_patterns = patterns('',
            (r'api', 'api.urls', 'api'),
        )
        self.assertEqual(len(host_patterns), 1)
        self.assertTrue(isinstance(host_patterns[0], host))

    def test_pattern_with_duplicate(self):
        api_host = host(r'api', 'api.urls', name='api')
        self.assertRaises(ImproperlyConfigured,
                          patterns, '', api_host, api_host)

    def test_pattern_with_prefix(self):
        host_patterns = patterns('mysite',
            host(r'api', 'api.urls', name='api'),
        )
        self.assertEqual(len(host_patterns), 1)
        self.assertTrue(isinstance(host_patterns[0], host))
        self.assertEqual(host_patterns[0].urlconf, 'mysite.api.urls')


class HostTests(HostsTestCase):

    def test_host(self):
        api_host = host(r'api', 'api.urls', name='api')
        self.assertTrue(isinstance(api_host, host))

    def test_host_prefix(self):
        api_host = host(r'api', 'api.urls', name='api', prefix='spam.eggs')
        self.assertEqual(api_host.urlconf, 'spam.eggs.api.urls')

    def test_host_string_callback(self):
        api_host = host(r'api', 'api.urls', name='api',
                        callback='django_hosts.resolvers.get_host_patterns')
        self.assertEqual(api_host.callback, get_host_patterns)

    def test_host_callable_callback(self):
        api_host = host(r'api', 'api.urls', name='api',
                        callback=get_host_patterns)
        self.assertEqual(api_host.callback, get_host_patterns)

    def test_host_nonexistent_callback(self):
        api_host = host(r'api', 'api.urls', name='api',
                        callback='whatever.non_existent')
        self.assertRaisesMessage(ImproperlyConfigured,
            "Could not import 'whatever'. Error was: No module named",
            lambda: api_host.callback)

        api_host = host(r'api', 'api.urls', name='api',
                        callback='django_hosts.non_existent')
        self.assertRaisesMessage(ImproperlyConfigured,
            "Could not import 'django_hosts.non_existent'. "
            "Callable does not exist in module",
            lambda: api_host.callback)

        api_host = host(r'api', 'api.urls', name='api',
                        callback='tests.broken_module.yeah_yeah')
        self.assertRaises(ImproperlyConfigured, lambda: api_host.callback)
