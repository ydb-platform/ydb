from django.core.exceptions import ImproperlyConfigured
from django.test.utils import override_settings
from django.urls import NoReverseMatch

from django_hosts.resolvers import (get_host, get_host_patterns, get_hostconf,
                                    get_hostconf_module, reverse, reverse_host)

from .base import HostsTestCase
from tests.hosts import simple


class ReverseTest(HostsTestCase):

    @override_settings(ROOT_HOSTCONF='tests.hosts.simple')
    def test_reverse_host(self):
        self.assertRaises(ValueError,
            reverse_host, 'with_kwargs', ['spam'], dict(eggs='spam'))
        self.assertRaises(NoReverseMatch,
            reverse_host, 'with_kwargs', ['spam', 'eggs'])
        self.assertRaises(NoReverseMatch,
            reverse_host, 'with_kwargs', [], dict(eggs='spam', spam='eggs'))
        self.assertEqual('johndoe',
            reverse_host('with_kwargs', None, dict(username='johndoe')))
        self.assertEqual(reverse_host('with_args', ['johndoe']), 'johndoe')
        with self.settings(PARENT_HOST='spam.eggs'):
            self.assertEqual(reverse_host('with_args', ['johndoe']),
                             'johndoe.spam.eggs')

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        PARENT_HOST='spam.eggs')
    def test_reverse(self):
        self.assertEqual(reverse('simple-direct', host='static'),
                         '//static.spam.eggs/simple/')

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        PARENT_HOST='example.com')
    def test_reverse_without_www(self):
        self.assertEqual(reverse('simple-direct', host='without_www'),
                         '//example.com/simple/')

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.blank',
        PARENT_HOST='example.com')
    def test_reverse_blank(self):
        self.assertEqual(reverse('simple-direct', host='blank_or_www'),
                         '//example.com/simple/')

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        PARENT_HOST='spam.eggs')
    def test_reverse_custom_scheme(self):
        self.assertEqual(reverse('simple-direct', host='scheme'),
                         'https://scheme.spam.eggs/simple/')
        self.assertEqual(reverse('simple-direct', host='scheme', scheme='ftp'),
                         'ftp://scheme.spam.eggs/simple/')

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        PARENT_HOST='spam.eggs')
    def test_reverse_custom_port(self):
        self.assertEqual(reverse('simple-direct', host='port'),
                         '//port.spam.eggs:12345/simple/')
        self.assertEqual(reverse('simple-direct', host='port', port='1337'),
                         '//port.spam.eggs:1337/simple/')


class UtilityTests(HostsTestCase):

    @override_settings(ROOT_HOSTCONF='tests.hosts.simple')
    def test_get_hostconf_module(self):
        self.assertEqual(get_hostconf_module(), simple)

    def test_get_hostconf_module_no_default(self):
        self.assertEqual(
            get_hostconf_module('tests.hosts.simple'), simple)

    def test_missing_host_patterns(self):
        self.assertRaisesMessage(ImproperlyConfigured,
            'Missing ROOT_HOSTCONF setting', get_host_patterns)

    @override_settings(ROOT_HOSTCONF='tests.hosts')
    def test_missing_host_patterns_in_module(self):
        self.assertRaisesMessage(ImproperlyConfigured,
            "Missing host_patterns in 'tests.hosts'",
            get_host_patterns)

    @override_settings(ROOT_HOSTCONF='tests.hosts.simple')
    def test_get_working_host_patterns(self):
        self.assertEqual(get_host_patterns(), simple.host_patterns)

    @override_settings(ROOT_HOSTCONF='tests.hosts.simple')
    def test_get_host(self):
        self.assertEqual(get_host('static').name, 'static')
        self.assertRaisesMessage(NoReverseMatch,
            "No host called 'non-existent' exists", get_host, 'non-existent')

    @override_settings(ROOT_HOSTCONF='tests.hosts.appended')
    def test_appended_patterns(self):
        self.assertEqual(get_host('special').name, 'special')


@override_settings(
    ROOT_HOSTCONF='tests.hosts.simple',
    DEFAULT_HOST='www',
)
class SettingChangedClearCacheTests(HostsTestCase):
    def test_root_hostconf(self):
        self.assertEqual(get_hostconf(), 'tests.hosts.simple')
        with self.settings(ROOT_HOSTCONF='tests.hosts.appended'):
            self.assertEqual(get_hostconf(), 'tests.hosts.appended')
        self.assertEqual(get_hostconf(), 'tests.hosts.simple')

    def test_default_host(self):
        self.assertEqual(get_host().name, 'www')
        with self.settings(DEFAULT_HOST='static'):
            self.assertEqual(get_host().name, 'static')
        self.assertEqual(get_host().name, 'www')
