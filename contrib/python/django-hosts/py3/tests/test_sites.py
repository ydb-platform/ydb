from django.contrib.sites.models import Site
from django.http import HttpResponse
from django.test import RequestFactory
from django.test.utils import override_settings
from django.utils.functional import empty

from django_hosts.middleware import HostsRequestMiddleware

from .base import HostsTestCase
from tests.models import Author, BlogPost, WikiPage


def get_response_empty(request):
    return HttpResponse()


@override_settings(ALLOWED_HOSTS=['wiki.site1', 'wiki.site2', 'admin.site4', 'static'])
class SitesTests(HostsTestCase):

    def setUp(self):
        super().setUp()
        self.site1 = Site.objects.create(domain='wiki.site1', name='site1')
        self.site2 = Site.objects.create(domain='wiki.site2', name='site2')
        self.site3 = Site.objects.create(domain='wiki.site3', name='site3')
        self.site4 = Site.objects.create(domain='admin.site4', name='site4')
        self.page1 = WikiPage.objects.create(content='page1', site=self.site1)
        self.page2 = WikiPage.objects.create(content='page2', site=self.site1)
        self.page3 = WikiPage.objects.create(content='page3', site=self.site2)
        self.page4 = WikiPage.objects.create(content='page4', site=self.site3)

        self.author1 = Author.objects.create(name='john', site=self.site1)
        self.author2 = Author.objects.create(name='terry', site=self.site2)
        self.post1 = BlogPost.objects.create(content='post1',
                                             author=self.author1)
        self.post2 = BlogPost.objects.create(content='post2',
                                             author=self.author2)

    def tearDown(self):
        for model in [WikiPage, BlogPost, Author, Site]:
            model.objects.all().delete()

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        DEFAULT_HOST='www')
    def test_sites_callback(self):
        rf = RequestFactory(headers={'host': 'wiki.site1'})
        request = rf.get('/simple/')
        middleware = HostsRequestMiddleware(get_response_empty)
        middleware.process_request(request)
        self.assertEqual(request.urlconf, 'tests.urls.simple')
        self.assertEqual(request.site.pk, self.site1.pk)

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        DEFAULT_HOST='www')
    def test_sites_cached_callback(self):
        rf = RequestFactory(headers={'host': 'admin.site4'})
        request = rf.get('/simple/')
        middleware = HostsRequestMiddleware(get_response_empty)
        middleware.process_request(request)

        get_site = lambda: request.site.domain

        # first checking if there is a db query
        self.assertEqual(request.site._wrapped, empty)
        self.assertNumQueries(1, get_site)
        self.assertEqual(request.site._wrapped, self.site4)

        # resetting the wrapped site instance to check the cache value
        request.site._wrapped = empty
        self.assertNumQueries(0, get_site)
        self.assertEqual(request.site.pk, self.site4.pk)

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        DEFAULT_HOST='www')
    def test_sites_callback_with_parent_host(self):
        rf = RequestFactory(headers={'host': 'wiki.site2'})
        request = rf.get('/simple/')
        middleware = HostsRequestMiddleware(get_response_empty)
        middleware.process_request(request)
        self.assertEqual(request.urlconf, 'tests.urls.simple')
        self.assertEqual(request.site.pk, self.site2.pk)

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        DEFAULT_HOST='www')
    def test_manager_simple(self):
        rf = RequestFactory(headers={'host': 'wiki.site2'})
        request = rf.get('/simple/')
        middleware = HostsRequestMiddleware(get_response_empty)
        middleware.process_request(request)
        self.assertEqual(request.urlconf, 'tests.urls.simple')
        self.assertEqual(request.site.pk, self.site2.pk)
        self.assertEqual(list(WikiPage.on_site.by_request(request)),
                         [self.page3])

    @override_settings(
        ROOT_HOSTCONF='tests.hosts.simple',
        DEFAULT_HOST='www')
    def test_manager_missing_site(self):
        rf = RequestFactory(headers={'host': 'static'})
        request = rf.get('/simple/')
        middleware = HostsRequestMiddleware(get_response_empty)
        middleware.process_request(request)
        self.assertEqual(request.urlconf, 'tests.urls.simple')
        self.assertRaises(AttributeError, lambda: request.site)
        self.assertEqual(list(WikiPage.on_site.by_request(request)), [])

    def test_manager_default_site(self):
        with self.settings(SITE_ID=self.site1.id):
            self.assertEqual(list(WikiPage.on_site.all()),
                             [self.page1, self.page2])

    def test_manager_related_site(self):
        with self.settings(SITE_ID=self.site1.id):
            self.assertEqual(list(BlogPost.on_site.all()), [self.post1])
        with self.settings(SITE_ID=self.site2.id):
            self.assertEqual(list(BlogPost.on_site.all()), [self.post2])

    def test_no_select_related(self):
        with self.settings(SITE_ID=self.site1.id):
            self.assertEqual(list(BlogPost.no_select_related.all()),
                             [self.post1])

    def test_non_existing_field(self):
        with self.settings(SITE_ID=self.site1.id):
            self.assertRaises(ValueError, BlogPost.non_existing.all)

    def test_dead_end_field(self):
        with self.settings(SITE_ID=self.site1.id):
            self.assertRaises(ValueError, BlogPost.dead_end.all)

    def test_non_rel_field(self):
        with self.settings(SITE_ID=self.site1.id):
            self.assertRaises(TypeError, BlogPost.non_rel.all)
