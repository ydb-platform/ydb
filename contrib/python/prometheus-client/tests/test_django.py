from unittest import skipUnless

from prometheus_client import CollectorRegistry, Counter, generate_latest
from prometheus_client.openmetrics.exposition import ALLOWUTF8

try:
    import django
    from django.test import RequestFactory, TestCase

    from prometheus_client.django import PrometheusDjangoView

    HAVE_DJANGO = True
except ImportError:
    from unittest import TestCase

    HAVE_DJANGO = False

else:
    from django.conf import settings

    if not settings.configured:
        settings.configure(
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    'NAME': ':memory:'
                }
            },
            INSTALLED_APPS=[],
        )
        django.setup()


class MetricsResourceTest(TestCase):
    @skipUnless(HAVE_DJANGO, "Don't have django installed.")
    def setUp(self):
        self.registry = CollectorRegistry()
        self.factory = RequestFactory()

    def test_reports_metrics(self):
        c = Counter('cc', 'A counter', registry=self.registry)
        c.inc()

        request = self.factory.get("/metrics")

        response = PrometheusDjangoView.as_view(registry=self.registry)(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, generate_latest(self.registry, ALLOWUTF8))
