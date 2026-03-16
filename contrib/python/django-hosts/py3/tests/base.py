import pytest
from django.test import TestCase, RequestFactory


@pytest.mark.django_db()
class HostsTestCase(TestCase):

    def setUp(self):
        super().setUp()
        # Every test needs access to the request factory.
        self.factory = RequestFactory()
