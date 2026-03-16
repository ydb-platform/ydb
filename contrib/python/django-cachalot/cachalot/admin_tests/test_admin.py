from django.test import TestCase
from django.contrib.auth.models import User
from .models import TestModel
from django.test import Client


class AdminTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create(username='admin', is_staff=True, is_superuser=True)

    def test_save_test_model(self):
        """
        Model 'TestModel' has UniqueConstraint which caused problems when saving TestModelAdmin in Django >= 4.1
        """
        self.client.force_login(self.user)
        response = self.client.post('/admin/admin_tests/testmodel/add/', {'name': 'test', 'public': True})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(TestModel.objects.count(), 1)
