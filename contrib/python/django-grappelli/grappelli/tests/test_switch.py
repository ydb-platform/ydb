# coding: utf-8

from django.contrib.auth.models import Permission, User
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.test.utils import override_settings
from django.urls import reverse
from django.utils.html import escape
from django.utils.translation import gettext_lazy as _

from grappelli.templatetags.grp_tags import switch_user_dropdown
from grappelli.tests.models import Category


@override_settings(GRAPPELLI_SWITCH_USER=True)
@override_settings(GRAPPELLI_SWITCH_USER_ORIGINAL=lambda user: user.is_superuser)
@override_settings(GRAPPELLI_SWITCH_USER_TARGET=lambda original_user, user: user.is_staff and not user.is_superuser)
@override_settings(ROOT_URLCONF="grappelli.tests.urls")
class SwitchTests(TestCase):

    def setUp(self):
        """
        Create superusers and editors
        """
        self.superuser_1 = User.objects.create_superuser('Superuser001', 'superuser001@example.com', 'superuser001')
        self.superuser_2 = User.objects.create_superuser('Superuser002', 'superuser002@example.com', 'superuser002')
        self.editor_1 = User.objects.create_user('Editor001', 'editor001@example.com', 'editor001')
        self.editor_1.is_staff = True
        self.editor_1.save()
        self.editor_2 = User.objects.create_user('Editor002', 'editor002@example.com', 'editor002')
        self.editor_2.is_staff = True
        self.editor_2.save()
        self.user_1 = User.objects.create_user('User001', 'user001@example.com', 'user001')
        self.user_1.is_staff = False
        self.user_1.save()

        # add permissions for editor001
        content_type = ContentType.objects.get_for_model(Category)
        permissions = Permission.objects.filter(content_type=content_type)
        self.editor_1.user_permissions.set(permissions)

        # add categories
        for i in range(100):
            Category.objects.create(name="Category No %s" % (i))

    def test_switch_login(self):
        """
        Test login users
        """
        self.assertEqual(User.objects.all().count(), 5)
        self.assertEqual(Category.objects.all().count(), 100)

        self.client.login(username="Superuser001", password="superuser001")
        response = self.client.get(reverse("admin:grappelli_category_changelist"))
        self.assertEqual(response.status_code, 200)

        # templatetag (Editor001, Editor002)
        t = switch_user_dropdown(response.context)
        t_cmp = '<li><a href="/grappelli/switch/user/3/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-target">Editor001</a></li><li><a href="/grappelli/switch/user/4/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-target">Editor002</a></li>'
        self.assertEqual(t, t_cmp)

        self.client.login(username="Superuser002", password="superuser002")
        response = self.client.get(reverse("admin:grappelli_category_changelist"))
        self.assertEqual(response.status_code, 200)

        # templatetag (Editor001, Editor002)
        t = switch_user_dropdown(response.context)
        t_cmp = '<li><a href="/grappelli/switch/user/3/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-target">Editor001</a></li><li><a href="/grappelli/switch/user/4/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-target">Editor002</a></li>'
        self.assertEqual(t, t_cmp)

        self.client.login(username="Editor001", password="editor001")
        response = self.client.get(reverse("admin:grappelli_category_changelist"))
        self.assertEqual(response.status_code, 200)

        # templatetag (empty)
        t = switch_user_dropdown(response.context)
        t_cmp = ''
        self.assertEqual(t, t_cmp)

        self.client.login(username="Editor002", password="editor002")
        response = self.client.get(reverse("admin:grappelli_category_changelist"))
        self.assertEqual(response.status_code, 403)

        self.client.login(username="User001", password="user001")
        response = self.client.get(reverse("admin:grappelli_category_changelist"), follow=True)
        self.assertEqual(response.status_code, 200)  # redirect to login, FIXME: better testing

    def test_switch_superuser001_superuser002(self):
        """
        Test switching from superuser001 to superuser002

        That should not work, because one superuser is not allowed to login
        as another superuser (given the standard grappelli settings)
        """
        original_user = User.objects.get(username="Superuser001")
        target_user = User.objects.get(username="Superuser002")

        self.client.login(username="Superuser001", password="superuser001")
        response = self.client.get("%s?redirect=%s" % (reverse("grp_switch_user", args=[target_user.id]), reverse("admin:grappelli_category_changelist")), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual([m.message for m in list(response.context['messages'])], [_("Permission denied.")])
        self.assertEqual(self.client.session.get("original_user", None), None)
        self.assertEqual(int(self.client.session['_auth_user_id']), original_user.pk)

    def test_switch_superuser001_editor001(self):
        """
        Test switching from superuser001 to Editor001

        That should work.
        """
        original_user = User.objects.get(username="Superuser001")
        target_user = User.objects.get(username="Editor001")

        self.client.login(username="Superuser001", password="superuser001")
        response = self.client.get("%s?redirect=%s" % (reverse("grp_switch_user", args=[target_user.id]), reverse("admin:grappelli_category_changelist")), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.session.get("original_user", None), {"id": original_user.id, "username": original_user.username})
        self.assertEqual(int(self.client.session['_auth_user_id']), target_user.pk)

        # templatetag (Superuser001, Editor001, Editor002)
        # now we have an additional list element with the original user, Superuser001
        t = switch_user_dropdown(response.context)
        t_cmp = '<li><a href="/grappelli/switch/user/1/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-original">Superuser001</a></li><li><a href="/grappelli/switch/user/3/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-target">Editor001</a></li><li><a href="/grappelli/switch/user/4/?redirect=/admin/grappelli/category/" class="grp-switch-user-is-target">Editor002</a></li>'
        self.assertEqual(t, t_cmp)

        # switch back to superuser
        response = self.client.get("%s?redirect=%s" % (reverse("grp_switch_user", args=[original_user.id]), reverse("admin:grappelli_category_changelist")), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.session.get("original_user", None), None)
        self.assertEqual(int(self.client.session['_auth_user_id']), original_user.pk)

    def test_switch_superuser001_user001(self):
        """
        Test switching from superuser001 to user001

        That should not work, because user001 is not found
        """
        original_user = User.objects.get(username="Superuser001")
        target_user = User.objects.get(username="User001")

        self.client.login(username="Superuser001", password="superuser001")
        response = self.client.get("%s?redirect=%s" % (reverse("grp_switch_user", args=[target_user.id]), reverse("admin:grappelli_category_changelist")), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual([m.message for m in list(response.context['messages'])], [_('%(name)s object with primary key %(key)r does not exist.') % {'name': "User", 'key': escape(target_user.id)}])
        self.assertEqual(self.client.session.get("original_user", None), None)
        self.assertEqual(int(self.client.session['_auth_user_id']), original_user.pk)

    def test_switch_editor001_user001(self):
        """
        Test switching from editor001 to user001

        That should not work, because editor001 is not a superuser
        """
        original_user = User.objects.get(username="Editor001")
        target_user = User.objects.get(username="User001")

        self.client.login(username="Editor001", password="editor001")
        response = self.client.get("%s?redirect=%s" % (reverse("grp_switch_user", args=[target_user.id]), reverse("admin:grappelli_category_changelist")), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual([m.message for m in list(response.context['messages'])], [_("Permission denied.")])
        self.assertEqual(self.client.session.get("original_user", None), None)
        self.assertEqual(int(self.client.session['_auth_user_id']), original_user.pk)
