import copy

from django.apps import apps
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand

from admin_view_permission.apps import update_permissions
from admin_view_permission.utils import get_all_permissions


class Command(BaseCommand):
    """
    Add permissions for proxy model. This is needed because of the
    bug https://code.djangoproject.com/ticket/11154.

    When a permission is created for a proxy model, it actually
    creates it for it's base model app_label (eg: for "article"
    instead of "about", for the About proxy model).
    """
    help = "Fix permissions for proxy models."

    def handle(self, *args, **options):
        # We need to execute the post migration callback manually in order
        # to append the view permission on the proxy model. Then the following
        # script will create the appropriate content type and move the
        # permissions under this. If we don't call the callback the script
        # will create only the basic permissions (add, change, delete)
        update_permissions(
            apps.get_app_config('admin_view_permission'),
            apps.get_app_config('admin_view_permission'),
            verbosity=1,
            interactive=True,
            using='default',
        )

        for model in apps.get_models():
            opts = model._meta
            ctype, created = ContentType.objects.get_or_create(
                app_label=opts.app_label,
                model=opts.object_name.lower(),
            )

            for codename, name in get_all_permissions(opts, ctype):
                perm, created = Permission.objects.get_or_create(
                    codename=codename,
                    content_type=ctype,
                    defaults={'name': name},
                )
                if created:
                    self.delete_parent_perms(perm)
                    self.stdout.write('Adding permission {}\n'.format(perm))

    def delete_parent_perms(self, perm):
        # Try to delete the permission attached to the parent model
        # if exists
        parent_perms = Permission.objects.filter(
            codename=perm.codename,
        ).exclude(
            content_type__app_label=perm.content_type.app_label,
        )

        if parent_perms.exists():
            copied_parent_perms = list(copy.deepcopy(parent_perms))
            parent_perms.delete()
            for parent_perm in copied_parent_perms:
                self.stdout.write('Delete permission {}\n'.format(parent_perm))
