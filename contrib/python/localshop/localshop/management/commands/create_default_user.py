import uuid

from django.core.management.base import BaseCommand

from localshop.apps.accounts.models import User


class Command(BaseCommand):

    def handle(self, *args, **kwargs):
        num_superusers = User.objects.filter(is_superuser=True).count()
        if not num_superusers:
            password = str(uuid.uuid4())

            user = User.objects.create(
                username='admin',
                is_superuser=True,
                is_staff=True)
            user.set_password(password)
            user.save()

            self.stdout.write("You can now login with the credentials: ")
            self.stdout.write("  Username: %s" % user.username)
            self.stdout.write("  Password: %s" % password)
