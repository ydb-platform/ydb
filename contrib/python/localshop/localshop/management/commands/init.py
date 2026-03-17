from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):

    def handle(self, *args, **kwargs):
        call_command('migrate', database='default', interactive=False)
        call_command('create_default_user', interactive=False)
