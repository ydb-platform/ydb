from django.core.management.base import BaseCommand, CommandError

from django_alive.utils import perform_healthchecks


class Command(BaseCommand):
    help = "Perform healthchecks"

    def handle(self, *args, **options):
        healthy, errors = perform_healthchecks()
        if not healthy:
            raise CommandError("Not Healthy: {}".format("\n  - ".join(errors)))
        self.stdout.write("OK")
