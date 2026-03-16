from traceback import format_exc

from django.core.management.base import BaseCommand

from ...toolbox import check_undelivered


class Command(BaseCommand):

    help = 'Sends a notification email if any undelivered dispatches.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--to', action='store', dest='to', default=None,
            help='Recipient e-mail. If not set Django ADMINS setting is used.')

    def handle(self, *args, **options):

        to = options.get('to', None)

        self.stdout.write('Checking for undelivered dispatches ...\n')

        try:
            undelivered_count = check_undelivered(to=to)

            self.stdout.write(f'Undelivered dispatches count: {undelivered_count}.\n')

        except Exception as e:
            self.stderr.write(self.style.ERROR(f'Error on check: {e}\n{format_exc()}'))

        else:
            self.stdout.write('Check done.\n')
