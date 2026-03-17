from traceback import format_exc

from django.core.management.base import BaseCommand

from ...toolbox import send_scheduled_messages


class Command(BaseCommand):

    help = 'Sends scheduled messages (both in pending and error statuses).'

    def add_arguments(self, parser):
        parser.add_argument(
            '--priority', action='store', dest='priority', default=None,
            help='Allows to filter scheduled messages by a priority number. Defaults to None.')

    def handle(self, *args, **options):
        priority = options.get('priority', None)
        priority_str = ''

        if priority is not None:
            priority_str = f'with priority {priority} '

        self.stdout.write(f'Sending scheduled messages {priority_str} ...\n')

        try:
            send_scheduled_messages(priority=priority)

        except Exception as e:
            self.stderr.write(self.style.ERROR(f'Error on send: {e}\n{format_exc()}'))

        else:
            self.stdout.write('Sending done.\n')
