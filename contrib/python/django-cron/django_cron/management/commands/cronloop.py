from time import sleep

from django.core.management import BaseCommand, call_command


class Command(BaseCommand):
    help = 'Run cronjobs in loop.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-s',
            '--sleep',
            dest='sleep',
            type=int,
            help="Sleep interval in seconds.",
            default=5 * 60,
        )
        parser.add_argument(
            '--cron_classes',
            dest='cron_classes',
            nargs='+',
            help="List of cron classes to run.",
        )
        parser.add_argument(
            '--repeat',
            dest='repeat',
            type=int,
            help="Repeat only X times.",
        )

    def handle(self, *args, **options):
        s = options['sleep']
        classes = options['cron_classes']
        if not classes:
            classes = []
        repeat = options["repeat"]
        if repeat:
            for _ in range(repeat):
                if self._call_command_or_return_true('runcrons', classes, s):
                    break
        else:
            while True:
                if self._call_command_or_return_true('runcrons', classes, s):
                    break

    def _call_command_or_return_true(self, command, classes, s):
        try:
            call_command(command, *classes)
            sleep(s)
        except KeyboardInterrupt:
            return True
