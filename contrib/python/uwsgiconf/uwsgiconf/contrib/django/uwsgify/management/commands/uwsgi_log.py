from ._base import FifoCommand, Fifo


class Command(FifoCommand):

    help = 'Allows managing of uWSGI log related stuff'

    def add_arguments(self, parser):  # pragma: nocover

        super().add_arguments(parser)

        parser.add_argument(
            '--reopen', action='store_true', dest='reopen',
            help='Reopen log file. Could be required after third party rotation.',
        )
        parser.add_argument(
            '--rotate', action='store_true', dest='rotate',
            help='Trigger built-in log rotation.',
        )

    def run_cmd(self, fifo: Fifo, options: dict):
        fifo.cmd_log(reopen=options['reopen'], rotate=options['rotate'])
