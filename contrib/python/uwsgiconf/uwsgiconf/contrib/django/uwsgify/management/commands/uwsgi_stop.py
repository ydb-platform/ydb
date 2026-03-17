from ._base import FifoCommand, Fifo


class Command(FifoCommand):

    help = 'Shutdown uWSGI instance'

    def add_arguments(self, parser):  # pragma: nocover

        super().add_arguments(parser)

        parser.add_argument(
            '--force', action='store_true', dest='force',
            help='Use forced (brutal) shutdown instead of a graceful one.',
        )

    def run_cmd(self, fifo: Fifo, options: dict):
        fifo.cmd_stop(force=options['force'])
