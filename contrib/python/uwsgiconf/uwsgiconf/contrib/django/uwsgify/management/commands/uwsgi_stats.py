from ._base import FifoCommand, Fifo


class Command(FifoCommand):

    help = 'Dump uWSGI configuration and current stats into the log'

    def run_cmd(self, fifo: Fifo, options: dict):
        fifo.cmd_stats()
