from os import path

from django.core.management.base import BaseCommand

from uwsgiconf.sysinit import get_config, TYPE_SYSTEMD
from uwsgiconf.utils import Finder
from ...toolbox import SectionMutator


class Command(BaseCommand):

    help = 'Generates configuration files for Systemd, Upstart, etc.'

    def add_arguments(self, parser):  # pragma: nocover

        super().add_arguments(parser)

        parser.add_argument(
            '--systype', dest='systype',
            help='System type alias to make configuration for. E.g.: systemd, upstart.',
        )
        parser.add_argument(
            '--nostatic', action='store_true', dest='nostatic',
            help='Do not serve static and media files.',
        )
        parser.add_argument(
            '--noruntimes', action='store_true', dest='noruntimes',
            help='Do not automatically use a runtime directory to store pid and fifo files.',
        )
        parser.add_argument(
            '--noerrpages', action='store_true', dest='noerrpages',
            help='Do not to configure custom error pages (403, 404, 500).',
        )

    def handle(self, *args, **options):
        systype = options['systype'] or TYPE_SYSTEMD

        mutator = SectionMutator.spawn()
        command = 'manage.py uwsgi_run'

        for opt in ('nostatic', 'noruntimes', 'noerrpages'):
            if options.get(opt, False):
                command = command + f' --{opt}'

        config = get_config(
            systype,
            conf=mutator.section,
            conf_path=path.join(mutator.dir_base, command),
            runner=Finder.python(),
        )

        print(config)
