from django.core.management.base import BaseCommand

from ...toolbox import run_uwsgi, SectionMutator


class Command(BaseCommand):

    help = 'Runs uWSGI to serve your project'

    def add_arguments(self, parser):  # pragma: nocover

        super().add_arguments(parser)

        parser.add_argument(
            '--nostatic', action='store_false', dest='contribute_static',
            help='Do not serve static and media files.',
        )
        parser.add_argument(
            '--noruntimes', action='store_false', dest='contribute_runtimes',
            help='Do not automatically use a runtime directory to store pid and fifo files.',
        )
        parser.add_argument(
            '--noerrpages', action='store_false', dest='contribute_errpages',
            help='Do not to configure custom error pages (403, 404, 500).',
        )
        parser.add_argument(
            '--compile', action='store_true', dest='compile',
            help='Do not run just print out compiled uWSGI .ini configuration.',
        )
        parser.add_argument(
            '--embedded', action='store_true', dest='embedded',
            help='Do not create temporary config files and try to use resource files for configuration',
        )

    def handle(self, *args, **options):
        mutator = SectionMutator.spawn(options=options)
        run_uwsgi(
            mutator.section,
            compile_only=options['compile'],
            embedded=options['embedded'],
        )
