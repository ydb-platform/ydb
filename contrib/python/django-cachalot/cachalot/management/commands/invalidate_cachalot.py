from django.conf import settings
from django.core.management.base import BaseCommand
from django.apps import apps

from ...api import invalidate


class Command(BaseCommand):
    help = 'Invalidates the cache keys set by django-cachalot.'

    def add_arguments(self, parser):
        parser.add_argument('app_label[.model_name]', nargs='*')
        parser.add_argument(
            '-c', '--cache', action='store', dest='cache_alias',
            choices=list(settings.CACHES.keys()),
            help='Cache alias from the CACHES setting.')
        parser.add_argument(
            '-d', '--db', action='store', dest='db_alias',
            choices=list(settings.DATABASES.keys()),
            help='Database alias from the DATABASES setting.')

    def handle(self, *args, **options):
        cache_alias = options['cache_alias']
        db_alias = options['db_alias']
        verbosity = int(options['verbosity'])
        labels = options['app_label[.model_name]']

        models = []
        for label in labels:
            try:
                models.extend(apps.get_app_config(label).get_models())
            except LookupError:
                app_label = '.'.join(label.split('.')[:-1])
                model_name = label.split('.')[-1]
                models.append(apps.get_model(app_label, model_name))

        cache_str = '' if cache_alias is None else "on cache '%s'" % cache_alias
        db_str = '' if db_alias is None else "for database '%s'" % db_alias
        keys_str = 'keys for %s models' % len(models) if labels else 'all keys'

        if verbosity > 0:
            self.stdout.write(' '.join(filter(bool, ['Invalidating', keys_str,
                                                     cache_str, db_str]))
                              + '...')

        invalidate(*models, cache_alias=cache_alias, db_alias=db_alias)
        if verbosity > 0:
            self.stdout.write('Cache keys successfully invalidated.')
