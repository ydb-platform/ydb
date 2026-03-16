from django.core.management.base import BaseCommand, CommandError

from waffle import get_waffle_sample_model

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            'name',
            nargs='?',
            help='The name of the sample.')
        parser.add_argument(
            'percent',
            nargs='?',
            type=int,
            help='The percentage of the time this sample will be active.')
        parser.add_argument(
            '-l', '--list',
            action='store_true', dest='list_samples', default=False,
            help='List existing samples.')
        parser.add_argument(
            '--create',
            action='store_true',
            dest='create',
            default=False,
            help='If the sample does not exist, create it.')

    help = 'Change percentage of a sample.'

    def handle(self, *args, **options):
        if options['list_samples']:
            self.stdout.write('Samples:')
            for sample in get_waffle_sample_model().objects.iterator():
                self.stdout.write(f'{sample.name}: {sample.percent:.1f}%')
            self.stdout.write('')
            return

        sample_name = options['name']
        percent = options['percent']

        if not (sample_name and percent):
            raise CommandError(
                'You need to specify a sample name and percentage.'
            )

        try:
            percent = float(percent)
            if not (0.0 <= percent <= 100.0):
                raise ValueError()
        except ValueError:
            raise CommandError('You need to enter a valid percentage value.')

        if options['create']:
            sample, created = get_waffle_sample_model().objects.get_or_create(
                name=sample_name, defaults={'percent': 0})
            if created:
                self.stdout.write('Creating sample: %s' % sample_name)
        else:
            try:
                sample = get_waffle_sample_model().objects.get(name=sample_name)
            except get_waffle_sample_model().DoesNotExist:
                raise CommandError('This sample does not exist.')

        sample.percent = percent
        sample.save()
