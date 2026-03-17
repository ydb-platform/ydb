from django.apps import apps
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    args = '<model model ...>'
    help = 'Restore the primary ordering fields of a model containing a special ordering field'

    def add_arguments(self, parser):
        parser.add_argument('models', nargs='+', type=str)

    def handle(self, *args, **options):
        for modelname in options['models']:
            try:
                app_label, model_name = modelname.rsplit('.', 1)
                Model = apps.get_model(app_label, model_name)
            except ImportError:
                raise CommandError('Unable to load model "%s"' % modelname)

            if not hasattr(Model._meta, 'ordering') or len(Model._meta.ordering) == 0:
                raise CommandError(f'Model "{modelname}" does not define field "ordering" in its Meta class')

            orderfield = Model._meta.ordering[0]
            if orderfield[0] == '-':
                orderfield = orderfield[1:]

            for order, obj in enumerate(Model.objects.iterator(), start=1):
                setattr(obj, orderfield, order)
                obj.save()

            self.stdout.write(f'Successfully reordered model "{modelname}"')
