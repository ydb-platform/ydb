from django import VERSION
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, CommandError
from django.utils.translation import gettext as _

from ... import config
from ...forms import ConstanceForm
from ...utils import get_values
from ...models import Constance


def _set_constance_value(key, value):
    """
    Parses and sets a Constance value from a string
    :param key:
    :param value:
    :return:
    """

    form = ConstanceForm(initial=get_values())

    field = form.fields[key]

    clean_value = field.clean(field.to_python(value))
    setattr(config, key, clean_value)


class Command(BaseCommand):
    help = _('Get/Set In-database config settings handled by Constance')

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest='command')
        # API changed in Django>=2.1. cmd argument was removed.
        parser_list = self._subparsers_add_parser(subparsers, 'list', cmd=self, help='list all Constance keys and their values')

        parser_get = self._subparsers_add_parser(subparsers, 'get', cmd=self, help='get the value of a Constance key')
        parser_get.add_argument('key', help='name of the key to get', metavar='KEY')

        parser_set = self._subparsers_add_parser(subparsers, 'set', cmd=self, help='set the value of a Constance key')
        parser_set.add_argument('key', help='name of the key to get', metavar='KEY')
        # use nargs='+' so that we pass a list to MultiValueField (eg SplitDateTimeField)
        parser_set.add_argument('value', help='value to set', metavar='VALUE', nargs='+')

        self._subparsers_add_parser(
            subparsers,
            'remove_stale_keys',
            cmd=self,
            help='delete all Constance keys and their values if they are not in settings.CONSTANCE_CONFIG (stale keys)',
        )

    def _subparsers_add_parser(self, subparsers, name, **kwargs):
        # API in Django >= 2.1 changed and removed cmd parameter from add_parser
        if VERSION >= (2, 1) and 'cmd' in kwargs:
            kwargs.pop('cmd')
        return subparsers.add_parser(name, **kwargs)


    def handle(self, command, key=None, value=None, *args, **options):

        if command == 'get':
            try:
                self.stdout.write("{}".format(getattr(config, key)), ending="\n")
            except AttributeError as e:
                raise CommandError(key + " is not defined in settings.CONSTANCE_CONFIG")

        elif command == 'set':
            try:
                if len(value) == 1:
                    # assume that if a single argument was passed, the field doesn't expect a list
                    value = value[0]

                _set_constance_value(key, value)
            except KeyError as e:
                raise CommandError(key + " is not defined in settings.CONSTANCE_CONFIG")
            except ValidationError as e:
                raise CommandError(", ".join(e))

        elif command == 'list':
            for k, v in get_values().items():
                self.stdout.write("{}\t{}".format(k, v), ending="\n")

        elif command == 'remove_stale_keys':

            actual_keys = settings.CONSTANCE_CONFIG.keys()

            stale_records = Constance.objects.exclude(key__in=actual_keys)
            if stale_records:
                self.stdout.write("The following record will be deleted:", ending="\n")
            else:
                self.stdout.write("There are no stale records in database.", ending="\n")

            for stale_record in stale_records:
                self.stdout.write("{}\t{}".format(stale_record.key, stale_record.value), ending="\n")

            stale_records.delete()
