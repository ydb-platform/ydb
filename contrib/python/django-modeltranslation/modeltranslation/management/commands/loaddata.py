from django import VERSION
from django.core.management.commands.loaddata import Command as LoadDataCommand

# Because this command is used (instead of default loaddata), then settings have been imported
# and we can safely import MT modules
from modeltranslation import settings as mt_settings
from modeltranslation.utils import auto_populate


ALLOWED = (None, False, 'all', 'default', 'required')
ALLOWED_FOR_PRINT = ', '.join(str(i) for i in (0,) + ALLOWED[1:])  # For pretty-printing


def check_mode(option, opt_str, value, parser, namespace=None):
    if value == '0' or value.lower() == 'false':
        value = False
    if value not in ALLOWED:
        raise ValueError("%s option can be only one of: %s" % (opt_str, ALLOWED_FOR_PRINT))
    setattr(namespace or parser.values, option.dest, value)


class Command(LoadDataCommand):
    leave_locale_alone = mt_settings.LOADDATA_RETAIN_LOCALE  # Django 1.6

    help = (
        'Using this option will cause fixtures to be loaded under auto-population MODE.'
        + 'Allowed values are: %s' % ALLOWED_FOR_PRINT
    )
    if VERSION < (1, 8):
        from optparse import make_option

        option_list = LoadDataCommand.option_list + (
            make_option(
                '--populate',
                action='callback',
                callback=check_mode,
                type='string',
                dest='populate',
                metavar='MODE',
                help=help,
            ),
        )
    else:
        import argparse

        class CheckAction(argparse.Action):
            def __call__(self, parser, namespace, value, option_string=None):
                check_mode(self, option_string, value, parser, namespace)

        def add_arguments(self, parser):
            super(Command, self).add_arguments(parser)
            parser.add_argument(
                '--populate',
                action=self.CheckAction,
                type=str,
                dest='populate',
                metavar='MODE',
                help=self.help,
            )

    def __init__(self):
        super(Command, self).__init__()
        if mt_settings.LOADDATA_RETAIN_LOCALE and VERSION < (1, 6):
            from django.utils import translation

            self.locale = translation.get_language()

    def handle(self, *fixture_labels, **options):
        if hasattr(self, 'locale'):
            from django.utils import translation

            translation.activate(self.locale)

        mode = options.get('populate')
        if mode is not None:
            with auto_populate(mode):
                return super(Command, self).handle(*fixture_labels, **options)
        else:
            return super(Command, self).handle(*fixture_labels, **options)
