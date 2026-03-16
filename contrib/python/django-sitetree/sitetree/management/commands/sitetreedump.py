from django.core import serializers
from django.core.management.base import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS

from sitetree.utils import get_tree_model, get_tree_item_model
from sitetree.compat import CommandOption, options_getter


MODEL_TREE_CLASS = get_tree_model()
MODEL_TREE_ITEM_CLASS = get_tree_item_model()


get_options = options_getter((
    CommandOption(
        '--indent', default=None, dest='indent', type=int,
        help='Specifies the indent level to use when pretty-printing output.'),

    CommandOption('--items_only', action='store_true', dest='items_only', default=False,
         help='Export tree items only.'),

    CommandOption('--database', action='store', dest='database', default=DEFAULT_DB_ALIAS,
         help='Nominates a specific database to export fixtures from. Defaults to the "default" database.'),
))


class Command(BaseCommand):

    option_list = get_options()
    help = 'Output sitetrees from database as a fixture in JSON format.'
    args = '[tree_alias tree_alias ...]'

    def add_arguments(self, parser):
        parser.add_argument('args', metavar='tree', nargs='*', help='Tree aliases.', default=[])
        get_options(parser.add_argument)

    def handle(self, *aliases, **options):

        indent = options.get('indent', None)
        using = options.get('database', DEFAULT_DB_ALIAS)
        items_only = options.get('items_only', False)

        objects = []

        if aliases:
            trees = MODEL_TREE_CLASS._default_manager.using(using).filter(alias__in=aliases)
        else:
            trees = MODEL_TREE_CLASS._default_manager.using(using).all()

        if not items_only:
            objects.extend(trees)

        for tree in trees:
            objects.extend(MODEL_TREE_ITEM_CLASS._default_manager.using(using).filter(tree=tree).order_by('parent'))

        try:
            return serializers.serialize('json', objects, indent=indent)

        except Exception as e:
            raise CommandError(f'Unable to serialize sitetree(s): {e}')
