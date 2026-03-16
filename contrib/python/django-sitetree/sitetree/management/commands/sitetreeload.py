from collections import defaultdict

import sys
from django.core import serializers
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand, CommandError
from django.core.management.color import no_style
from django.db import connections, router, transaction, DEFAULT_DB_ALIAS

from sitetree.compat import CommandOption, options_getter
from sitetree.utils import get_tree_model, get_tree_item_model

MODEL_TREE_CLASS = get_tree_model()
MODEL_TREE_ITEM_CLASS = get_tree_item_model()


get_options = options_getter((
    CommandOption(
        '--database', action='store', dest='database',
        default=DEFAULT_DB_ALIAS, help='Nominates a specific database to load fixtures into. '
                                       'Defaults to the "default" database.'),

    CommandOption(
        '--mode', action='store', dest='mode', default='append',
        help='Mode to put data into DB. Variants: `replace`, `append`.'),

    CommandOption(
        '--items_into_tree', action='store', dest='items_into_tree', default=None,
        help='Import only tree items data into tree with given alias.'),
))


class Command(BaseCommand):

    option_list = get_options()

    help = 'Loads sitetrees from fixture in JSON format into database.'
    args = '[fixture_file fixture_file ...]'

    def add_arguments(self, parser):
        parser.add_argument('args', metavar='fixture', nargs='+', help='Fixture files.')
        get_options(parser.add_argument)

    def handle(self, *fixture_files, **options):

        using = options.get('database', DEFAULT_DB_ALIAS)
        mode = options.get('mode', 'append')
        items_into_tree = options.get('items_into_tree', None)

        if items_into_tree is not None:
            try:
                items_into_tree = MODEL_TREE_CLASS.objects.get(alias=items_into_tree)
            except ObjectDoesNotExist:
                raise CommandError(
                    f'Target tree aliased `{items_into_tree}` does not exist. Please create it before import.')
            else:
                mode = 'append'

        connection = connections[using]
        cursor = connection.cursor()

        self.style = no_style()

        loaded_object_count = 0

        if mode == 'replace':
            MODEL_TREE_CLASS.objects.all().delete()
            MODEL_TREE_ITEM_CLASS.objects.all().delete()

        for fixture_file in fixture_files:

            self.stdout.write(f'Loading fixture from `{fixture_file}` ...\n')

            fixture = open(fixture_file, 'r')

            try:
                objects = serializers.deserialize('json', fixture, using=using)
            except (SystemExit, KeyboardInterrupt):
                raise

            trees = []
            tree_items = defaultdict(list)
            tree_item_parents = defaultdict(list)
            tree_items_new_indexes = {}

            try:
                allow_migrate = router.allow_migrate
            except AttributeError:
                # Django < 1.7
                allow_migrate = router.allow_syncdb

            for obj in objects:
                if allow_migrate(using, obj.object.__class__):
                    if isinstance(obj.object, (MODEL_TREE_CLASS, MODEL_TREE_ITEM_CLASS)):
                        if isinstance(obj.object, MODEL_TREE_CLASS):
                            trees.append(obj.object)
                        else:
                            if items_into_tree is not None:
                                obj.object.tree_id = items_into_tree.id
                            tree_items[obj.object.tree_id].append(obj.object)
                            tree_item_parents[obj.object.parent_id].append(obj.object.id)

            if items_into_tree is not None:
                trees = [items_into_tree,]

            try:

                for tree in trees:

                    self.stdout.write(f'\nImporting tree `{tree.alias}` ...\n')
                    orig_tree_id = tree.id

                    if items_into_tree is None:
                        if mode == 'append':
                            tree.pk = None
                            tree.id = None

                        tree.save(using=using)
                        loaded_object_count += 1

                    parents_ahead = []

                    # Parents go first: enough for simple cases.
                    tree_items[orig_tree_id].sort(key=lambda item: item.id not in tree_item_parents.keys())

                    for tree_item in tree_items[orig_tree_id]:
                        parent_ahead = False
                        self.stdout.write(f'Importing item `{tree_item.title}` ...\n')
                        tree_item.tree_id = tree.id
                        orig_item_id = tree_item.id

                        if mode == 'append':
                            tree_item.pk = None
                            tree_item.id = None

                            if tree_item.id in tree_items_new_indexes:
                                tree_item.pk = tree_item.id = tree_items_new_indexes[tree_item.id]

                            if tree_item.parent_id is not None:
                                if tree_item.parent_id in tree_items_new_indexes:
                                    tree_item.parent_id = tree_items_new_indexes[tree_item.parent_id]
                                else:
                                    parent_ahead = True

                        tree_item.save(using=using)
                        loaded_object_count += 1

                        if mode == 'append':
                            tree_items_new_indexes[orig_item_id] = tree_item.id
                            if parent_ahead:
                                parents_ahead.append(tree_item)

                    # Second pass is necessary for tree items being imported before their parents.
                    for tree_item in parents_ahead:
                        tree_item.parent_id = tree_items_new_indexes[tree_item.parent_id]
                        tree_item.save(using=using)

            except (SystemExit, KeyboardInterrupt):
                raise

            except Exception:
                import traceback
                fixture.close()

                self.stderr.write(
                    self.style.ERROR(
                        f"Fixture `{fixture_file}` import error: "
                        f"{''.join(traceback.format_exception(*sys.exc_info()))}\n")
                )

            fixture.close()

        # Reset DB sequences, for DBMS with sequences support.
        if loaded_object_count > 0:
            sequence_sql = connection.ops.sequence_reset_sql(self.style, [MODEL_TREE_CLASS, MODEL_TREE_ITEM_CLASS])
            if sequence_sql:
                self.stdout.write('Resetting DB sequences ...\n')
                for line in sequence_sql:
                    cursor.execute(line)

        connection.close()
