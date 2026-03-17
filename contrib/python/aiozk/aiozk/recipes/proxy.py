import logging
from importlib import import_module

from .recipe import Recipe


log = logging.getLogger(__name__)


RECIPES = {
    'data_watcher': 'DataWatcher',
    'children_watcher': 'ChildrenWatcher',
    'lock': 'Lock',
    'shared_lock': 'SharedLock',
    'lease': 'Lease',
    'barrier': 'Barrier',
    'double_barrier': 'DoubleBarrier',
    'election': 'LeaderElection',
    'party': 'Party',
    'counter': 'Counter',
    'tree_cache': 'TreeCache',
    'allocator': 'Allocator',
}


class RecipeClassProxy:
    def __init__(self, client, recipe_class):
        self.client = client
        self.recipe_class = recipe_class

    def __call__(self, *args, **kwargs):
        recipe = self.recipe_class(*args, **kwargs)
        recipe.set_client(self.client)
        return recipe


class RecipeProxy:
    def __init__(self, client):
        self.client = client

        self.installed_classes = {}
        self.gather_installed_classes()

    def __getattr__(self, name):
        if name not in self.installed_classes:
            raise AttributeError('No such recipe: %s' % name)

        return RecipeClassProxy(self.client, self.installed_classes[name])

    def gather_installed_classes(self):
        for module, name in RECIPES.items():
            recipe_class = getattr(import_module('aiozk.recipes.{}'.format(module)), name)

            if not issubclass(recipe_class, Recipe):
                log.error('Could not load recipe %s: not a Recipe subclass', recipe_class.__name__)
                continue

            if not recipe_class.validate_dependencies():
                log.error('Could not load recipe %s has unmet dependencies', recipe_class.__name__)
                continue

            log.debug('Loaded recipe %s', recipe_class.__name__)
            self.installed_classes[recipe_class.__name__] = recipe_class
