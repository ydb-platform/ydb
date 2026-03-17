import inspect
import itertools
from . import mommy
from .exceptions import RecipeNotFound

# Enable seq to be imported from recipes
from .utils import seq  # NoQA


finder = mommy.ModelFinder()


class Recipe(object):
    def __init__(self, _model, **attrs):
        self.attr_mapping = attrs
        self._model = _model
        # _iterator_backups will hold values of the form (backup_iterator, usable_iterator).
        self._iterator_backups = {}

    def _mapping(self, new_attrs):
        _save_related = new_attrs.get('_save_related', True)
        rel_fields_attrs = dict((k, v) for k, v in new_attrs.items() if '__' in k)
        new_attrs = dict((k, v) for k, v in new_attrs.items() if '__' not in k)
        mapping = self.attr_mapping.copy()
        for k, v in self.attr_mapping.items():
            # do not generate values if field value is provided
            if new_attrs.get(k):
                continue
            elif mommy.is_iterator(v):
                if isinstance(self._model, str):
                    m = finder.get_model(self._model)
                else:
                    m = self._model
                if k not in self._iterator_backups or m.objects.count() == 0:
                    self._iterator_backups[k] = itertools.tee(
                        self._iterator_backups.get(k, [v])[0]
                    )
                mapping[k] = self._iterator_backups[k][1]
            elif isinstance(v, RecipeForeignKey):
                a = {}
                for key, value in list(rel_fields_attrs.items()):
                    if key.startswith('%s__' % k):
                        a[key] = rel_fields_attrs.pop(key)
                recipe_attrs = mommy.filter_rel_attrs(k, **a)
                if _save_related:
                    mapping[k] = v.recipe.make(**recipe_attrs)
                else:
                    mapping[k] = v.recipe.prepare(**recipe_attrs)
            elif isinstance(v, related):
                mapping[k] = v.make()
        mapping.update(new_attrs)
        mapping.update(rel_fields_attrs)
        return mapping

    def make(self, **attrs):
        return mommy.make(self._model, **self._mapping(attrs))

    def prepare(self, **attrs):
        defaults = {'_save_related': False}
        defaults.update(attrs)
        return mommy.prepare(self._model, **self._mapping(defaults))

    def extend(self, **attrs):
        attr_mapping = self.attr_mapping.copy()
        attr_mapping.update(attrs)
        return type(self)(self._model, **attr_mapping)


class RecipeForeignKey(object):

    def __init__(self, recipe):
        if isinstance(recipe, Recipe):
            self.recipe = recipe
        elif isinstance(recipe, str):
            frame = inspect.stack()[2]
            caller_module = inspect.getmodule(frame[0])
            recipe = getattr(caller_module, recipe)
            if recipe:
                self.recipe = recipe
            else:
                raise RecipeNotFound
        else:
            raise TypeError('Not a recipe')


def foreign_key(recipe):
    """
      Returns the callable, so that the associated _model
      will not be created during the recipe definition.
    """
    return RecipeForeignKey(recipe)


class related(object):
    def __init__(self, *args):
        self.related = []
        for recipe in args:
            if isinstance(recipe, Recipe):
                self.related.append(recipe)
            elif isinstance(recipe, str):
                frame = inspect.stack()[1]
                caller_module = inspect.getmodule(frame[0])
                recipe = getattr(caller_module, recipe)
                if recipe:
                    self.related.append(recipe)
                else:
                    raise RecipeNotFound
            else:
                raise TypeError('Not a recipe')

    def make(self):
        """
         Persists objects to m2m relation
        """
        return [m.make() for m in self.related]
