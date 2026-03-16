from os.path import dirname, join

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.apps import apps
from django.contrib.contenttypes.fields import GenericRelation

from django.db.models.base import ModelBase
from django.db.models import (
    ForeignKey, ManyToManyField, OneToOneField, Field, AutoField, BooleanField, FileField
)
from django.db.models.fields.related import \
    ReverseManyToOneDescriptor as ForeignRelatedObjectsDescriptor
from django.db.models.fields.proxy import OrderWrt

from . import generators, random_gen
from .exceptions import (
    ModelNotFound, AmbiguousModelName, InvalidQuantityException, RecipeIteratorEmpty,
    CustomMommyNotFound, InvalidCustomMommy
)
from .utils import import_from_str, import_if_str

recipes = None

# FIXME: use pkg_resource
mock_file_jpeg = join(dirname(__file__), 'mock-img.jpeg')
mock_file_txt = join(dirname(__file__), 'mock_file.txt')

MAX_MANY_QUANTITY = 5


def _valid_quantity(quantity):
    return quantity is not None and (not isinstance(quantity, int) or quantity < 1)


def make(_model, _quantity=None, make_m2m=False, _save_kwargs=None, _refresh_after_create=False,
         _create_files=False, **attrs):
    """
    Creates a persisted instance from a given model its associated models.
    It fill the fields with random values or you can specify
    which fields you want to define its values by yourself.
    """
    _save_kwargs = _save_kwargs or {}
    mommy = Mommy.create(_model, make_m2m=make_m2m, create_files=_create_files)
    if _valid_quantity(_quantity):
        raise InvalidQuantityException

    if _quantity:
        return [
            mommy.make(
                _save_kwargs=_save_kwargs,
                _refresh_after_create=_refresh_after_create,
                **attrs
            )
            for _ in range(_quantity)
        ]
    return mommy.make(
        _save_kwargs=_save_kwargs,
        _refresh_after_create=_refresh_after_create,
        **attrs
    )


def prepare(_model, _quantity=None, _save_related=False, **attrs):
    """
    Creates BUT DOESN'T persist an instance from a given model its
    associated models.
    It fill the fields with random values or you can specify
    which fields you want to define its values by yourself.
    """
    mommy = Mommy.create(_model)
    if _valid_quantity(_quantity):
        raise InvalidQuantityException

    if _quantity:
        return [mommy.prepare(_save_related=_save_related, **attrs) for i in range(_quantity)]
    else:
        return mommy.prepare(_save_related=_save_related, **attrs)


def _recipe(name):
    app, recipe_name = name.rsplit('.', 1)
    return import_from_str('.'.join((app, 'mommy_recipes', recipe_name)))


def make_recipe(mommy_recipe_name, _quantity=None, **new_attrs):
    return _recipe(mommy_recipe_name).make(_quantity=_quantity, **new_attrs)


def prepare_recipe(mommy_recipe_name, _quantity=None, _save_related=False, **new_attrs):
    return _recipe(mommy_recipe_name).prepare(
        _quantity=_quantity,
        _save_related=_save_related,
        **new_attrs
    )


class ModelFinder(object):
    """
    Encapsulates all the logic for finding a model to Mommy.
    """
    _unique_models = None
    _ambiguous_models = None

    def get_model(self, name):
        """
        Get a model.

        :param name String on the form 'applabel.modelname' or 'modelname'.
        :return a model class.
        """
        try:
            if '.' in name:
                app_label, model_name = name.split('.')
                model = apps.get_model(app_label, model_name)
            else:
                model = self.get_model_by_name(name)
        except LookupError:
            model = None

        if not model:
            raise ModelNotFound("Could not find model '%s'." % name.title())

        return model

    def get_model_by_name(self, name):
        """
        Get a model by name.

        If a model with that name exists in more than one app,
        raises AmbiguousModelName.
        """
        name = name.lower()

        if self._unique_models is None:
            self._populate()

        if name in self._ambiguous_models:
            raise AmbiguousModelName('%s is a model in more than one app. '
                                     'Use the form "app.model".' % name.title())

        return self._unique_models.get(name)

    def _populate(self):
        """
        Cache models for faster self._get_model.
        """
        unique_models = {}
        ambiguous_models = []

        all_models = apps.all_models

        for app_model in all_models.values():
            for name, model in app_model.items():
                if name not in unique_models:
                    unique_models[name] = model
                else:
                    ambiguous_models.append(name)

        for name in ambiguous_models:
            unique_models.pop(name, None)

        self._ambiguous_models = ambiguous_models
        self._unique_models = unique_models


def is_iterator(value):
    if not hasattr(value, '__iter__'):
        return False

    return hasattr(value, '__next__')


def _custom_mommy_class():
    """
    Returns custom mommy class specified by MOMMY_CUSTOM_CLASS in the django
    settings, or None if no custom class is defined
    """
    custom_class_string = getattr(settings, 'MOMMY_CUSTOM_CLASS', None)
    if custom_class_string is None:
        return None

    try:
        mommy_class = import_from_str(custom_class_string)

        for required_function_name in ['make', 'prepare']:
            if not hasattr(mommy_class, required_function_name):
                raise InvalidCustomMommy(
                    'Custom Mommy classes must have a "%s" function' % required_function_name
                )

        return mommy_class
    except ImportError:
        raise CustomMommyNotFound("Could not find custom mommy class '%s'" % custom_class_string)


class Mommy(object):
    attr_mapping = {}
    type_mapping = None

    # Note: we're using one finder for all Mommy instances to avoid
    # rebuilding the model cache for every make_* or prepare_* call.
    finder = ModelFinder()

    @classmethod
    def create(cls, _model, make_m2m=False, create_files=False):
        """
        Factory which creates the mommy class defined by the MOMMY_CUSTOM_CLASS setting
        """
        mommy_class = _custom_mommy_class() or cls
        return mommy_class(_model, make_m2m, create_files)

    def __init__(self, _model, make_m2m=False, create_files=False):
        self.make_m2m = make_m2m
        self.create_files = create_files
        self.m2m_dict = {}
        self.iterator_attrs = {}
        self.model_attrs = {}
        self.rel_attrs = {}
        self.rel_fields = []

        if isinstance(_model, ModelBase):
            self.model = _model
        else:
            self.model = self.finder.get_model(_model)

        self.init_type_mapping()

    def init_type_mapping(self):
        self.type_mapping = generators.get_type_mapping()
        generators_from_settings = getattr(settings, 'MOMMY_CUSTOM_FIELDS_GEN', {})
        for k, v in generators_from_settings.items():
            field_class = import_if_str(k)
            generator = import_if_str(v)
            self.type_mapping[field_class] = generator

    def make(
        self,
        _save_kwargs=None,
        _refresh_after_create=False,
        _from_manager=None,
        **attrs
    ):
        """Creates and persists an instance of the model associated
        with Mommy instance."""
        params = {
            'commit': True,
            'commit_related': True,
            '_save_kwargs': _save_kwargs,
            '_refresh_after_create': _refresh_after_create,
            '_from_manager': _from_manager,
        }
        params.update(attrs)
        return self._make(**params)

    def prepare(self, _save_related=False, **attrs):
        """Creates, but does not persist, an instance of the model
        associated with Mommy instance."""
        return self._make(commit=False, commit_related=_save_related, **attrs)

    def get_fields(self):
        return self.model._meta.fields + self.model._meta.many_to_many

    def get_related(self):
        return [r for r in self.model._meta.related_objects if not r.many_to_many]

    def _make(
        self,
        commit=True,
        commit_related=True,
        _save_kwargs=None,
        _refresh_after_create=False,
        _from_manager=None,
        **attrs
    ):
        _save_kwargs = _save_kwargs or {}

        self._clean_attrs(attrs)
        for field in self.get_fields():
            if self._skip_field(field):
                continue

            if isinstance(field, ManyToManyField):
                if field.name not in self.model_attrs:
                    self.m2m_dict[field.name] = self.m2m_value(field)
                else:
                    self.m2m_dict[field.name] = self.model_attrs.pop(field.name)
            elif field.name not in self.model_attrs:
                if not isinstance(field, ForeignKey) or \
                        '{0}_id'.format(field.name) not in self.model_attrs:
                    self.model_attrs[field.name] = self.generate_value(field, commit_related)
            elif callable(self.model_attrs[field.name]):
                self.model_attrs[field.name] = self.model_attrs[field.name]()
            elif field.name in self.iterator_attrs:
                try:
                    self.model_attrs[field.name] = next(self.iterator_attrs[field.name])
                except StopIteration:
                    raise RecipeIteratorEmpty('{0} iterator is empty.'.format(field.name))

        instance = self.instance(
            self.model_attrs,
            _commit=commit,
            _save_kwargs=_save_kwargs,
            _from_manager=_from_manager,
        )
        if commit:
            for related in self.get_related():
                self.create_by_related_name(instance, related)

        if _refresh_after_create:
            instance.refresh_from_db()

        return instance

    def m2m_value(self, field):
        if field.name in self.rel_fields:
            return self.generate_value(field)
        if not self.make_m2m or field.null and not field.fill_optional:
            return []
        return self.generate_value(field)

    def instance(self, attrs, _commit, _save_kwargs, _from_manager):
        one_to_many_keys = {}
        for k in tuple(attrs.keys()):
            field = getattr(self.model, k, None)
            if isinstance(field, ForeignRelatedObjectsDescriptor):
                one_to_many_keys[k] = attrs.pop(k)

        instance = self.model(**attrs)
        # m2m only works for persisted instances
        if _commit:
            instance.save(**_save_kwargs)
            self._handle_one_to_many(instance, one_to_many_keys)
            self._handle_m2m(instance)

            if _from_manager:
                # Fetch the instance using the given Manager, e.g.
                # 'objects'. This will ensure any additional code
                # within its get_queryset() method (e.g. annotations)
                # is run.
                manager = getattr(self.model, _from_manager)
                instance = manager.get(pk=instance.pk)

        return instance

    def create_by_related_name(self, instance, related):
        rel_name = related.get_accessor_name()
        if rel_name not in self.rel_fields:
            return

        kwargs = filter_rel_attrs(rel_name, **self.rel_attrs)
        kwargs[related.field.name] = instance
        kwargs['_model'] = related.field.model

        make(**kwargs)

    def _clean_attrs(self, attrs):
        def is_rel_field(x):
            return '__' in x
        self.fill_in_optional = attrs.pop('_fill_optional', False)
        # error for non existing fields
        if isinstance(self.fill_in_optional, (tuple, list, set)):
            # parents and relations
            wrong_fields = set(self.fill_in_optional) - set(
                f.name for f in self.get_fields()
            )
            if wrong_fields:
                raise AttributeError(
                    '_fill_optional field(s) %s are not related to model %s'
                    % (list(wrong_fields), self.model.__name__)
                )
        self.iterator_attrs = dict((k, v) for k, v in attrs.items() if is_iterator(v))
        self.model_attrs = dict((k, v) for k, v in attrs.items() if not is_rel_field(k))
        self.rel_attrs = dict((k, v) for k, v in attrs.items() if is_rel_field(k))
        self.rel_fields = [x.split('__')[0] for x in self.rel_attrs.keys() if is_rel_field(x)]

    def _skip_field(self, field):
        # check for fill optional argument
        if isinstance(self.fill_in_optional, bool):
            field.fill_optional = self.fill_in_optional
        else:
            field.fill_optional = field.name in self.fill_in_optional

        if isinstance(field, FileField) and not self.create_files:
            return True

        # Skip links to parent so parent is not created twice.
        if isinstance(field, OneToOneField) and self._remote_field(field).parent_link:
            return True

        if isinstance(field, (AutoField, GenericRelation, OrderWrt)):
            return True

        if all([
            field.name not in self.model_attrs,
            field.name not in self.rel_fields,
            field.name not in self.attr_mapping
        ]):
            # Django is quirky in that BooleanFields are always "blank",
            # but have no default.
            if not field.fill_optional and (
                not issubclass(field.__class__, Field) or
                field.has_default() or
                (field.blank and not isinstance(field, BooleanField))
            ):
                return True

        if field.name not in self.model_attrs:
            if field.name not in self.rel_fields and (field.null and not field.fill_optional):
                return True

        return False

    def _handle_one_to_many(self, instance, attrs):
        for k, v in attrs.items():
            manager = getattr(instance, k)

            try:
                manager.set(v, bulk=False, clear=True)
            except TypeError:
                # for many-to-many relationships the bulk keyword argument doesn't exist
                manager.set(v, clear=True)

    def _handle_m2m(self, instance):
        for key, values in self.m2m_dict.items():
            for value in values:
                if not value.pk:
                    value.save()
            m2m_relation = getattr(instance, key)
            through_model = m2m_relation.through

            # using related manager to fire m2m_changed signal
            if through_model._meta.auto_created:
                m2m_relation.add(*values)
            else:
                for value in values:
                    base_kwargs = {
                        m2m_relation.source_field_name: instance,
                        m2m_relation.target_field_name: value
                    }
                    make(through_model, **base_kwargs)

    def _remote_field(self, field):
        return field.remote_field

    def generate_value(self, field, commit=True):
        """
        Calls the generator associated with a field passing all required args.

        Generator Resolution Precedence Order:
        -- attr_mapping - mapping per attribute name
        -- choices -- mapping from avaiable field choices
        -- type_mapping - mapping from user defined type associated generators
        -- default_mapping - mapping from pre-defined type associated
           generators

        `attr_mapping` and `type_mapping` can be defined easily overwriting the
        model.
        """
        if field.name in self.attr_mapping:
            generator = self.attr_mapping[field.name]
        elif getattr(field, 'choices'):
            generator = random_gen.gen_from_choices(field.choices)
        elif isinstance(field, ForeignKey) and \
                issubclass(self._remote_field(field).model, ContentType):
            generator = self.type_mapping[ContentType]
        elif generators.get(field.__class__):
            generator = generators.get(field.__class__)
        elif field.__class__ in self.type_mapping:
            generator = self.type_mapping[field.__class__]
        elif field.has_default():
            return field.default
        else:
            raise TypeError('%s is not supported by mommy.' % field.__class__)

        # attributes like max_length, decimal_places are taken into account when
        # generating the value.
        generator_attrs = get_required_values(generator, field)

        if field.name in self.rel_fields:
            generator_attrs.update(filter_rel_attrs(field.name, **self.rel_attrs))

        if not commit:
            generator = getattr(generator, 'prepare', generator)
        return generator(**generator_attrs)


def get_required_values(generator, field):
    """
    Gets required values for a generator from the field.
    If required value is a function, calls it with field as argument.
    If required value is a string, simply fetch the value from the field
    and return.
    """
    # FIXME: avoid abbreviations
    rt = {}
    if hasattr(generator, 'required'):
        for item in generator.required:

            if callable(item):  # mommy can deal with the nasty hacking too!
                key, value = item(field)
                rt[key] = value

            elif isinstance(item, str):
                rt[item] = getattr(field, item)

            else:
                raise ValueError("Required value '%s' is of wrong type. \
                                  Don't make mommy sad." % str(item))

    return rt


def filter_rel_attrs(field_name, **rel_attrs):
    clean_dict = {}

    for k, v in rel_attrs.items():
        if k.startswith(field_name + '__'):
            splitted_key = k.split('__')
            key = '__'.join(splitted_key[1:])
            clean_dict[key] = v
        else:
            clean_dict[k] = v

    return clean_dict
