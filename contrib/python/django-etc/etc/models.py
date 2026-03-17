from types import ModuleType
from typing import Type

from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.db.models.base import ModelBase, Model


class InheritedModelMetaclass(ModelBase):

    def __new__(cls, name, bases, attrs):
        cl = super(InheritedModelMetaclass, cls).__new__(cls, name, bases, attrs)

        texts_marker = '_texts_applied'

        if not getattr(cl, texts_marker, False):
            try:
                names_map = {f.name: f for f in cl._meta.fields}

            except AttributeError:
                return cl

            fields_cl = getattr(cl, 'Fields', None)

            if fields_cl is not None:
                for attr_name, val in fields_cl.__dict__.items():
                    if attr_name.startswith('_'):
                        continue

                    if attr_name not in names_map:
                        continue

                    field = names_map[attr_name]

                    if not isinstance(val, dict):
                        val = {'verbose_name': val}

                    for field_attr, field_val in val.items():
                        setattr(field, field_attr, field_val)

            setattr(cl, texts_marker, True)

        return cl


class InheritedModel(metaclass=InheritedModelMetaclass):
    """Mix in this class into target model (inherit from it) and define `Fields` class inside it
    to be able to customize field attributes (e.g. texts) of a base-parent model.


    Example:

        from etc.models import InheritedModel


        class MyParentModel(models.Model):

            code = models.CharField('dummy', max_length=64)
            expired = models.BooleanField('Expired', help_text='dummy')

            class Meta:
                abstract = True


        class MyChildModel1(InheritedModel, MyParentModel):  # NOTE: InheritedModel must go first.

            time_created = models.DateTimeField('Date created', auto_now_add=True)

            class Fields:  # Defining a class with fields custom fields data.
                code = 'Secret code'  # This is treated as verbose_name.
                expired = {'help_text': 'This code is expired.'}


        class MyChildModel2(InheritedModel, MyParentModel):

            code = models.CharField('dummy', max_length=128, unique=True, editable=False)

            class Fields:
                code = 'Non-secret code'
                expired = {'help_text': 'Do not check it. Do not.'}

    """


def get_model_class_from_string(model_path: str) -> Type[Model]:
    """Returns a certain model as defined in a string formatted `<app_name>.<model_name>`.

    Example:

        model = get_model_class_from_string('myapp.MyModel')

    """
    try:
        app_name, model_name = model_path.split('.')

    except ValueError:
        raise ImproperlyConfigured(
            f'`{model_path}` must have the following format: `app_name.model_name`.')

    try:
        model = apps.get_model(app_name, model_name)

    except (LookupError, ValueError):
        model = None

    if model is None:
        raise ImproperlyConfigured(
            f'`{model_path}` refers to a model `{model_name}` that has not been installed.')

    return model


def get_model_class_from_settings(settings_module: ModuleType, settings_entry_name: str):
    """Returns a certain model as defined in a given settings module.

    Example:

        myapp/settings.py

            from django.conf import settings

            MY_MODEL = getattr(settings, 'MYAPP_MY_MODEL', 'myapp.MyModel')


        myapp/some.py

            from myapp import settings

            model = get_model_class_from_settings(settings, 'MY_MODEL')

    """
    return get_model_class_from_string(getattr(settings_module, settings_entry_name))
