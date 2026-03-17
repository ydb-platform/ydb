from __future__ import absolute_import
import sys

from django.db import models
from django.apps import apps
from django.db.models.base import ModelBase
from django.utils.functional import SimpleLazyObject


class SortableThroughModelBase(ModelBase):
    """
    This model meta class sets Meta.auto_created to point to the model class
    which tricks django into thinking that a custom M2M through-table was
    auto-created by the ORM.
    """

    def __new__(cls, name, bases, attrs):
        # This is the super __new__ of the parent class. If we directly
        # call the parent class we'll register the model, which we don't
        # want to do yet
        super_new = super(SortableThroughModelBase, cls).__new__
        base_super_new = super(ModelBase, cls).__new__

        parents = [b for b in bases if isinstance(b, SortableThroughModelBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)

        module = attrs.get('__module__')

        # Create a class for the purposes of grabbing attributes that would
        # be set from inheritance
        tmp_new_class = base_super_new(cls, name, bases, {'__module__': module})

        attr_meta = attrs.get('Meta', None)
        if not attr_meta:
            meta = getattr(tmp_new_class, 'Meta', None)
        else:
            meta = attr_meta
        if meta is None:
            class Meta: pass
            meta = Meta
            meta.__module__ = module

        # Determine the app_label. We need it to call get_model()
        app_label = getattr(meta, 'app_label', None)
        if not app_label:
            # All of this logic is from the parent class
            module = attrs.get('__module__')
            new_class = base_super_new(cls, name, bases, {'__module__': module})
            model_module = sys.modules[new_class.__module__]
            app_label = model_module.__name__.split('.')[-2]

        # Create a callbable using closure variables that returns
        # get_model() for this model
        def _get_model():
            return apps.get_model(app_label, name, False)

        # Pass the callable to SimpleLazyObject
        lazy_model = SimpleLazyObject(_get_model)
        # And set the auto_created to a lazy-loaded model object
        # of the class currently being created
        setattr(meta, 'auto_created', lazy_model)

        attrs['Meta'] = meta

        return super_new(cls, name, bases, attrs)


class SortableThroughModel(models.Model, metaclass=SortableThroughModelBase):

    class Meta:
        abstract = True
