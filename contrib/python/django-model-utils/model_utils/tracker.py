from copy import deepcopy
from functools import wraps

from django.core.exceptions import FieldError
from django.db import models
from django.db.models.fields.files import FieldFile


class LightStateFieldFile(FieldFile):
    """
    FieldFile subclass with the only aim to remove the instance from the state.

    The change introduced in Django 3.1 on FieldFile subclasses results in pickling the
    whole instance for every field tracked.
    As this is done on the initialization of objects, a simple queryset evaluation on
    Django 3.1+ can make the app unusable, as CPU and memory usage gets easily
    multiplied by magnitudes.
    """
    def __getstate__(self):
        """
        We don't need to deepcopy the instance, so nullify if provided.
        """
        state = super().__getstate__()
        if 'instance' in state:
            state['instance'] = None
        return state


def lightweight_deepcopy(value):
    """
    Use our lightweight class to avoid copying the instance on a FieldFile deepcopy.
    """
    if isinstance(value, FieldFile):
        value = LightStateFieldFile(
            instance=value.instance,
            field=value.field,
            name=value.name,
        )
    return deepcopy(value)


class DescriptorWrapper:

    def __init__(self, field_name, descriptor, tracker_attname):
        self.field_name = field_name
        self.descriptor = descriptor
        self.tracker_attname = tracker_attname

    def __get__(self, instance, owner):
        if instance is None:
            return self
        was_deferred = self.field_name in instance.get_deferred_fields()
        try:
            value = self.descriptor.__get__(instance, owner)
        except AttributeError:
            value = self.descriptor
        if was_deferred:
            tracker_instance = getattr(instance, self.tracker_attname)
            tracker_instance.saved_data[self.field_name] = lightweight_deepcopy(value)
        return value

    def __set__(self, instance, value):
        initialized = hasattr(instance, '_instance_initialized')
        was_deferred = self.field_name in instance.get_deferred_fields()

        # Sentinel attribute to detect whether we are already trying to
        # set the attribute higher up the stack. This prevents infinite
        # recursion when retrieving deferred values from the database.
        recursion_sentinel_attname = '_setting_' + self.field_name
        already_setting = hasattr(instance, recursion_sentinel_attname)

        if initialized and was_deferred and not already_setting:
            setattr(instance, recursion_sentinel_attname, True)
            try:
                # Retrieve the value to set the saved_data value.
                # This will undefer the field
                getattr(instance, self.field_name)
            finally:
                instance.__dict__.pop(recursion_sentinel_attname, None)
        if hasattr(self.descriptor, '__set__'):
            self.descriptor.__set__(instance, value)
        else:
            instance.__dict__[self.field_name] = value

    def __getattr__(self, attr):
        return getattr(self.descriptor, attr)

    @staticmethod
    def cls_for_descriptor(descriptor):
        if hasattr(descriptor, '__delete__'):
            return FullDescriptorWrapper
        else:
            return DescriptorWrapper


class FullDescriptorWrapper(DescriptorWrapper):
    """
    Wrapper for descriptors with all three descriptor methods.
    """
    def __delete__(self, obj):
        self.descriptor.__delete__(obj)


class FieldsContext:
    """
    A context manager for tracking nested reset fields contexts.

    If tracked fields is mentioned in more than one FieldsContext, it's state
    is being reset on exiting last context that mentions that field.

    >>> with fields_context(obj.tracker, 'f1', state=state):
    ...     with fields_context(obj.tracker, 'f1', 'f2', state=state):
    ...         obj.do_something_useful()
    ...     # f2 is reset after inner context exit
    ...     obj.do_something_else()
    ... # f1 is reset after outer context exit
    >>>

    * Note that fields are counted by passing same state dict
    * FieldsContext is instantiated using FieldInstanceTracker (`obj.tracker`)
    * Different objects has own state stack

    """

    def __init__(self, tracker, *fields, state=None):
        """
        :param tracker: FieldInstanceTracker instance to be reset after
            context exit
        :param fields: a list of field names to be tracked in current context
        :param state: shared state dict used to count number of field
            occurrences in context stack.

        On context enter each field mentioned in `fields` has +1 in shared
        state, and on exit it receives -1. Fields that have zero after context
        exit are reset in tracker instance.
        """
        if state is None:
            state = {}
        self.tracker = tracker
        self.fields = fields
        self.state = state

    def __enter__(self):
        """
        Increments tracked fields occurrences count in shared state.
        """
        for f in self.fields:
            self.state.setdefault(f, 0)
            self.state[f] += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Decrements tracked fields occurrences count in shared state.

        If any field has no more occurrences in shared state, this field is
        being reset by tracker.
        """
        reset_fields = []
        for f in self.fields:
            self.state[f] -= 1
            if self.state[f] == 0:
                reset_fields.append(f)
                del self.state[f]
        if reset_fields:
            self.tracker.set_saved_fields(fields=reset_fields)


class FieldInstanceTracker:
    def __init__(self, instance, fields, field_map):
        self.instance = instance
        self.fields = fields
        self.field_map = field_map
        self.context = FieldsContext(self, *self.fields)

    def __enter__(self):
        return self.context.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.context.__exit__(exc_type, exc_val, exc_tb)

    def __call__(self, *fields):
        return FieldsContext(self, *fields, state=self.context.state)

    @property
    def deferred_fields(self):
        return self.instance.get_deferred_fields()

    def get_field_value(self, field):
        return getattr(self.instance, self.field_map[field])

    def set_saved_fields(self, fields=None):
        if not self.instance.pk:
            self.saved_data = {}
        elif fields is None:
            self.saved_data = self.current()
        else:
            self.saved_data.update(**self.current(fields=fields))

        # preventing mutable fields side effects
        for field, field_value in self.saved_data.items():
            self.saved_data[field] = lightweight_deepcopy(field_value)

    def current(self, fields=None):
        """Returns dict of current values for all tracked fields"""
        if fields is None:
            deferred_fields = self.deferred_fields
            if deferred_fields:
                fields = [
                    field for field in self.fields
                    if field not in deferred_fields
                ]
            else:
                fields = self.fields

        return {f: self.get_field_value(f) for f in fields}

    def has_changed(self, field):
        """Returns ``True`` if field has changed from currently saved value"""
        if field in self.fields:
            # deferred fields haven't changed
            if field in self.deferred_fields and field not in self.instance.__dict__:
                return False
            return self.previous(field) != self.get_field_value(field)
        else:
            raise FieldError('field "%s" not tracked' % field)

    def previous(self, field):
        """Returns currently saved value of given field"""

        # handle deferred fields that have not yet been loaded from the database
        if self.instance.pk and field in self.deferred_fields and field not in self.saved_data:

            # if the field has not been assigned locally, simply fetch and un-defer the value
            if field not in self.instance.__dict__:
                self.get_field_value(field)

            # if the field has been assigned locally, store the local value, fetch the database value,
            # store database value to saved_data, and restore the local value
            else:
                current_value = self.get_field_value(field)
                self.instance.refresh_from_db(fields=[field])
                self.saved_data[field] = lightweight_deepcopy(self.get_field_value(field))
                setattr(self.instance, self.field_map[field], current_value)

        return self.saved_data.get(field)

    def changed(self):
        """Returns dict of fields that changed since save (with old values)"""
        return {
            field: self.previous(field)
            for field in self.fields
            if self.has_changed(field)
        }


class FieldTracker:

    tracker_class = FieldInstanceTracker

    def __init__(self, fields=None):
        self.fields = fields

    def __call__(self, func=None, fields=None):
        def decorator(f):
            @wraps(f)
            def inner(obj, *args, **kwargs):
                tracker = getattr(obj, self.attname)
                field_list = tracker.fields if fields is None else fields
                with tracker(*field_list):
                    return f(obj, *args, **kwargs)

            return inner
        if func is None:
            return decorator
        return decorator(func)

    def get_field_map(self, cls):
        """Returns dict mapping fields names to model attribute names"""
        field_map = {field: field for field in self.fields}
        all_fields = {f.name: f.attname for f in cls._meta.fields}
        field_map.update(**{k: v for (k, v) in all_fields.items()
                            if k in field_map})
        return field_map

    def contribute_to_class(self, cls, name):
        self.name = name
        self.attname = '_%s' % name
        models.signals.class_prepared.connect(self.finalize_class, sender=cls)

    def finalize_class(self, sender, **kwargs):
        if self.fields is None:
            self.fields = (field.attname for field in sender._meta.fields)
        self.fields = set(self.fields)
        for field_name in self.fields:
            descriptor = getattr(sender, field_name)
            wrapper_cls = DescriptorWrapper.cls_for_descriptor(descriptor)
            wrapped_descriptor = wrapper_cls(field_name, descriptor, self.attname)
            setattr(sender, field_name, wrapped_descriptor)
        self.field_map = self.get_field_map(sender)
        self.patch_init(sender)
        self.model_class = sender
        setattr(sender, self.name, self)
        self.patch_save(sender)

    def initialize_tracker(self, sender, instance, **kwargs):
        if not isinstance(instance, self.model_class):
            return  # Only init instances of given model (including children)
        tracker = self.tracker_class(instance, self.fields, self.field_map)
        setattr(instance, self.attname, tracker)
        tracker.set_saved_fields()
        instance._instance_initialized = True

    def patch_init(self, model):
        original = getattr(model, '__init__')

        @wraps(original)
        def inner(instance, *args, **kwargs):
            original(instance, *args, **kwargs)
            self.initialize_tracker(model, instance)

        setattr(model, '__init__', inner)

    def patch_save(self, model):
        self._patch(model, 'save_base', 'update_fields')
        self._patch(model, 'refresh_from_db', 'fields')

    def _patch(self, model, method, fields_kwarg):
        original = getattr(model, method)

        @wraps(original)
        def inner(instance, *args, **kwargs):
            update_fields = kwargs.get(fields_kwarg)
            if update_fields is None:
                fields = self.fields
            else:
                fields = (
                    field for field in update_fields if
                    field in self.fields
                )
            tracker = getattr(instance, self.attname)
            with tracker(*fields):
                return original(instance, *args, **kwargs)

        setattr(model, method, inner)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            return getattr(instance, self.attname)


class ModelInstanceTracker(FieldInstanceTracker):

    def has_changed(self, field):
        """Returns ``True`` if field has changed from currently saved value"""
        if not self.instance.pk:
            return True
        elif field in self.saved_data:
            return self.previous(field) != self.get_field_value(field)
        else:
            raise FieldError('field "%s" not tracked' % field)

    def changed(self):
        """Returns dict of fields that changed since save (with old values)"""
        if not self.instance.pk:
            return {}
        saved = self.saved_data.items()
        current = self.current()
        return {k: v for k, v in saved if v != current[k]}


class ModelTracker(FieldTracker):
    tracker_class = ModelInstanceTracker

    def get_field_map(self, cls):
        return {field: field for field in self.fields}
