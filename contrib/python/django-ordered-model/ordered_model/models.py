from functools import partial, reduce

from django.core import checks
from django.core.exceptions import ObjectDoesNotExist, FieldDoesNotExist
from django.db import models
from django.db.models import Max, Min, F
from django.db.models.fields.related import ForeignKey
from django.db.models.constants import LOOKUP_SEP
from django.utils.module_loading import import_string
from django.utils.translation import gettext_lazy as _


def get_lookup_value(obj, field):
    try:
        return reduce(lambda i, f: getattr(i, f), field.split(LOOKUP_SEP), obj)
    except ObjectDoesNotExist:
        return None


class OrderedModelQuerySet(models.QuerySet):
    def _get_order_field_name(self):
        return self.model.order_field_name

    def _get_order_field_lookup(self, lookup):
        order_field_name = self._get_order_field_name()
        return LOOKUP_SEP.join([order_field_name, lookup])

    def get_max_order(self):
        order_field_name = self._get_order_field_name()
        return self.aggregate(Max(order_field_name)).get(
            self._get_order_field_lookup("max")
        )

    def get_min_order(self):
        order_field_name = self._get_order_field_name()
        return self.aggregate(Min(order_field_name)).get(
            self._get_order_field_lookup("min")
        )

    def get_next_order(self):
        order = self.get_max_order()
        return order + 1 if order is not None else 0

    def above(self, order, inclusive=False):
        """Filter items above order."""
        lookup = "gte" if inclusive else "gt"
        return self.filter(**{self._get_order_field_lookup(lookup): order})

    def above_instance(self, ref, inclusive=False):
        """Filter items above ref's order."""
        order_field_name = self._get_order_field_name()
        order = getattr(ref, order_field_name)
        return self.above(order, inclusive=inclusive)

    def below(self, order, inclusive=False):
        """Filter items below order."""
        lookup = "lte" if inclusive else "lt"
        return self.filter(**{self._get_order_field_lookup(lookup): order})

    def below_instance(self, ref, inclusive=False):
        """Filter items below ref's order."""
        order_field_name = self._get_order_field_name()
        order = getattr(ref, order_field_name)
        return self.below(order, inclusive=inclusive)

    def decrease_order(self, **extra_kwargs):
        """Decrease `order_field_name` value by 1."""
        order_field_name = self._get_order_field_name()
        update_kwargs = {order_field_name: F(order_field_name) - 1}
        if extra_kwargs:
            update_kwargs.update(extra_kwargs)
        return self.update(**update_kwargs)

    def increase_order(self, **extra_kwargs):
        """Increase `order_field_name` value by 1."""
        order_field_name = self._get_order_field_name()
        update_kwargs = {order_field_name: F(order_field_name) + 1}
        if extra_kwargs:
            update_kwargs.update(extra_kwargs)
        return self.update(**update_kwargs)

    def bulk_create(self, objs, *args, **kwargs):
        order_field_name = self._get_order_field_name()
        order_with_respect_to = self.model.get_order_with_respect_to()
        objs = list(objs)
        order_with_respect_to_mapping = {}
        for obj in objs:
            key = frozenset(obj._wrt_map().items())
            if key in order_with_respect_to_mapping:
                order_with_respect_to_mapping[key] += 1
            else:
                order_with_respect_to_mapping[key] = self.filter(
                    **obj._wrt_map()
                ).get_next_order()
            setattr(obj, order_field_name, order_with_respect_to_mapping[key])
        return super().bulk_create(objs, *args, **kwargs)


class OrderedModelManager(models.Manager.from_queryset(OrderedModelQuerySet)):
    pass


class OrderedModelBase(models.Model):
    """
    An abstract model that allows objects to be ordered relative to each other.
    Usage (See ``OrderedModel``):
     - create a model subclassing ``OrderedModelBase``
     - add an indexed ``PositiveIntegerField`` to the model
     - set ``order_field_name`` to the name of that field
     - use the same field name in ``Meta.ordering``
    [optional]
     - set ``order_with_respect_to`` to limit order to a subset
     - specify ``order_class_path`` in case of polymorphic classes
    """

    objects = OrderedModelManager()

    order_field_name = None
    order_with_respect_to = None
    order_class_path = None

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super(OrderedModelBase, self).__init__(*args, **kwargs)
        self._original_wrt_map = self._wrt_map()

    def _wrt_map(self):
        d = {}
        for order_wrt_name in self.get_order_with_respect_to():
            # we know order_wrt_name is a ForeignKey, so use a cheaper _id lookup
            field_path = order_wrt_name + "_id"
            d[order_wrt_name] = get_lookup_value(self, field_path)
        return d

    def _get_related_objects(self):
        # slow path, for use in the admin which requires the objects
        # expected to generate extra queries
        return [
            get_lookup_value(self, name) for name in self.get_order_with_respect_to()
        ]

    @classmethod
    def _on_ordered_model_delete(cls, sender=None, instance=None, **kwargs):
        """
        This signal handler makes sure that when an OrderedModelBase is deleted via
        cascade database deletes, or queryset delete that the models keep order.
        """

        if getattr(instance, "_was_deleted_via_delete_method", False):
            return

        extra_update = kwargs.get("extra_update", None)

        # Copy of upshuffle logic from OrderedModelBase.delete
        qs = instance.get_ordering_queryset()
        extra_update = {} if extra_update is None else extra_update
        qs.above_instance(instance).decrease_order(**extra_update)

        setattr(instance, "_was_deleted_via_delete_method", True)

    @classmethod
    def get_order_with_respect_to(cls):
        if type(cls.order_with_respect_to) is tuple:
            return cls.order_with_respect_to
        elif type(cls.order_with_respect_to) is str:
            return (cls.order_with_respect_to,)
        elif cls.order_with_respect_to is None:
            return tuple()
        else:
            raise ValueError("Invalid value for model.order_with_respect_to")

    def _validate_ordering_reference(self, ref):
        if self._wrt_map() != ref._wrt_map():
            raise ValueError(
                "{0!r} can only be swapped with instances of {1!r} with equal {2!s} fields.".format(
                    self,
                    self._meta.default_manager.model,
                    " and ".join(
                        ["'{}'".format(o) for o in self.get_order_with_respect_to()]
                    ),
                )
            )

    def get_ordering_queryset(self, qs=None, wrt=None):
        if qs is None:
            if self.order_class_path:
                model = import_string(self.order_class_path)
                qs = model._meta.default_manager.all()
            else:
                qs = self._meta.default_manager.all()
        if wrt:
            return qs.filter(**wrt)
        return qs.filter(**self._wrt_map())

    def previous(self):
        """
        Get previous element in this object's ordered stack.
        """
        return self.get_ordering_queryset().below_instance(self).last()

    def next(self):
        """
        Get next element in this object's ordered stack.
        """
        return self.get_ordering_queryset().above_instance(self).first()

    def save(self, *args, **kwargs):
        order_field_name = self.order_field_name
        wrt_changed = self._wrt_map() != self._original_wrt_map

        if wrt_changed and getattr(self, order_field_name) is not None:
            # do delete-like upshuffle using original_wrt values!
            qs = self.get_ordering_queryset(wrt=self._original_wrt_map)
            qs.above_instance(self).decrease_order()

        if getattr(self, order_field_name) is None or wrt_changed:
            order = self.get_ordering_queryset().get_next_order()
            setattr(self, order_field_name, order)
        super().save(*args, **kwargs)

        self._original_wrt_map = self._wrt_map()

    def delete(self, *args, extra_update=None, **kwargs):
        # Flag re-ordering performed so that post_delete signal
        # does not duplicate the re-ordering. See signals.py
        self._was_deleted_via_delete_method = True

        qs = self.get_ordering_queryset()
        extra_update = {} if extra_update is None else extra_update
        qs.above_instance(self).decrease_order(**extra_update)
        super().delete(*args, **kwargs)

    def swap(self, replacement):
        """
        Swap the position of this object with a replacement object.
        """
        self._validate_ordering_reference(replacement)

        order_field_name = self.order_field_name
        order, replacement_order = (
            getattr(self, order_field_name),
            getattr(replacement, order_field_name),
        )
        setattr(self, order_field_name, replacement_order)
        setattr(replacement, order_field_name, order)
        self.save()
        replacement.save()

    def up(self):
        """
        Move this object up one position.
        """
        previous = self.previous()
        if previous:
            self.swap(previous)

    def down(self):
        """
        Move this object down one position.
        """
        _next = self.next()
        if _next:
            self.swap(_next)

    def to(self, order, extra_update=None):
        """
        Move object to a certain position, updating all affected objects to move accordingly up or down.
        """
        if not isinstance(order, int):
            raise TypeError(
                "Order value must be set using an 'int', not using a '{0}'.".format(
                    type(order).__name__
                )
            )

        order_field_name = self.order_field_name
        if order is None or getattr(self, order_field_name) == order:
            # object is already at desired position
            return
        qs = self.get_ordering_queryset()
        extra_update = {} if extra_update is None else extra_update
        if getattr(self, order_field_name) > order:
            qs.below_instance(self).above(order, inclusive=True).increase_order(
                **extra_update
            )
        else:
            qs.above_instance(self).below(order, inclusive=True).decrease_order(
                **extra_update
            )
        setattr(self, order_field_name, order)
        self.save()

    def above(self, ref, extra_update=None):
        """
        Move this object above the referenced object.
        """
        self._validate_ordering_reference(ref)
        order_field_name = self.order_field_name
        if getattr(self, order_field_name) == getattr(ref, order_field_name):
            return
        if getattr(self, order_field_name) > getattr(ref, order_field_name):
            o = getattr(ref, order_field_name)
        else:
            o = self.get_ordering_queryset().below_instance(ref).get_max_order() or 0
        self.to(o, extra_update=extra_update)

    def below(self, ref, extra_update=None):
        """
        Move this object below the referenced object.
        """
        self._validate_ordering_reference(ref)
        order_field_name = self.order_field_name
        if getattr(self, order_field_name) == getattr(ref, order_field_name):
            return
        if getattr(self, order_field_name) > getattr(ref, order_field_name):
            o = self.get_ordering_queryset().above_instance(ref).get_min_order() or 0
        else:
            o = getattr(ref, order_field_name)
        self.to(o, extra_update=extra_update)

    def top(self, extra_update=None):
        """
        Move this object to the top of the ordered stack.
        """
        o = self.get_ordering_queryset().get_min_order()
        self.to(o, extra_update=extra_update)

    def bottom(self, extra_update=None):
        """
        Move this object to the bottom of the ordered stack.
        """
        o = self.get_ordering_queryset().get_max_order()
        self.to(o, extra_update=extra_update)

    @classmethod
    def check(cls, **kwargs):
        errors = super().check(**kwargs)

        ordering = getattr(cls._meta, "ordering", None)
        if ordering is None or len(ordering) < 1:
            errors.append(
                checks.Error(
                    "OrderedModelBase subclass needs Meta.ordering specified.",
                    hint="If you have overwritten Meta, try inheriting with Meta(OrderedModel.Meta).",
                    obj=str(cls.__qualname__),
                    id="ordered_model.E001",
                )
            )
        owrt = getattr(cls, "order_with_respect_to")
        if not (type(owrt) is tuple or type(owrt) is str or owrt is None):
            errors.append(
                checks.Error(
                    "OrderedModelBase subclass order_with_respect_to value invalid. Expected tuple, str or None.",
                    obj=str(cls.__qualname__),
                    id="ordered_model.E002",
                )
            )
        if not issubclass(cls.objects.__class__, OrderedModelManager):
            # Not using our Manager. This is an Error if the queryset is also wrong, or
            # a Warning if our own QuerySet is returned.
            if issubclass(cls.objects.none().__class__, OrderedModelQuerySet):
                errors.append(
                    checks.Warning(
                        "OrderedModelBase subclass has a ModelManager that does not inherit from OrderedModelManager. This is not ideal but will work.",
                        obj=str(cls.__qualname__),
                        id="ordered_model.W003",
                    )
                )
            else:
                errors.append(
                    checks.Error(
                        "OrderedModelBase subclass has a ModelManager that does not inherit from OrderedModelManager.",
                        obj=str(cls.__qualname__),
                        id="ordered_model.E003",
                    )
                )
        elif not issubclass(cls.objects.none().__class__, OrderedModelQuerySet):
            errors.append(
                checks.Error(
                    "OrderedModelBase subclass ModelManager did not return a QuerySet inheriting from OrderedModelQuerySet.",
                    obj=str(cls.__qualname__),
                    id="ordered_model.E004",
                )
            )

        # each field may be an FK, or recursively an FK ref to an FK
        try:
            for wrt_field in cls.get_order_with_respect_to():
                mc = cls
                for p in wrt_field.split(LOOKUP_SEP):
                    try:
                        f = mc._meta.get_field(p)
                        if not isinstance(f, ForeignKey):
                            errors.append(
                                checks.Error(
                                    "OrderedModel order_with_respect_to specifies field '{0}' (within '{1}') which is not a ForeignKey. This is unsupported.".format(
                                        p, wrt_field
                                    ),
                                    obj=str(cls.__qualname__),
                                    id="ordered_model.E005",
                                )
                            )
                            break
                        mc = f.remote_field.model
                    except FieldDoesNotExist:
                        errors.append(
                            checks.Error(
                                "OrderedModel order_with_respect_to specifies field '{0}' (within '{1}') which does not exist.".format(
                                    p, wrt_field
                                ),
                                obj=str(cls.__qualname__),
                                id="ordered_model.E006",
                            )
                        )
        except ValueError:
            # already handled by type checks for E002
            pass
        return errors


class OrderedModel(OrderedModelBase):
    """
    An abstract model that allows objects to be ordered relative to each other.
    Provides an ``order`` field.
    """

    order = models.PositiveIntegerField(_("order"), editable=False, db_index=True)
    order_field_name = "order"

    class Meta:
        abstract = True
        ordering = ("order",)
