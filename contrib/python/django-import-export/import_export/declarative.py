import logging
import warnings
from collections import OrderedDict

from django.apps import apps
from django.core.exceptions import FieldDoesNotExist
from django.db.models.fields.related import ForeignObjectRel

from import_export.options import ResourceOptions

from .fields import Field
from .instance_loaders import ModelInstanceLoader
from .utils import get_related_model

logger = logging.getLogger(__name__)


class DeclarativeMetaclass(type):
    def __new__(cls, name, bases, attrs):
        def _load_meta_options(base_, meta_):
            options = getattr(base_, "Meta", None)

            for option in [
                option
                for option in dir(options)
                if not option.startswith("_") and hasattr(options, option)
            ]:
                option_value = getattr(options, option)
                if option == "model" and isinstance(option_value, str):
                    option_value = apps.get_model(option_value)

                setattr(meta_, option, option_value)

        declared_fields = []
        meta = ResourceOptions()

        # If this class is subclassing another Resource, add that Resource's
        # fields. Note that we loop over the bases in *reverse*. This is
        # necessary in order to preserve the correct order of fields.
        for base in bases[::-1]:
            if hasattr(base, "fields"):
                declared_fields = list(base.fields.items()) + declared_fields
                # Collect the Meta options
                # #1363 If there are any parent classes, set those options first
                for parent in base.__bases__:
                    _load_meta_options(parent, meta)
                _load_meta_options(base, meta)

        # Add direct fields
        for field_name, obj in attrs.copy().items():
            if isinstance(obj, Field):
                field = attrs.pop(field_name)
                if not field.column_name:
                    field.column_name = field_name
                declared_fields.append((field_name, field))

        attrs["fields"] = OrderedDict(declared_fields)
        new_class = super().__new__(cls, name, bases, attrs)
        # add direct fields
        _load_meta_options(new_class, meta)
        new_class._meta = meta

        return new_class


class ModelDeclarativeMetaclass(DeclarativeMetaclass):
    def __new__(cls, name, bases, attrs):
        # Save the names of fields declared on this class
        class_fields = {name for name, obj in attrs.items() if isinstance(obj, Field)}
        new_class = super().__new__(cls, name, bases, attrs)

        opts = new_class._meta

        if not opts.instance_loader_class:
            opts.instance_loader_class = ModelInstanceLoader

        if opts.model:
            model_opts = opts.model._meta

            # #1693 check the fields explicitly declared as attributes of the Resource
            # class.
            # if 'fields' property is defined, declared fields can only be included
            # if they appear in the 'fields' iterable.
            declared_fields = {}
            for field_name, field in new_class.fields.items():
                column_name = field.column_name
                if (
                    opts.fields is not None
                    and field_name not in opts.fields
                    and column_name not in opts.fields
                ):
                    # #2017 warn only if the unlisted field is
                    # part of the current class
                    if field_name in class_fields:
                        warnings.warn(
                            f"{name}: ignoring field '{field_name}' because "
                            "not declared in 'fields' whitelist",
                            stacklevel=2,
                        )
                    continue
                declared_fields[field_name] = field

            field_list = []
            for f in sorted(model_opts.fields + model_opts.many_to_many):
                if opts.fields is not None and f.name not in opts.fields:
                    continue
                if opts.exclude and f.name in opts.exclude:
                    continue

                if f.name in declared_fields:
                    # If model field is declared in `ModelResource`,
                    # remove it from `declared_fields`
                    # to keep exact order of model fields
                    field = declared_fields.pop(f.name)
                else:
                    field = new_class.field_from_django_field(f.name, f, readonly=False)

                field_list.append(
                    (
                        f.name,
                        field,
                    )
                )

            # Order as model fields first then declared fields by default
            new_class.fields = OrderedDict([*field_list, *declared_fields.items()])

            # add fields that follow relationships
            if opts.fields is not None:
                field_list = []
                for field_name in opts.fields:
                    if field_name in declared_fields:
                        continue
                    if field_name.find("__") == -1:
                        continue

                    model = opts.model
                    attrs = field_name.split("__")
                    for i, attr in enumerate(attrs):
                        verbose_path = ".".join(
                            [opts.model.__name__] + attrs[0 : i + 1]
                        )

                        try:
                            f = model._meta.get_field(attr)
                        except FieldDoesNotExist as e:
                            logger.debug(e, exc_info=e)
                            raise FieldDoesNotExist(
                                "%s: %s has no field named '%s'"
                                % (verbose_path, model.__name__, attr)
                            )

                        if i < len(attrs) - 1:
                            # We're not at the last attribute yet, so check
                            # that we're looking at a relation, and move on to
                            # the next model.
                            if isinstance(f, ForeignObjectRel):
                                model = get_related_model(f)
                            else:
                                if get_related_model(f) is None:
                                    raise KeyError(
                                        "%s is not a relation" % verbose_path
                                    )
                                model = get_related_model(f)

                    if isinstance(f, ForeignObjectRel):
                        f = f.field

                    field = new_class.field_from_django_field(
                        field_name, f, readonly=True
                    )
                    field_list.append((field_name, field))

                new_class.fields.update(OrderedDict(field_list))

        return new_class
