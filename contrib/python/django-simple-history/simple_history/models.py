import copy
import importlib
import uuid
import warnings
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Union

import django
from django.apps import apps
from django.conf import settings
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist
from django.db import models
from django.db.models import ManyToManyField
from django.db.models.fields.proxy import OrderWrt
from django.db.models.fields.related import ForeignKey
from django.db.models.fields.related_descriptors import (
    ForwardManyToOneDescriptor,
    ForwardOneToOneDescriptor,
    ReverseManyToOneDescriptor,
    ReverseOneToOneDescriptor,
    create_reverse_many_to_one_manager,
)
from django.db.models.query import QuerySet
from django.db.models.signals import m2m_changed
from django.forms.models import model_to_dict
from django.urls import reverse
from django.utils import timezone
from django.utils.encoding import smart_str
from django.utils.functional import cached_property
from django.utils.text import format_lazy
from django.utils.translation import gettext_lazy as _

from . import exceptions, utils
from .manager import (
    SIMPLE_HISTORY_REVERSE_ATTR_NAME,
    HistoricalQuerySet,
    HistoryDescriptor,
    HistoryManager,
)
from .signals import (
    post_create_historical_m2m_records,
    post_create_historical_record,
    pre_create_historical_m2m_records,
    pre_create_historical_record,
)

try:
    from asgiref.local import Local as LocalContext
except ImportError:
    from threading import local as LocalContext

if TYPE_CHECKING:
    ModelTypeHint = models.Model
else:
    ModelTypeHint = object

registered_models = {}


def _default_get_user(request, **kwargs):
    try:
        return request.user
    except AttributeError:
        return None


def _history_user_getter(historical_instance):
    if historical_instance.history_user_id is None:
        return None
    User = get_user_model()
    try:
        return User.objects.get(pk=historical_instance.history_user_id)
    except User.DoesNotExist:
        return None


def _history_user_setter(historical_instance, user):
    if user is not None:
        historical_instance.history_user_id = user.pk


class HistoricalRecords:
    DEFAULT_MODEL_NAME_PREFIX = "Historical"

    thread = context = LocalContext()  # retain thread for backwards compatibility
    m2m_models = {}

    def __init__(
        self,
        verbose_name=None,
        verbose_name_plural=None,
        bases=(models.Model,),
        user_related_name="+",
        table_name=None,
        inherit=False,
        excluded_fields=None,
        history_id_field=None,
        history_change_reason_field=None,
        user_model=None,
        get_user=_default_get_user,
        cascade_delete_history=False,
        custom_model_name=None,
        app=None,
        history_user_id_field=None,
        history_user_getter=_history_user_getter,
        history_user_setter=_history_user_setter,
        related_name=None,
        use_base_model_db=False,
        user_db_constraint=True,
        no_db_index=list(),
        excluded_field_kwargs=None,
        history_manager=HistoryManager,
        historical_queryset=HistoricalQuerySet,
        m2m_fields=(),
        m2m_fields_model_field_name="_history_m2m_fields",
        m2m_bases=(models.Model,),
    ):
        self.user_set_verbose_name = verbose_name
        self.user_set_verbose_name_plural = verbose_name_plural
        self.user_related_name = user_related_name
        self.user_db_constraint = user_db_constraint
        self.table_name = table_name
        self.inherit = inherit
        self.history_id_field = history_id_field
        self.history_change_reason_field = history_change_reason_field
        self.user_model = user_model
        self.get_user = get_user
        self.cascade_delete_history = cascade_delete_history
        self.custom_model_name = custom_model_name
        self.app = app
        self.user_id_field = history_user_id_field
        self.user_getter = history_user_getter
        self.user_setter = history_user_setter
        self.related_name = related_name
        self.use_base_model_db = use_base_model_db
        self.history_manager = history_manager
        self.historical_queryset = historical_queryset
        self.m2m_fields = m2m_fields
        self.m2m_fields_model_field_name = m2m_fields_model_field_name

        if isinstance(no_db_index, str):
            no_db_index = [no_db_index]
        self.no_db_index = no_db_index

        if excluded_fields is None:
            excluded_fields = []
        self.excluded_fields = excluded_fields

        if excluded_field_kwargs is None:
            excluded_field_kwargs = {}
        self.excluded_field_kwargs = excluded_field_kwargs
        try:
            if isinstance(bases, str):
                raise TypeError
            self.bases = (HistoricalChanges,) + tuple(bases)
        except TypeError:
            raise TypeError("The `bases` option must be a list or a tuple.")
        try:
            if isinstance(m2m_bases, str):
                raise TypeError
            self.m2m_bases = (HistoricalChanges,) + tuple(m2m_bases)
        except TypeError:
            raise TypeError("The `m2m_bases` option must be a list or a tuple.")

    def contribute_to_class(self, cls, name):
        self.manager_name = name
        self.module = cls.__module__
        self.cls = cls
        models.signals.class_prepared.connect(self.finalize, weak=False)
        self.add_extra_methods(cls)

        if cls._meta.abstract and not self.inherit:
            msg = (
                "HistoricalRecords added to abstract model ({}) without "
                "inherit=True".format(self.cls.__name__)
            )
            warnings.warn(msg, UserWarning)

    def add_extra_methods(self, cls):
        def save_without_historical_record(self, *args, **kwargs):
            """
            Save the model instance without creating a historical record.

            Make sure you know what you're doing before using this method.
            """
            self.skip_history_when_saving = True
            try:
                ret = self.save(*args, **kwargs)
            finally:
                del self.skip_history_when_saving
            return ret

        setattr(cls, "save_without_historical_record", save_without_historical_record)

    def finalize(self, sender, **kwargs):
        inherited = False
        if self.cls is not sender:  # set in concrete
            inherited = self.inherit and issubclass(sender, self.cls)
            if not inherited:
                return  # set in abstract

        if hasattr(sender._meta, "simple_history_manager_attribute"):
            raise exceptions.MultipleRegistrationsError(
                "{}.{} registered multiple times for history tracking.".format(
                    sender._meta.app_label, sender._meta.object_name
                )
            )
        history_model = self.create_history_model(sender, inherited)

        if inherited:
            # Make sure history model is in same module as concrete model
            module = importlib.import_module(history_model.__module__)
        else:
            module = importlib.import_module(self.module)
        setattr(module, history_model.__name__, history_model)

        # The HistoricalRecords object will be discarded,
        # so the signal handlers can't use weak references.
        models.signals.post_save.connect(self.post_save, sender=sender, weak=False)
        models.signals.post_delete.connect(self.post_delete, sender=sender, weak=False)
        models.signals.pre_delete.connect(self.pre_delete, sender=sender, weak=False)

        m2m_fields = self.get_m2m_fields_from_model(sender)

        for field in m2m_fields:
            m2m_changed.connect(
                partial(self.m2m_changed, attr=field.name),
                sender=field.remote_field.through,
                weak=False,
            )

        descriptor = HistoryDescriptor(
            history_model,
            manager=self.history_manager,
            queryset=self.historical_queryset,
        )
        setattr(sender, self.manager_name, descriptor)
        sender._meta.simple_history_manager_attribute = self.manager_name

        for field in m2m_fields:
            m2m_model = self.create_history_m2m_model(
                history_model, field.remote_field.through
            )
            self.m2m_models[field] = m2m_model

            setattr(module, m2m_model.__name__, m2m_model)

            m2m_descriptor = HistoryDescriptor(m2m_model)
            setattr(history_model, field.name, m2m_descriptor)

    def get_history_model_name(self, model):
        if not self.custom_model_name:
            return f"{self.DEFAULT_MODEL_NAME_PREFIX}{model._meta.object_name}"
        # Must be trying to use a custom history model name
        if callable(self.custom_model_name):
            name = self.custom_model_name(model._meta.object_name)
        else:
            #  simple string
            name = self.custom_model_name
        # Desired class name cannot be same as the model it is tracking
        if not (
            name.lower() == model._meta.object_name.lower()
            and model.__module__ == self.module
        ):
            return name
        raise ValueError(
            "The 'custom_model_name' option '{}' evaluates to a name that is the same "
            "as the model it is tracking. This is not permitted.".format(
                self.custom_model_name
            )
        )

    def create_history_m2m_model(self, model, through_model):
        attrs = {}

        fields = self.copy_fields(through_model)
        attrs.update(fields)
        attrs.update(self.get_extra_fields_m2m(model, through_model, fields))

        name = self.get_history_model_name(through_model)
        registered_models[through_model._meta.db_table] = through_model

        attrs.update(Meta=type("Meta", (), self.get_meta_options_m2m(through_model)))

        m2m_history_model = type(str(name), self.m2m_bases, attrs)

        return m2m_history_model

    def create_history_model(self, model, inherited):
        """
        Creates a historical model to associate with the model provided.
        """
        attrs = {
            "__module__": self.module,
            "_history_excluded_fields": self.excluded_fields,
            "_history_m2m_fields": self.get_m2m_fields_from_model(model),
            "tracked_fields": self.fields_included(model),
        }

        app_module = "%s.models" % model._meta.app_label

        if inherited:
            # inherited use models module
            attrs["__module__"] = model.__module__
        elif model.__module__ != self.module:
            # registered under different app
            attrs["__module__"] = self.module
        elif app_module != self.module:
            # Abuse an internal API because the app registry is loading.
            app = apps.app_configs[model._meta.app_label]
            models_module = app.name
            attrs["__module__"] = models_module

        fields = self.copy_fields(model)
        attrs.update(fields)
        attrs.update(self.get_extra_fields(model, fields))
        # type in python2 wants str as a first argument
        attrs.update(Meta=type("Meta", (), self.get_meta_options(model)))
        if not inherited and self.table_name is not None:
            attrs["Meta"].db_table = self.table_name

        # Set as the default then check for overrides
        name = self.get_history_model_name(model)

        registered_models[model._meta.db_table] = model
        history_model = type(str(name), self.bases, attrs)
        return history_model

    def fields_included(self, model):
        fields = []
        for field in model._meta.fields:
            if field.name not in self.excluded_fields:
                fields.append(field)
        return fields

    def field_excluded_kwargs(self, field):
        """
        Find the excluded kwargs for a given field.
        """
        return self.excluded_field_kwargs.get(field.name, set())

    def copy_fields(self, model):
        """
        Creates copies of the model's original fields, returning
        a dictionary mapping field name to copied field object.
        """
        fields = {}
        for field in self.fields_included(model):
            field = copy.copy(field)
            field.remote_field = copy.copy(field.remote_field)
            if isinstance(field, OrderWrt):
                # OrderWrt is a proxy field, switch to a plain IntegerField
                field.__class__ = models.IntegerField
            if isinstance(field, models.ForeignKey):
                old_field = field
                old_swappable = old_field.swappable
                old_field.swappable = False
                try:
                    _name, _path, args, field_args = old_field.deconstruct()
                finally:
                    old_field.swappable = old_swappable
                if getattr(old_field, "one_to_one", False) or isinstance(
                    old_field, models.OneToOneField
                ):
                    FieldType = models.ForeignKey
                else:
                    FieldType = type(old_field)

                # Remove any excluded kwargs for the field.
                # This is useful when a custom OneToOneField is being used that
                # has a different set of arguments than ForeignKey
                for exclude_arg in self.field_excluded_kwargs(old_field):
                    field_args.pop(exclude_arg, None)

                # If field_args['to'] is 'self' then we have a case where the object
                # has a foreign key to itself. If we pass the historical record's
                # field to = 'self', the foreign key will point to an historical
                # record rather than the base record. We can use old_field.model here.
                if field_args.get("to", None) == "self":
                    field_args["to"] = old_field.model

                # Override certain arguments passed when creating the field
                # so that they work for the historical field.
                field_args.update(
                    db_constraint=False,
                    related_name="+",
                    null=True,
                    blank=True,
                    primary_key=False,
                    db_index=True,
                    serialize=True,
                    unique=False,
                    on_delete=models.DO_NOTHING,
                )
                field = FieldType(*args, **field_args)
                field.name = old_field.name
            else:
                transform_field(field)

            # drop db index
            if field.name in self.no_db_index:
                field.db_index = False

            fields[field.name] = field
        return fields

    def _get_history_change_reason_field(self):
        if self.history_change_reason_field:
            # User specific field from init
            history_change_reason_field = self.history_change_reason_field
        elif getattr(
            settings, "SIMPLE_HISTORY_HISTORY_CHANGE_REASON_USE_TEXT_FIELD", False
        ):
            # Use text field with no max length, not enforced by DB anyways
            history_change_reason_field = models.TextField(null=True)
        else:
            # Current default, with max length
            history_change_reason_field = models.CharField(max_length=100, null=True)

        return history_change_reason_field

    def _get_history_id_field(self):
        if self.history_id_field:
            history_id_field = self.history_id_field.clone()
            history_id_field.primary_key = True
            history_id_field.editable = False
        elif getattr(settings, "SIMPLE_HISTORY_HISTORY_ID_USE_UUID", False):
            history_id_field = models.UUIDField(
                primary_key=True, default=uuid.uuid4, editable=False
            )
        else:
            history_id_field = models.AutoField(primary_key=True)

        return history_id_field

    def _get_history_user_fields(self):
        if self.user_id_field is not None:
            # Tracking user using explicit id rather than Django ForeignKey
            history_user_fields = {
                "history_user": property(self.user_getter, self.user_setter),
                "history_user_id": self.user_id_field,
            }
        else:
            user_model = self.user_model or getattr(
                settings, "AUTH_USER_MODEL", "auth.User"
            )

            history_user_fields = {
                "history_user": models.ForeignKey(
                    user_model,
                    null=True,
                    related_name=self.user_related_name,
                    on_delete=models.SET_NULL,
                    db_constraint=self.user_db_constraint,
                )
            }

        return history_user_fields

    def _get_history_related_field(self, model):
        if self.related_name:
            if self.manager_name == self.related_name:
                raise exceptions.RelatedNameConflictError(
                    "The related name must not be called like the history manager."
                )
            return {
                "history_relation": models.ForeignKey(
                    model,
                    on_delete=models.DO_NOTHING,
                    related_name=self.related_name,
                    db_constraint=False,
                )
            }
        else:
            return {}

    def get_extra_fields_m2m(self, model, through_model, fields):
        """Return dict of extra fields added to the m2m historical record model"""

        extra_fields = {
            "__module__": model.__module__,
            "__str__": lambda self: "{} as of {}".format(
                self._meta.verbose_name, self.history.history_date
            ),
            "history": models.ForeignKey(
                model,
                db_constraint=False,
                on_delete=models.DO_NOTHING,
            ),
            "instance_type": through_model,
            "m2m_history_id": self._get_history_id_field(),
        }

        return extra_fields

    def get_extra_fields(self, model, fields):
        """Return dict of extra fields added to the historical record model"""

        def revert_url(self):
            """URL for this change in the default admin site."""
            opts = model._meta
            app_label, model_name = opts.app_label, opts.model_name
            return reverse(
                f"{admin.site.name}:{app_label}_{model_name}_simple_history",
                args=[getattr(self, opts.pk.attname), self.history_id],
            )

        def get_instance(self):
            attrs = {
                field.attname: getattr(self, field.attname) for field in fields.values()
            }
            if self._history_excluded_fields:
                # We don't add ManyToManyFields to this list because they may cause
                # the subsequent `.get()` call to fail. See #706 for context.
                excluded_attnames = [
                    model._meta.get_field(field).attname
                    for field in self._history_excluded_fields
                    if not isinstance(model._meta.get_field(field), ManyToManyField)
                ]
                try:
                    values = (
                        model.objects.filter(pk=getattr(self, model._meta.pk.attname))
                        .values(*excluded_attnames)
                        .get()
                    )
                except ObjectDoesNotExist:
                    pass
                else:
                    attrs.update(values)
            result = model(**attrs)
            # this is the only way external code could know an instance is historical
            setattr(result, SIMPLE_HISTORY_REVERSE_ATTR_NAME, self)
            return result

        def get_next_record(self):
            """
            Get the next history record for the instance. `None` if last.
            """
            history = utils.get_history_manager_from_history(self)
            return (
                history.filter(history_date__gt=self.history_date)
                .order_by("history_date")
                .first()
            )

        def get_prev_record(self):
            """
            Get the previous history record for the instance. `None` if first.
            """
            history = utils.get_history_manager_from_history(self)
            return (
                history.filter(history_date__lt=self.history_date)
                .order_by("history_date")
                .last()
            )

        def get_default_history_user(instance):
            """
            Returns the user specified by `get_user` method for manually creating
            historical objects
            """
            return self.get_history_user(instance)

        extra_fields = {
            "history_id": self._get_history_id_field(),
            "history_date": models.DateTimeField(db_index=self._date_indexing is True),
            "history_change_reason": self._get_history_change_reason_field(),
            "history_type": models.CharField(
                max_length=1,
                choices=(("+", _("Created")), ("~", _("Changed")), ("-", _("Deleted"))),
            ),
            "history_object": HistoricalObjectDescriptor(
                model, self.fields_included(model)
            ),
            "instance": property(get_instance),
            "instance_type": model,
            "next_record": property(get_next_record),
            "prev_record": property(get_prev_record),
            "revert_url": revert_url,
            "__str__": lambda self: "{} as of {}".format(
                self.history_object, self.history_date
            ),
            "get_default_history_user": staticmethod(get_default_history_user),
        }

        extra_fields.update(self._get_history_related_field(model))
        extra_fields.update(self._get_history_user_fields())

        return extra_fields

    @property
    def _date_indexing(self):
        """False, True, or 'composite'; default is True"""
        result = getattr(settings, "SIMPLE_HISTORY_DATE_INDEX", True)
        valid = True
        if isinstance(result, str):
            result = result.lower()
            if result not in ("composite",):
                valid = False
        elif not isinstance(result, bool):
            valid = False
        if not valid:
            raise ImproperlyConfigured(
                "SIMPLE_HISTORY_DATE_INDEX must be one of (False, True, 'Composite')"
            )
        return result

    def get_meta_options_m2m(self, through_model):
        """
        Returns a dictionary of fields that will be added to
        the Meta inner class of the m2m historical record model.
        """
        name = self.get_history_model_name(through_model)

        meta_fields = {"verbose_name": name}

        if self.app:
            meta_fields["app_label"] = self.app

        return meta_fields

    def get_meta_options(self, model):
        """
        Returns a dictionary of fields that will be added to
        the Meta inner class of the historical record model.
        """
        meta_fields = {
            "ordering": ("-history_date", "-history_id"),
            "get_latest_by": ("history_date", "history_id"),
        }
        if self.user_set_verbose_name:
            name = self.user_set_verbose_name
        else:
            name = format_lazy("historical {}", smart_str(model._meta.verbose_name))
        if self.user_set_verbose_name_plural:
            plural_name = self.user_set_verbose_name_plural
        else:
            plural_name = format_lazy(
                "historical {}", smart_str(model._meta.verbose_name_plural)
            )
        meta_fields["verbose_name"] = name
        meta_fields["verbose_name_plural"] = plural_name
        if self.app:
            meta_fields["app_label"] = self.app
        if self._date_indexing == "composite":
            meta_fields["indexes"] = (
                models.Index(fields=("history_date", model._meta.pk.attname)),
            )
        return meta_fields

    def post_save(self, instance, created, using=None, **kwargs):
        if not getattr(settings, "SIMPLE_HISTORY_ENABLED", True):
            return
        if hasattr(instance, "skip_history_when_saving"):
            return

        if not kwargs.get("raw", False):
            self.create_historical_record(instance, created and "+" or "~", using=using)

    def post_delete(self, instance, using=None, **kwargs):
        if not getattr(settings, "SIMPLE_HISTORY_ENABLED", True):
            return
        if self.cascade_delete_history:
            manager = getattr(instance, self.manager_name)
            manager.using(using).all().delete()
        else:
            self.create_historical_record(instance, "-", using=using)

    def pre_delete(self, instance, **kwargs):
        """
        pre_delete method to ensure all deferred fields are loaded on the model
        """
        # First check that history is enabled (on model and globally)
        if not getattr(settings, "SIMPLE_HISTORY_ENABLED", True):
            return
        if not hasattr(instance._meta, "simple_history_manager_attribute"):
            return
        fields = self.fields_included(instance)
        field_attrs = {field.attname for field in fields}
        deferred_attrs = instance.get_deferred_fields()
        # Load all deferred fields that are present in fields_included
        fields = field_attrs.intersection(deferred_attrs)
        if fields:
            instance.refresh_from_db(fields=fields)

    def get_change_reason_for_object(self, instance, history_type, using):
        """
        Get change reason for object.
        Customize this method to automatically fill change reason from context.
        """
        return utils.get_change_reason_from_object(instance)

    def m2m_changed(self, instance, action, attr, pk_set, reverse, **_):
        if not getattr(settings, "SIMPLE_HISTORY_ENABLED", True):
            return
        if hasattr(instance, "skip_history_when_saving"):
            return

        if action in ("post_add", "post_remove", "post_clear"):
            # It should be safe to ~ this since the row must exist to modify m2m on it
            self.create_historical_record(instance, "~")

    def create_historical_record_m2ms(self, history_instance, instance):
        for field in history_instance._history_m2m_fields:
            m2m_history_model = self.m2m_models[field]
            original_instance = history_instance.instance
            through_model = getattr(original_instance, field.name).through
            through_model_field_names = [f.name for f in through_model._meta.fields]
            through_model_fk_field_names = [
                f.name for f in through_model._meta.fields if isinstance(f, ForeignKey)
            ]

            insert_rows = []

            through_field_name = utils.get_m2m_field_name(field)
            rows = through_model.objects.filter(**{through_field_name: instance})
            rows = rows.select_related(*through_model_fk_field_names)
            for row in rows:
                insert_row = {"history": history_instance}

                for field_name in through_model_field_names:
                    insert_row[field_name] = getattr(row, field_name)
                insert_rows.append(m2m_history_model(**insert_row))

            pre_create_historical_m2m_records.send(
                sender=m2m_history_model,
                rows=insert_rows,
                history_instance=history_instance,
                instance=instance,
                field=field,
            )
            created_rows = m2m_history_model.objects.bulk_create(insert_rows)
            post_create_historical_m2m_records.send(
                sender=m2m_history_model,
                created_rows=created_rows,
                history_instance=history_instance,
                instance=instance,
                field=field,
            )

    def create_historical_record(self, instance, history_type, using=None):
        using = using if self.use_base_model_db else None
        history_date = getattr(instance, "_history_date", timezone.now())
        history_user = self.get_history_user(instance)
        history_change_reason = self.get_change_reason_for_object(
            instance, history_type, using
        )
        manager = getattr(instance, self.manager_name)

        attrs = {}
        for field in self.fields_included(instance):
            attrs[field.attname] = getattr(instance, field.attname)

        relation_field = getattr(manager.model, "history_relation", None)
        if relation_field is not None:
            attrs["history_relation"] = instance

        history_instance = manager.model(
            history_date=history_date,
            history_type=history_type,
            history_user=history_user,
            history_change_reason=history_change_reason,
            **attrs,
        )

        pre_create_historical_record.send(
            sender=manager.model,
            instance=instance,
            history_date=history_date,
            history_user=history_user,
            history_change_reason=history_change_reason,
            history_instance=history_instance,
            using=using,
        )

        history_instance.save(using=using)
        self.create_historical_record_m2ms(history_instance, instance)

        post_create_historical_record.send(
            sender=manager.model,
            instance=instance,
            history_instance=history_instance,
            history_date=history_date,
            history_user=history_user,
            history_change_reason=history_change_reason,
            using=using,
        )

    def get_history_user(self, instance):
        """Get the modifying user from instance or middleware."""
        try:
            return instance._history_user
        except AttributeError:
            request = None
            try:
                if self.context.request.user.is_authenticated:
                    request = self.context.request
            except AttributeError:
                pass

        return self.get_user(instance=instance, request=request)

    def get_m2m_fields_from_model(self, model):
        m2m_fields = set(self.m2m_fields)
        try:
            m2m_fields.update(getattr(model, self.m2m_fields_model_field_name))
        except AttributeError:
            pass
        field_names = [
            field if isinstance(field, str) else field.name for field in m2m_fields
        ]
        return [getattr(model, field_name).field for field_name in field_names]


def transform_field(field):
    """Customize field appropriately for use in historical model"""
    field.name = field.attname
    if isinstance(field, models.BigAutoField):
        field.__class__ = models.BigIntegerField
    elif isinstance(field, models.AutoField):
        field.__class__ = models.IntegerField

    elif isinstance(field, models.FileField):
        # Don't copy file, just path.
        if getattr(settings, "SIMPLE_HISTORY_FILEFIELD_TO_CHARFIELD", False):
            field.__class__ = models.CharField
        else:
            field.__class__ = models.TextField

    # Historical instance shouldn't change create/update timestamps
    field.auto_now = False
    field.auto_now_add = False
    # Just setting db_collation explicitly since we're not using
    # field.deconstruct() here
    field.db_collation = None

    if field.primary_key or field.unique:
        # Unique fields can no longer be guaranteed unique,
        # but they should still be indexed for faster lookups.
        field.primary_key = False
        # DEV: Remove this check (but keep the contents) when the minimum required
        #      Django version is 5.1
        if django.VERSION >= (5, 1):
            field.unique = False
        # (Django < 5.1) Can't set `unique` as it's a property, so set the backing field
        # (Django >= 5.1) Set the backing field in addition to the cached property
        #                 above, to cover all bases
        field._unique = False
        field.db_index = True
        field.serialize = True


class HistoricDescriptorMixin:

    def get_queryset(self, **hints):
        instance = hints.get("instance")
        if instance:
            history = getattr(instance, SIMPLE_HISTORY_REVERSE_ATTR_NAME, None)
            histmgr = getattr(
                self.get_related_model(),
                getattr(
                    self.get_related_model()._meta,
                    "simple_history_manager_attribute",
                    "_notthere",
                ),
                None,
            )
            if history and histmgr:
                return histmgr.as_of(getattr(history, "_as_of", history.history_date))
        return super().get_queryset(**hints)


class HistoricForwardManyToOneDescriptor(
    HistoricDescriptorMixin, ForwardManyToOneDescriptor
):
    """
    Overrides get_queryset to provide historic query support, should the
    instance be historic (and therefore was generated by a timepoint query)
    and the other side of the relation also uses a history manager.
    """

    def get_related_model(self):
        return self.field.remote_field.model


class HistoricReverseManyToOneDescriptor(ReverseManyToOneDescriptor):
    """
    Overrides get_queryset to provide historic query support, should the
    instance be historic (and therefore was generated by a timepoint query)
    and the other side of the relation also uses a history manager.
    """

    @cached_property
    def related_manager_cls(self):
        related_model = self.rel.related_model

        class HistoricRelationModelManager(related_model._default_manager.__class__):
            def get_queryset(self):
                cache_name = (
                    # DEV: Remove this when support for Django 5.0 has been dropped
                    self.field.remote_field.get_cache_name()
                    if django.VERSION < (5, 1)
                    else self.field.remote_field.cache_name
                )
                try:
                    return self.instance._prefetched_objects_cache[cache_name]
                except (AttributeError, KeyError):
                    history = getattr(
                        self.instance, SIMPLE_HISTORY_REVERSE_ATTR_NAME, None
                    )
                    histmgr = getattr(
                        self.model,
                        getattr(
                            self.model._meta,
                            "simple_history_manager_attribute",
                            "_notthere",
                        ),
                        None,
                    )
                    if history and histmgr:
                        queryset = histmgr.as_of(
                            getattr(history, "_as_of", history.history_date)
                        )
                    else:
                        queryset = super().get_queryset()
                    return self._apply_rel_filters(queryset)

        return create_reverse_many_to_one_manager(
            HistoricRelationModelManager, self.rel
        )


class HistoricForeignKey(ForeignKey):
    """
    Allows foreign keys to work properly from a historic instance.

    If you use as_of queries to extract historical instances from
    a model, and you have other models that are related by foreign
    key and also historic, changing them to a HistoricForeignKey
    field type will allow you to naturally cross the relationship
    boundary at the same point in time as the origin instance.

    A historic instance maintains an attribute ("_historic") when
    it is historic, holding the historic record instance and the
    timepoint used to query it ("_as_of").  HistoricForeignKey
    looks for this and uses an as_of query against the related
    object so the relationship is assessed at the same timepoint.
    """

    forward_related_accessor_class = HistoricForwardManyToOneDescriptor
    related_accessor_class = HistoricReverseManyToOneDescriptor


class HistoricForwardOneToOneDescriptor(
    HistoricDescriptorMixin, ForwardOneToOneDescriptor
):
    """
    Overrides get_queryset to provide historic query support, should the
    instance be historic (and therefore was generated by a timepoint query)
    and the other side of the relation also uses a history manager.
    """

    def get_related_model(self):
        return self.field.remote_field.model


class HistoricReverseOneToOneDescriptor(
    HistoricDescriptorMixin, ReverseOneToOneDescriptor
):
    """
    Overrides get_queryset to provide historic query support, should the
    instance be historic (and therefore was generated by a timepoint query)
    and the other side of the relation also uses a history manager.
    """

    def get_related_model(self):
        return self.related.related_model


class HistoricOneToOneField(models.OneToOneField):
    """
    Allows one to one fields to work properly from a historic instance.

    If you use as_of queries to extract historical instances from
    a model, and you have other models that are related by one to
    one fields and also historic, changing them to a
    HistoricOneToOneField field type will allow you to naturally
    cross the relationship boundary at the same point in time as
    the origin instance.

    A historic instance maintains an attribute ("_historic") when
    it is historic, holding the historic record instance and the
    timepoint used to query it ("_as_of").  HistoricOneToOneField
    looks for this and uses an as_of query against the related
    object so the relationship is assessed at the same timepoint.
    """

    forward_related_accessor_class = HistoricForwardOneToOneDescriptor
    related_accessor_class = HistoricReverseOneToOneDescriptor


def is_historic(instance):
    """
    Returns True if the instance was acquired with an as_of timepoint.
    """
    return to_historic(instance) is not None


def to_historic(instance):
    """
    Returns a historic model instance if the instance was acquired with
    an as_of timepoint, or None.
    """
    return getattr(instance, SIMPLE_HISTORY_REVERSE_ATTR_NAME, None)


class HistoricalObjectDescriptor:
    def __init__(self, model, fields_included):
        self.model = model
        self.fields_included = fields_included

    def __get__(self, instance, owner):
        if instance is None:
            return self
        values = {f.attname: getattr(instance, f.attname) for f in self.fields_included}
        return self.model(**values)


class HistoricalChanges(ModelTypeHint):
    def diff_against(
        self,
        old_history: "HistoricalChanges",
        excluded_fields: Iterable[str] = None,
        included_fields: Iterable[str] = None,
        *,
        foreign_keys_are_objs=False,
    ) -> "ModelDelta":
        """
        :param old_history:
        :param excluded_fields: The names of fields to exclude from diffing.
               This takes precedence over ``included_fields``.
        :param included_fields: The names of the only fields to include when diffing.
               If not provided, all history-tracked fields will be included.
        :param foreign_keys_are_objs: If ``False``, the returned diff will only contain
               the raw PKs of any ``ForeignKey`` fields.
               If ``True``, the diff will contain the actual related model objects
               instead of just the PKs; deleted related objects will be instances of
               ``DeletedObject``.
               Note that passing ``True`` will necessarily query the database if the
               related objects have not been prefetched (using e.g.
               ``select_related()``).
        """
        if not isinstance(old_history, type(self)):
            raise TypeError(
                "unsupported type(s) for diffing:"
                f" '{type(self)}' and '{type(old_history)}'"
            )
        if excluded_fields is None:
            excluded_fields = set()

        included_m2m_fields = {field.name for field in old_history._history_m2m_fields}
        if included_fields is None:
            included_fields = {f.name for f in old_history.tracked_fields if f.editable}
        else:
            included_m2m_fields = included_m2m_fields.intersection(included_fields)

        fields = (
            set(included_fields)
            .difference(included_m2m_fields)
            .difference(excluded_fields)
        )
        m2m_fields = set(included_m2m_fields).difference(excluded_fields)

        changes = [
            *self._get_field_changes_for_diff(
                old_history, fields, foreign_keys_are_objs
            ),
            *self._get_m2m_field_changes_for_diff(
                old_history, m2m_fields, foreign_keys_are_objs
            ),
        ]
        # Sort by field (attribute) name, to ensure a consistent order
        changes.sort(key=lambda change: change.field)
        changed_fields = [change.field for change in changes]
        return ModelDelta(changes, changed_fields, old_history, self)

    def _get_field_changes_for_diff(
        self,
        old_history: "HistoricalChanges",
        fields: Iterable[str],
        foreign_keys_are_objs: bool,
    ) -> list["ModelChange"]:
        """Helper method for ``diff_against()``."""
        changes = []

        old_values = model_to_dict(old_history, fields=fields)
        new_values = model_to_dict(self, fields=fields)

        for field in fields:
            old_value = old_values[field]
            new_value = new_values[field]

            if old_value != new_value:
                field_meta = self._meta.get_field(field)
                if foreign_keys_are_objs and isinstance(field_meta, ForeignKey):
                    # Set the fields to their related model objects instead of
                    # the raw PKs from `model_to_dict()`
                    def get_value(record, foreign_key):
                        try:
                            value = getattr(record, field)
                        # `value` seems to be None (without raising this exception)
                        # if the object has not been refreshed from the database
                        except ObjectDoesNotExist:
                            value = None

                        if value is None:
                            value = DeletedObject(field_meta.related_model, foreign_key)
                        return value

                    old_value = get_value(old_history, old_value)
                    new_value = get_value(self, new_value)

                change = ModelChange(field, old_value, new_value)
                changes.append(change)

        return changes

    def _get_m2m_field_changes_for_diff(
        self,
        old_history: "HistoricalChanges",
        m2m_fields: Iterable[str],
        foreign_keys_are_objs: bool,
    ) -> list["ModelChange"]:
        """Helper method for ``diff_against()``."""
        changes = []

        for field in m2m_fields:
            original_field_meta = self.instance_type._meta.get_field(field)
            reverse_field_name = utils.get_m2m_reverse_field_name(original_field_meta)
            # Sort the M2M rows by the related object, to ensure a consistent order
            old_m2m_qs = getattr(old_history, field).order_by(reverse_field_name)
            new_m2m_qs = getattr(self, field).order_by(reverse_field_name)
            m2m_through_model_opts = new_m2m_qs.model._meta

            # Create a list of field names to compare against.
            # The list is generated without the PK of the intermediate (through)
            # table, the foreign key to the history record, and the actual `history`
            # field, to avoid false positives while diffing.
            through_model_fields = [
                f.name
                for f in m2m_through_model_opts.fields
                if f.editable and f.name not in ["id", "m2m_history_id", "history"]
            ]
            old_rows = list(old_m2m_qs.values(*through_model_fields))
            new_rows = list(new_m2m_qs.values(*through_model_fields))

            if old_rows != new_rows:
                if foreign_keys_are_objs:
                    fk_fields = [
                        f
                        for f in through_model_fields
                        if isinstance(m2m_through_model_opts.get_field(f), ForeignKey)
                    ]

                    # Set the through fields to their related model objects instead of
                    # the raw PKs from `values()`
                    def rows_with_foreign_key_objs(m2m_qs):
                        def get_value(obj, through_field):
                            try:
                                value = getattr(obj, through_field)
                            # If the related object has been deleted, `value` seems to
                            # usually already be None instead of raising this exception
                            except ObjectDoesNotExist:
                                value = None

                            if value is None:
                                meta = m2m_through_model_opts.get_field(through_field)
                                foreign_key = getattr(obj, meta.attname)
                                value = DeletedObject(meta.related_model, foreign_key)
                            return value

                        # Replicate the format of the return value of QuerySet.values()
                        return [
                            {
                                through_field: get_value(through_obj, through_field)
                                for through_field in through_model_fields
                            }
                            for through_obj in m2m_qs.select_related(*fk_fields)
                        ]

                    old_rows = rows_with_foreign_key_objs(old_m2m_qs)
                    new_rows = rows_with_foreign_key_objs(new_m2m_qs)

                change = ModelChange(field, old_rows, new_rows)
                changes.append(change)

        return changes


@dataclass(frozen=True)
class DeletedObject:
    model: type[models.Model]
    pk: Any

    def __str__(self):
        deleted_model_str = _("Deleted %(type_name)s") % {
            "type_name": self.model._meta.verbose_name,
        }
        return f"{deleted_model_str} (pk={self.pk})"


# Either:
# - The value of a foreign key field:
#   - If ``foreign_keys_are_objs=True`` is passed to ``diff_against()``:
#     Either the related object or ``DeletedObject``.
#   - Otherwise:
#     The PK of the related object.
#
# - The value of a many-to-many field:
#   A list of dicts from the through model field names to either:
#   - If ``foreign_keys_are_objs=True`` is passed to ``diff_against()``:
#     Either the through model's related objects or ``DeletedObject``.
#   - Otherwise:
#     The PK of the through model's related objects.
#
# - Any of the other possible values of a model field.
ModelChangeValue = Union[Any, DeletedObject, list[dict[str, Union[Any, DeletedObject]]]]


@dataclass(frozen=True)
class ModelChange:
    field: str
    old: ModelChangeValue
    new: ModelChangeValue


@dataclass(frozen=True)
class ModelDelta:
    changes: Sequence[ModelChange]
    changed_fields: Sequence[str]
    old_record: HistoricalChanges
    new_record: HistoricalChanges
