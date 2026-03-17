from collections import defaultdict
from itertools import chain, groupby
import logging

import django
from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.core.exceptions import ObjectDoesNotExist
from django.core.serializers.base import DeserializationError
from django.db import IntegrityError, connections, models, router, transaction
from django.db.models.deletion import Collector
from django.db.models.functions import Cast
from django.utils.encoding import force_str
from django.utils.functional import cached_property
from django.utils.translation import gettext
from django.utils.translation import gettext_lazy as _

from reversion.errors import RevertError
from reversion.revisions import (_follow_relations_recursive,
                                 _get_content_type, _get_options)


logger = logging.getLogger(__name__)


def _safe_revert(versions):
    unreverted_versions = []
    for version in versions:
        try:
            with transaction.atomic(using=version.db):
                version.revert()
        except (IntegrityError, ObjectDoesNotExist):
            logger.warning(f'Could not revert to {version}', exc_info=True)
            unreverted_versions.append(version)
    if len(unreverted_versions) == len(versions):
        raise RevertError(gettext("Could not save %(object_repr)s version - missing dependency.") % {
            "object_repr": unreverted_versions[0],
        })
    if unreverted_versions:
        _safe_revert(unreverted_versions)


class Revision(models.Model):

    """A group of related serialized versions."""

    date_created = models.DateTimeField(
        db_index=True,
        verbose_name=_("date created"),
        help_text="The date and time this revision was created.",
    )

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        on_delete=models.SET_NULL,
        verbose_name=_("user"),
        help_text="The user who created this revision.",
    )

    comment = models.TextField(
        blank=True,
        verbose_name=_("comment"),
        help_text="A text comment on this revision.",
    )

    def get_comment(self):
        try:
            LogEntry = apps.get_model('admin.LogEntry')
            return LogEntry(change_message=self.comment).get_change_message()
        except LookupError:
            return self.comment

    def revert(self, delete=False):
        # Group the models by the database of the serialized model.
        versions_by_db = defaultdict(list)
        for version in self.version_set.iterator():
            versions_by_db[version.db].append(version)
        # For each db, perform a separate atomic revert.
        for version_db, versions in versions_by_db.items():
            with transaction.atomic(using=version_db):
                # Optionally delete objects no longer in the current revision.
                if delete:
                    # Get a set of all objects in this revision.
                    old_revision = set()
                    for version in versions:
                        model = version._model
                        try:
                            # Load the model instance from the same DB as it was saved under.
                            old_revision.add(model._default_manager.using(version.db).get(pk=version.object_id))
                        except model.DoesNotExist:
                            pass
                    # Calculate the set of all objects that are in the revision now.
                    current_revision = chain.from_iterable(
                        _follow_relations_recursive(obj)
                        for obj in old_revision
                    )
                    # Delete objects that are no longer in the current revision.
                    collector = Collector(using=version_db)
                    new_objs = [item for item in current_revision
                                if item not in old_revision]
                    for model, group in groupby(new_objs, type):
                        collector.collect(list(group))
                    collector.delete()
                # Attempt to revert all revisions.
                _safe_revert(versions)

    def __str__(self):
        return ", ".join(force_str(version) for version in self.version_set.all())

    class Meta:
        verbose_name = _('revision')
        verbose_name_plural = _('revisions')
        app_label = "reversion"
        ordering = ("-pk",)


class VersionQuerySet(models.QuerySet):

    def get_for_model(self, model, model_db=None):
        model_db = model_db or router.db_for_write(model)
        content_type = _get_content_type(model, self.db)
        return self.filter(
            content_type=content_type,
            db=model_db,
        )

    def get_for_object_reference(self, model, object_id, model_db=None):
        return self.get_for_model(model, model_db=model_db).filter(
            object_id=object_id,
        )

    def get_for_object(self, obj, model_db=None):
        return self.get_for_object_reference(obj.__class__, obj.pk, model_db=model_db)

    def get_deleted(self, model, model_db=None):
        model_db = model_db or router.db_for_write(model)
        connection = connections[self.db]
        if self.db == model_db and connection.vendor in ("sqlite", "postgresql", "oracle"):
            pk_field_name = model._meta.pk.name
            object_id_cast_target = model._meta.get_field(pk_field_name)
            if django.VERSION >= (2, 1):
                # django 2.0 contains a critical bug that doesn't allow the code below to work,
                # fallback to casting primary keys then
                # see https://code.djangoproject.com/ticket/29142
                if django.VERSION < (2, 2):
                    # properly cast autofields for django before 2.2 as it was fixed in django itself later
                    # see https://github.com/django/django/commit/ac25dd1f8d48accc765c05aebb47c427e51f3255
                    object_id_cast_target = {
                        "AutoField": models.IntegerField(),
                        "BigAutoField": models.BigIntegerField(),
                    }.get(object_id_cast_target.__class__.__name__, object_id_cast_target)
                casted_object_id = Cast(models.OuterRef("object_id"), object_id_cast_target)
                model_qs = (
                    model._default_manager
                    .using(model_db)
                    .filter(**{pk_field_name: casted_object_id})
                )
            else:
                model_qs = (
                    model._default_manager
                    .using(model_db)
                    .annotate(_pk_to_object_id=Cast("pk", Version._meta.get_field("object_id")))
                    .filter(_pk_to_object_id=models.OuterRef("object_id"))
                )
            # conditional expressions are being supported since django 3.0
            # DISTINCT ON works only for Postgres DB
            if connection.vendor == "postgresql" and django.VERSION >= (3, 0):
                subquery = (
                    self.get_for_model(model, model_db=model_db)
                    .filter(~models.Exists(model_qs))
                    .order_by("object_id", "-pk")
                    .distinct("object_id")
                    .values("pk")
                )
            else:
                subquery = (
                    self.get_for_model(model, model_db=model_db)
                    .annotate(pk_not_exists=~models.Exists(model_qs))
                    .filter(pk_not_exists=True)
                    .values("object_id")
                    .annotate(latest_pk=models.Max("pk"))
                    .values("latest_pk")
                )
        else:
            # We have to use a slow subquery.
            subquery = self.get_for_model(model, model_db=model_db).exclude(
                object_id__in=list(
                    model._default_manager.using(model_db).values_list("pk", flat=True).order_by().iterator()
                ),
            ).values_list("object_id").annotate(
                latest_pk=models.Max("pk")
            ).order_by().values_list("latest_pk", flat=True)
        # Perform the subquery.
        # Filter by model to reduce query execution time.
        return self.get_for_model(model, model_db=model_db).filter(pk__in=subquery)

    def get_unique(self):
        last_key = None
        for version in self.iterator():
            key = (version.object_id, version.content_type_id, version.db, version._local_field_dict)
            if last_key != key:
                yield version
            last_key = key


class Version(models.Model):

    """A saved version of a database model."""

    objects = VersionQuerySet.as_manager()

    revision = models.ForeignKey(
        Revision,
        on_delete=models.CASCADE,
        help_text="The revision that contains this version.",
    )

    object_id = models.CharField(
        max_length=191,
        help_text="Primary key of the model under version control.",
    )

    content_type = models.ForeignKey(
        ContentType,
        on_delete=models.CASCADE,
        help_text="Content type of the model under version control.",
    )

    @property
    def _content_type(self):
        return ContentType.objects.db_manager(self._state.db).get_for_id(self.content_type_id)

    @property
    def _model(self):
        return self._content_type.model_class()

    # A link to the current instance, not the version stored in this Version!
    object = GenericForeignKey(
        ct_field="content_type",
        fk_field="object_id",
    )

    db = models.CharField(
        max_length=191,
        help_text="The database the model under version control is stored in.",
    )

    format = models.CharField(
        max_length=255,
        help_text="The serialization format used by this model.",
    )

    serialized_data = models.TextField(
        help_text="The serialized form of this version of the model.",
    )

    object_repr = models.TextField(
        help_text="A string representation of the object.",
    )

    @cached_property
    def _object_version(self):
        version_options = _get_options(self._model)
        data = self.serialized_data
        data = force_str(data.encode("utf8"))
        try:
            return list(serializers.deserialize(self.format, data, ignorenonexistent=True,
                        use_natural_foreign_keys=version_options.use_natural_foreign_keys))[0]
        except DeserializationError:
            raise RevertError(gettext("Could not load %(object_repr)s version - incompatible version data.") % {
                "object_repr": self.object_repr,
            })
        except serializers.SerializerDoesNotExist:
            raise RevertError(gettext("Could not load %(object_repr)s version - unknown serializer %(format)s.") % {
                "object_repr": self.object_repr,
                "format": self.format,
            })

    @cached_property
    def _local_field_dict(self):
        """
        A dictionary mapping field names to field values in this version
        of the model.

        Parent links of inherited multi-table models will not be followed.
        """
        version_options = _get_options(self._model)
        object_version = self._object_version
        obj = object_version.object
        model = self._model
        field_dict = {}
        for field_name in version_options.fields:
            field = model._meta.get_field(field_name)
            if isinstance(field, models.ManyToManyField):
                # M2M fields with a custom through are not stored in m2m_data, but as a separate model.
                if object_version.m2m_data and field.attname in object_version.m2m_data:
                    field_dict[field.attname] = object_version.m2m_data[field.attname]
            else:
                field_dict[field.attname] = getattr(obj, field.attname)
        return field_dict

    @cached_property
    def field_dict(self):
        """
        A dictionary mapping field names to field values in this version
        of the model.

        This method will follow parent links, if present.
        """
        field_dict = self._local_field_dict
        # Add parent data.
        for parent_model, field in self._model._meta.concrete_model._meta.parents.items():
            content_type = _get_content_type(parent_model, self._state.db)
            parent_id = field_dict[field.attname]
            parent_version = self.revision.version_set.get(
                content_type=content_type,
                object_id=parent_id,
                db=self.db,
            )
            field_dict.update(parent_version.field_dict)
        return field_dict

    def revert(self):
        self._object_version.save(using=self.db)

    def __str__(self):
        return self.object_repr

    class Meta:
        verbose_name = _('version')
        verbose_name_plural = _('versions')
        app_label = 'reversion'
        unique_together = (
            ("db", "content_type", "object_id", "revision"),
        )
        indexes = (
            models.Index(
                fields=["content_type", "db"]
            ),
        )
        ordering = ("-pk",)


class _Str(models.Func):

    """Casts a value to the database's text type."""

    function = "CAST"
    template = "%(function)s(%(expressions)s as %(db_type)s)"

    def __init__(self, expression):
        super().__init__(expression, output_field=models.TextField())

    def as_sql(self, compiler, connection):
        self.extra["db_type"] = self.output_field.db_type(connection)
        return super().as_sql(compiler, connection)


def _safe_subquery(method, left_query, left_field_name, right_subquery, right_field_name):
    right_subquery = right_subquery.order_by().values_list(right_field_name, flat=True)
    left_field = left_query.model._meta.get_field(left_field_name)
    right_field = right_subquery.model._meta.get_field(right_field_name)
    # If the databases don't match, we have to do it in-memory.
    # If it's not a supported database, we also have to do it in-memory.
    if (
        left_query.db != right_subquery.db or not
        (
            left_field.get_internal_type() != right_field.get_internal_type() and
            connections[left_query.db].vendor in ("sqlite", "postgresql")
        )
    ):
        return getattr(left_query, method)(**{
            f"{left_field_name}__in": list(right_subquery.iterator()),
        })
    else:
        # If the left hand side is not a text field, we need to cast it.
        if not isinstance(left_field, (models.CharField, models.TextField)):
            left_field_name_str = f"{left_field_name}_str"
            left_query = left_query.annotate(**{
                left_field_name_str: _Str(left_field_name),
            })
            left_field_name = left_field_name_str
        # If the right hand side is not a text field, we need to cast it.
        if not isinstance(right_field, (models.CharField, models.TextField)):
            right_field_name_str = f"{right_field_name}_str"
            right_subquery = right_subquery.annotate(**{
                right_field_name_str: _Str(right_field_name),
            }).values_list(right_field_name_str, flat=True)
            right_field_name = right_field_name_str
        # Use Exists if running on the same DB, it is much much faster
        exist_annotation_name = f"{right_subquery.model._meta.db_table}_annotation_str"
        right_subquery = right_subquery.filter(**{right_field_name: models.OuterRef(left_field_name)})
        left_query = left_query.annotate(**{exist_annotation_name: models.Exists(right_subquery)})
        return getattr(left_query, method)(**{exist_annotation_name: True})
