import functools
import logging
from collections import OrderedDict
from copy import deepcopy
from html import escape
from warnings import warn

import tablib
from diff_match_patch import diff_match_patch
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.core.management.color import no_style
from django.core.paginator import Paginator
from django.db import connections, router
from django.db.models import fields
from django.db.models.fields.related import ForeignKey
from django.db.models.query import QuerySet
from django.db.transaction import TransactionManagementError, set_rollback
from django.utils.encoding import force_str
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from . import exceptions, widgets
from .declarative import DeclarativeMetaclass, ModelDeclarativeMetaclass
from .fields import Field
from .results import Error, Result, RowResult
from .utils import atomic_if_using_transaction, get_related_model

logger = logging.getLogger(__name__)
# Set default logging handler to avoid "No handler found" warnings.
logger.addHandler(logging.NullHandler())


def has_natural_foreign_key(model):
    """
    Determine if a model has natural foreign key functions
    """
    return hasattr(model, "natural_key") and hasattr(
        model.objects, "get_by_natural_key"
    )


class Diff:
    def __init__(self, resource, instance, new):
        self.left = Diff._read_field_values(resource, instance)
        self.right = []
        self.new = new

    def compare_with(self, resource, instance):
        self.right = Diff._read_field_values(resource, instance)

    def as_html(self):
        data = []
        dmp = diff_match_patch()
        for v1, v2 in zip(self.left, self.right):
            if v1 != v2 and self.new:
                v1 = ""
            diff = dmp.diff_main(force_str(v1), force_str(v2))
            dmp.diff_cleanupSemantic(diff)
            html = dmp.diff_prettyHtml(diff)
            html = mark_safe(html)
            data.append(html)
        return data

    @classmethod
    def _read_field_values(cls, resource, instance):
        return [f.export(instance) for f in resource.get_import_fields()]


class Resource(metaclass=DeclarativeMetaclass):
    """
    Resource defines how objects are mapped to their import and export
    representations and handle importing and exporting data.
    """

    def __init__(self, **kwargs):
        """
        kwargs:
           An optional dict of kwargs.
           Subclasses can use kwargs to pass dynamic values to enhance import / exports.
        """
        # The fields class attribute is the *class-wide* definition of
        # fields. Because a particular *instance* of the class might want to
        # alter self.fields, we create self.fields here by copying cls.fields.
        # Instances should always modify self.fields; they should not modify
        # cls.fields.
        self.fields = deepcopy(self.fields)

        # lists to hold model instances in memory when bulk operations are enabled
        self.create_instances = []
        self.update_instances = []
        self.delete_instances = []

    @classmethod
    def get_result_class(self):
        """
        Returns the class used to store the result of an import.
        """
        return Result

    @classmethod
    def get_row_result_class(self):
        """
        Returns the class used to store the result of a row import.
        """
        return RowResult

    @classmethod
    def get_error_result_class(self):
        """
        Returns the class used to store an error resulting from an import.
        """
        return Error

    @classmethod
    def get_diff_class(self):
        """
        Returns the class used to display the diff for an imported instance.
        """
        return Diff

    @classmethod
    def get_db_connection_name(self):
        if self._meta.using_db is None:
            return router.db_for_write(self._meta.model)
        else:
            return self._meta.using_db

    def get_use_transactions(self):
        if self._meta.use_transactions is None:
            return getattr(settings, "IMPORT_EXPORT_USE_TRANSACTIONS", True)
        else:
            return self._meta.use_transactions

    def get_chunk_size(self):
        if self._meta.chunk_size is None:
            return getattr(settings, "IMPORT_EXPORT_CHUNK_SIZE", 100)
        else:
            return self._meta.chunk_size

    def get_fields(self, **kwargs):
        warn(
            "The 'get_fields()' method is deprecated and will be removed "
            "in a future release",
            DeprecationWarning,
            stacklevel=2,
        )
        return list(self.fields.values())

    def get_field_name(self, field):
        """
        Returns the field name for a given field.
        """
        for field_name, f in self.fields.items():
            if f == field:
                return field_name
        raise AttributeError(
            f"Field {field} does not exists in {self.__class__} resource"
        )

    def init_instance(self, row=None):
        """
        Initializes an object. Implemented in
        :meth:`import_export.resources.ModelResource.init_instance`.
        """
        raise NotImplementedError()

    def get_instance(self, instance_loader, row):
        """
        Calls the :doc:`InstanceLoader <api_instance_loaders>`.
        """
        import_id_fields = [self.fields[f] for f in self.get_import_id_fields()]
        for field in import_id_fields:
            if field.column_name not in row:
                # if there is an 'import id field' which is not defined in the
                # row, then it is not possible to return an existing instance,
                # so no need to proceed any further
                return
        return instance_loader.get_instance(row)

    def get_or_init_instance(self, instance_loader, row):
        """
        Either fetches an already existing instance or initializes a new one.
        """
        if not self._meta.force_init_instance:
            instance = self.get_instance(instance_loader, row)
            if instance:
                return instance, False
        return self.init_instance(row), True

    def get_import_id_fields(self):
        """ """
        return self._meta.import_id_fields

    def get_bulk_update_fields(self):
        """
        Returns the fields to be included in calls to bulk_update().
        ``import_id_fields`` are removed because `id` fields cannot be supplied to
        bulk_update().
        """
        return [f for f in self.fields if f not in self._meta.import_id_fields]

    def bulk_create(
        self, using_transactions, dry_run, raise_errors, batch_size=None, result=None
    ):
        """
        Creates objects by calling ``bulk_create``.
        """
        if len(self.create_instances) > 0 and (using_transactions or not dry_run):
            try:
                self._meta.model.objects.bulk_create(
                    self.create_instances, batch_size=batch_size
                )
            except Exception as e:
                self.handle_import_error(result, e, raise_errors)
            finally:
                self.create_instances.clear()

    def bulk_update(
        self, using_transactions, dry_run, raise_errors, batch_size=None, result=None
    ):
        """
        Updates objects by calling ``bulk_update``.
        """
        if len(self.update_instances) > 0 and (using_transactions or not dry_run):
            try:
                self._meta.model.objects.bulk_update(
                    self.update_instances,
                    self.get_bulk_update_fields(),
                    batch_size=batch_size,
                )
            except Exception as e:
                self.handle_import_error(result, e, raise_errors)
            finally:
                self.update_instances.clear()

    def bulk_delete(self, using_transactions, dry_run, raise_errors, result=None):
        """
        Deletes objects by filtering on a list of instances to be deleted,
        then calling ``delete()`` on the entire queryset.
        """
        if len(self.delete_instances) > 0 and (using_transactions or not dry_run):
            try:
                delete_ids = [o.pk for o in self.delete_instances]
                self._meta.model.objects.filter(pk__in=delete_ids).delete()
            except Exception as e:
                self.handle_import_error(result, e, raise_errors)
            finally:
                self.delete_instances.clear()

    def validate_instance(
        self, instance, import_validation_errors=None, validate_unique=True
    ):
        """
        Takes any validation errors that were raised by
        :meth:`~import_export.resources.Resource.import_instance`, and combines them
        with validation errors raised by the instance's ``full_clean()``
        method. The combined errors are then re-raised as single, multi-field
        ValidationError.

        If the ``clean_model_instances`` option is False, the instances's
        ``full_clean()`` method is not called, and only the errors raised by
        ``import_instance()`` are re-raised.
        """
        if import_validation_errors is None:
            errors = {}
        else:
            errors = import_validation_errors.copy()
        if self._meta.clean_model_instances:
            try:
                instance.full_clean(
                    exclude=errors.keys(),
                    validate_unique=validate_unique,
                )
            except ValidationError as e:
                errors = e.update_error_dict(errors)

        if errors:
            raise ValidationError(errors)

    def save_instance(self, instance, is_create, row, **kwargs):
        r"""
        Takes care of saving the object to the database.

        Objects can be created in bulk if ``use_bulk`` is enabled.

        :param instance: The instance of the object to be persisted.

        :param is_create: A boolean flag to indicate whether this is a new object
                          to be created, or an existing object to be updated.

        :param row: A dict representing the import row.

        :param \**kwargs:
            See :meth:`import_row
        """
        self.before_save_instance(instance, row, **kwargs)
        if self._meta.use_bulk:
            if is_create:
                self.create_instances.append(instance)
            else:
                self.update_instances.append(instance)
        elif not self._is_using_transactions(kwargs) and self._is_dry_run(kwargs):
            # we don't have transactions and we want to do a dry_run
            pass
        else:
            self.do_instance_save(instance, is_create)
        self.after_save_instance(instance, row, **kwargs)

    def do_instance_save(self, instance, is_create):
        """
        A method specifically to provide a single overridable hook for the instance
        save operation.
        For example, this can be overridden to implement update_or_create().

        :param instance: The model instance to be saved.
        :param is_create: A boolean flag to indicate whether this is a new object
                          to be created, or an existing object to be updated.
        """
        instance.save()

    def before_save_instance(self, instance, row, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def after_save_instance(self, instance, row, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def delete_instance(self, instance, row, **kwargs):
        r"""
        Calls :meth:`instance.delete` as long as ``dry_run`` is not set.
        If ``use_bulk`` then instances are appended to a list for bulk import.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        self.before_delete_instance(instance, row, **kwargs)
        if self._meta.use_bulk:
            self.delete_instances.append(instance)
        elif not self._is_using_transactions(kwargs) and self._is_dry_run(kwargs):
            # we don't have transactions and we want to do a dry_run
            pass
        else:
            instance.delete()
        self.after_delete_instance(instance, row, **kwargs)

    def before_delete_instance(self, instance, row, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def after_delete_instance(self, instance, row, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def import_field(self, field, instance, row, is_m2m=False, **kwargs):
        r"""
        Handles persistence of the field data.

        :param field: A :class:`import_export.fields.Field` instance.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param is_m2m: A boolean value indicating whether or not this is a
          many-to-many field.

        :param \**kwargs:
            See :meth:`import_row`
        """
        if not field.attribute:
            logger.debug(f"skipping field '{field}' - field attribute is not defined")
            return
        if field.column_name not in row:
            logger.debug(
                f"skipping field '{field}' "
                f"- column name '{field.column_name}' is not present in row"
            )
            return
        field.save(instance, row, is_m2m, **kwargs)

    def get_import_fields(self):
        import_fields = []
        missing = object()
        for field_name in self.get_import_order():
            field = self.fields.get(field_name, missing)
            if field is not missing:
                import_fields.append(field)
                continue
            # issue 1815
            # allow for fields to be referenced by column_name in `fields` list
            for field in self.fields.values():
                if field.column_name == field_name:
                    import_fields.append(field)
                    continue
        return import_fields

    def import_obj(self, obj, data, dry_run, **kwargs):
        warn(
            "The 'import_obj' method is deprecated and will be replaced "
            "with 'import_instance(self, instance, row, **kwargs)' "
            "in a future release.  Refer to Release Notes for details.",
            DeprecationWarning,
            stacklevel=2,
        )
        if dry_run is True:
            kwargs.update({"dry_run": dry_run})
        self.import_instance(obj, data, **kwargs)

    def import_instance(self, instance, row, **kwargs):
        r"""
        Traverses every field in this Resource and calls
        :meth:`~import_export.resources.Resource.import_field`. If
        ``import_field()`` results in a ``ValueError`` being raised for
        one of more fields, those errors are captured and reraised as a single,
        multi-field ValidationError.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        errors = {}
        for field in self.get_import_fields():
            if isinstance(field.widget, widgets.ManyToManyWidget):
                continue
            try:
                self.import_field(field, instance, row, **kwargs)
            except ValueError as e:
                errors[field.attribute] = ValidationError(force_str(e), code="invalid")
        if errors:
            raise ValidationError(errors)

    def save_m2m(self, instance, row, **kwargs):
        r"""
        Saves m2m fields.

        Model instance need to have a primary key value before
        a many-to-many relationship can be used.

        :param instance: A new or existing model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        using_transactions = self._is_using_transactions(kwargs)
        dry_run = self._is_dry_run(kwargs)
        if (not using_transactions and dry_run) or self._meta.use_bulk:
            # we don't have transactions and we want to do a dry_run
            # OR use_bulk is enabled (m2m operations are not supported
            # for bulk operations)
            pass
        else:
            for field in self.get_import_fields():
                if not isinstance(field.widget, widgets.ManyToManyWidget):
                    continue
                self.import_field(field, instance, row, True)

    def for_delete(self, row, instance):
        """
        Returns ``True`` if ``row`` importing should delete instance.

        Default implementation returns ``False``.
        Override this method to handle deletion.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param instance: A new or existing model instance.
        """
        return False

    def skip_row(self, instance, original, row, import_validation_errors=None):
        """
        Returns ``True`` if ``row`` importing should be skipped.

        Default implementation returns ``False`` unless skip_unchanged == True
        and skip_diff == False.

        If skip_diff is True, then no comparisons can be made because ``original``
        will be None.

        When left unspecified, skip_diff and skip_unchanged both default to ``False``,
        and rows are never skipped.

        By default, rows are not skipped if validation errors have been detected
        during import.  You can change this behavior and choose to ignore validation
        errors by overriding this method.

        Override this method to handle skipping rows meeting certain
        conditions.

        Use ``super`` if you want to preserve default handling while overriding
        ::

            class YourResource(ModelResource):
                def skip_row(self, instance, original,
                             row, import_validation_errors=None):
                    # Add code here
                    return super().skip_row(instance, original, row,
                                            import_validation_errors=import_validation_errors)

        :param instance: A new or updated model instance.

        :param original: The original persisted model instance.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param import_validation_errors: A ``dict`` containing key / value data for any
          identified validation errors.
        """
        if (
            not self._meta.skip_unchanged
            or self._meta.skip_diff
            or import_validation_errors
        ):
            return False
        for field in self.get_import_fields():
            # For fields that are models.fields.related.ManyRelatedManager
            # we need to compare the results
            if isinstance(field.widget, widgets.ManyToManyWidget):
                # #1437 - handle m2m field not present in import file
                if field.column_name not in row.keys():
                    continue
                # m2m instance values are taken from the 'row' because they
                # have not been written to the 'instance' at this point
                instance_values = list(field.clean(row))
                original_values = (
                    [] if original.pk is None else list(field.get_value(original).all())
                )
                if len(instance_values) != len(original_values):
                    return False

                if sorted(v.pk for v in instance_values) != sorted(
                    v.pk for v in original_values
                ):
                    return False
            elif field.get_value(instance) != field.get_value(original):
                return False
        return True

    def get_diff_headers(self):
        """
        Diff representation headers.
        """
        return [force_str(field.column_name) for field in self.get_import_fields()]

    def before_import(self, dataset, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param dataset: A ``tablib.Dataset``.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def after_import(self, dataset, result, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param dataset: A ``tablib.Dataset``.

        :param result: A :class:`import_export.results.Result` implementation
          containing a summary of the import.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def before_import_row(self, row, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def after_import_row(self, row, row_result, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param row_result: A ``RowResult`` instance.
          References the persisted ``instance`` as an attribute.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def after_import_instance(self, instance, new, row_number=None, **kwargs):
        warn(
            "The 'after_import_instance' method is deprecated and will be replaced "
            "with 'after_init_instance(self, instance, new, row, **kwargs)' "
            "in a future release.  Refer to Release Notes for details.",
            DeprecationWarning,
            stacklevel=2,
        )
        if row_number is not None:
            kwargs.update({"row_number": row_number})
        self.after_init_instance(instance, new, None, **kwargs)

    def after_init_instance(self, instance, new, row, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param instance: A new or existing model instance.

        :param new: a boolean flag indicating whether instance is new or existing.

        :param row: A ``dict`` containing key / value data for the row to be imported.

        :param \**kwargs:
            See :meth:`import_row`
        """
        pass

    def handle_import_error(self, result, error, raise_errors=False):
        logger.debug(error, exc_info=error)
        if result:
            result.append_base_error(self.get_error_result_class()(error))
        if raise_errors:
            raise exceptions.ImportError(error)

    def import_row(self, row, instance_loader, **kwargs):
        r"""
        Imports data from ``tablib.Dataset``. Refer to :doc:`import_workflow`
        for a more complete description of the whole import process.

        :param row: A ``dict`` of the 'row' to import.
          A row is a dict of data fields so can be a csv line, a JSON object,
          a YAML object etc.

        :param instance_loader: The instance loader to be used to load the model
          instance associated with the row (if there is one).

        :param \**kwargs:
            See below.

        :Keyword Arguments:
            * dry_run (``boolean``) --
              A True value means that no data should be persisted.
            * use_transactions (``boolean``) --
              A True value means that transactions will be rolled back.
            * row_number  (``int``) --
              The index of the row being imported.
        """
        skip_diff = self._meta.skip_diff

        if not self._meta.store_instance:
            self._meta.store_instance = kwargs.get(
                "retain_instance_in_row_result", False
            )

        row_result = self.get_row_result_class()()
        if self._meta.store_row_values:
            row_result.row_values = row
        original = None
        try:
            self.before_import_row(row, **kwargs)
            instance, new = self.get_or_init_instance(instance_loader, row)
            self.after_init_instance(instance, new, row, **kwargs)
            if new:
                row_result.import_type = RowResult.IMPORT_TYPE_NEW
            else:
                row_result.import_type = RowResult.IMPORT_TYPE_UPDATE
            if not skip_diff:
                original = deepcopy(instance)
                diff = self.get_diff_class()(self, original, new)
            if self.for_delete(row, instance):
                if new:
                    row_result.import_type = RowResult.IMPORT_TYPE_SKIP
                    if not skip_diff:
                        diff.compare_with(self, None)
                else:
                    row_result.import_type = RowResult.IMPORT_TYPE_DELETE
                    row_result.add_instance_info(instance)
                    if self._meta.store_instance:
                        # create a copy before deletion so id fields are retained
                        row_result.instance = deepcopy(instance)
                    self.delete_instance(instance, row, **kwargs)
                    if not skip_diff:
                        diff.compare_with(self, None)
            else:
                import_validation_errors = {}
                try:
                    self.import_instance(instance, row, **kwargs)
                except ValidationError as e:
                    # Validation errors are passed on to validate_instance(),
                    # where they can be combined with model instance validation
                    # errors if necessary
                    import_validation_errors = e.update_error_dict(
                        import_validation_errors
                    )

                if self.skip_row(instance, original, row, import_validation_errors):
                    row_result.import_type = RowResult.IMPORT_TYPE_SKIP
                else:
                    self.validate_instance(instance, import_validation_errors)
                    self.save_instance(instance, new, row, **kwargs)
                    self.save_m2m(instance, row, **kwargs)
                row_result.add_instance_info(instance)
                if self._meta.store_instance:
                    row_result.instance = instance
                if not skip_diff:
                    diff.compare_with(self, instance)
                    if not new:
                        row_result.original = original

            if not skip_diff and not self._meta.skip_html_diff:
                row_result.diff = diff.as_html()
            self.after_import_row(row, row_result, **kwargs)

        except ValidationError as e:
            row_result.import_type = RowResult.IMPORT_TYPE_INVALID
            row_result.validation_error = e
        except Exception as e:
            row_result.import_type = RowResult.IMPORT_TYPE_ERROR
            # There is no point logging a transaction error for each row
            # when only the original error is likely to be relevant
            if not isinstance(e, TransactionManagementError):
                logger.debug(e, exc_info=True)
            row_result.errors.append(
                self.get_error_result_class()(e, row=row, number=kwargs["row_number"])
            )

        return row_result

    def import_data(
        self,
        dataset,
        dry_run=False,
        raise_errors=False,
        use_transactions=None,
        collect_failed_rows=False,
        rollback_on_validation_errors=False,
        **kwargs,
    ):
        r"""
        Imports data from ``tablib.Dataset``. Refer to :doc:`import_workflow`
        for a more complete description of the whole import process.

        :param dataset: A ``tablib.Dataset``.

        :param raise_errors: Whether errors should be printed to the end user
                             or raised regularly.

        :param use_transactions: If ``True`` the import process will be processed
                                 inside a transaction.

        :param collect_failed_rows:
          If ``True`` the import process will create a new dataset object comprising
          failed rows and errors.
          This can be useful for debugging purposes but will cause higher memory usage
          for larger datasets.
          See :attr:`~import_export.results.Result.failed_dataset`.

        :param rollback_on_validation_errors: If both ``use_transactions`` and
          ``rollback_on_validation_errors`` are set to ``True``, the import process will
          be rolled back in case of ValidationError.

        :param dry_run: If ``dry_run`` is set, or an error occurs, if a transaction
            is being used, it will be rolled back.

        :param \**kwargs:
            Metadata which may be associated with the import.
        """

        if use_transactions is None:
            use_transactions = self.get_use_transactions()

        db_connection = self.get_db_connection_name()
        connection = connections[db_connection]
        supports_transactions = getattr(
            connection.features, "supports_transactions", False
        )

        if use_transactions and not supports_transactions:
            raise ImproperlyConfigured

        using_transactions = (use_transactions or dry_run) and supports_transactions

        if self._meta.batch_size is not None and (
            not isinstance(self._meta.batch_size, int) or self._meta.batch_size < 0
        ):
            raise ValueError("Batch size must be a positive integer")

        with atomic_if_using_transaction(using_transactions, using=db_connection):
            result = self.import_data_inner(
                dataset,
                dry_run,
                raise_errors,
                using_transactions,
                collect_failed_rows,
                **kwargs,
            )
            if using_transactions and (
                dry_run
                or result.has_errors()
                or (rollback_on_validation_errors and result.has_validation_errors())
            ):
                set_rollback(True, using=db_connection)
            return result

    def import_data_inner(
        self,
        dataset,
        dry_run,
        raise_errors,
        using_transactions,
        collect_failed_rows,
        **kwargs,
    ):
        result = self.get_result_class()()
        result.diff_headers = self.get_diff_headers()
        result.total_rows = len(dataset)
        db_connection = self.get_db_connection_name()

        try:
            with atomic_if_using_transaction(using_transactions, using=db_connection):
                self.before_import(dataset, **kwargs)
            self._check_import_id_fields(dataset.headers)
        except Exception as e:
            self.handle_import_error(result, e, raise_errors)

        instance_loader = self._meta.instance_loader_class(self, dataset)

        # Update the total in case the dataset was altered by before_import()
        result.total_rows = len(dataset)

        if collect_failed_rows:
            result.add_dataset_headers(dataset.headers)

        for i, data_row in enumerate(dataset, 1):
            row = OrderedDict(zip(dataset.headers, data_row))
            with atomic_if_using_transaction(
                using_transactions and not self._meta.use_bulk, using=db_connection
            ):
                kwargs.update(
                    {
                        "dry_run": dry_run,
                        "using_transactions": using_transactions,
                        "row_number": i,
                    }
                )
                row_result = self.import_row(
                    row,
                    instance_loader,
                    **kwargs,
                )
            if self._meta.use_bulk:
                # persist a batch of rows
                # because this is a batch, any exceptions are logged and not associated
                # with a specific row
                if len(self.create_instances) == self._meta.batch_size:
                    with atomic_if_using_transaction(
                        using_transactions, using=db_connection
                    ):
                        self.bulk_create(
                            using_transactions,
                            dry_run,
                            raise_errors,
                            batch_size=self._meta.batch_size,
                            result=result,
                        )
                if len(self.update_instances) == self._meta.batch_size:
                    with atomic_if_using_transaction(
                        using_transactions, using=db_connection
                    ):
                        self.bulk_update(
                            using_transactions,
                            dry_run,
                            raise_errors,
                            batch_size=self._meta.batch_size,
                            result=result,
                        )
                if len(self.delete_instances) == self._meta.batch_size:
                    with atomic_if_using_transaction(
                        using_transactions, using=db_connection
                    ):
                        self.bulk_delete(
                            using_transactions, dry_run, raise_errors, result=result
                        )

            result.increment_row_result_total(row_result)

            if row_result.errors:
                result.append_error_row(i, row, row_result.errors)
                if collect_failed_rows:
                    result.append_failed_row(row, row_result.errors[0])
                if raise_errors:
                    raise exceptions.ImportError(
                        row_result.errors[-1].error, number=i, row=row
                    )
            elif row_result.validation_error:
                result.append_invalid_row(i, row, row_result.validation_error)
                if collect_failed_rows:
                    result.append_failed_row(row, row_result.validation_error)
                if raise_errors:
                    raise exceptions.ImportError(
                        row_result.validation_error, number=i, row=row
                    )
            if (
                row_result.import_type != RowResult.IMPORT_TYPE_SKIP
                or self._meta.report_skipped
            ):
                result.append_row_result(row_result)

        if self._meta.use_bulk:
            # bulk persist any instances which are still pending
            with atomic_if_using_transaction(using_transactions, using=db_connection):
                self.bulk_create(
                    using_transactions, dry_run, raise_errors, result=result
                )
                self.bulk_update(
                    using_transactions, dry_run, raise_errors, result=result
                )
                self.bulk_delete(
                    using_transactions, dry_run, raise_errors, result=result
                )

        try:
            with atomic_if_using_transaction(using_transactions, using=db_connection):
                self.after_import(dataset, result, **kwargs)
        except Exception as e:
            self.handle_import_error(result, e, raise_errors)

        return result

    def get_import_order(self):
        return self._get_ordered_field_names("import_order")

    def get_export_order(self):
        return self._get_ordered_field_names("export_order")

    def before_export(self, queryset, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param queryset: The queryset for export.

        :param \**kwargs:
            Metadata which may be associated with the export.
        """
        pass

    def after_export(self, queryset, dataset, **kwargs):
        r"""
        Override to add additional logic. Does nothing by default.

        :param queryset: The queryset for export.

        :param dataset: A ``tablib.Dataset``.

        :param \**kwargs:
            Metadata which may be associated with the export.
        """
        pass

    def filter_export(self, queryset, **kwargs):
        r"""
        Override to filter an export queryset.

        :param queryset: The queryset for export.

        :param \**kwargs:
            Metadata which may be associated with the export.

        :returns: The filtered queryset.
        """
        return queryset

    def export_field(self, field, instance, **kwargs):
        field_name = self.get_field_name(field)
        dehydrate_method = field.get_dehydrate_method(field_name)

        if callable(dehydrate_method):
            method = dehydrate_method
        else:
            method = getattr(self, dehydrate_method, None)

        if method is not None:
            return method(instance)
        return field.export(instance, **kwargs)

    def get_export_fields(self, selected_fields=None):
        fields_ = selected_fields if selected_fields else self.fields
        export_fields = []
        export_order = self.get_export_order()
        for field_name in export_order:
            if field_name in fields_:
                field = self._select_field(field_name)
                if field is not None:
                    export_fields.append(field)
        return export_fields

    def export_resource(self, instance, selected_fields=None, **kwargs):
        export_fields = self.get_export_fields(selected_fields)
        return [self.export_field(field, instance, **kwargs) for field in export_fields]

    def get_export_headers(self, selected_fields=None):
        export_fields = self.get_export_fields(selected_fields)
        return [force_str(field.column_name) for field in export_fields if field]

    def get_user_visible_fields(self):
        return self.get_import_fields()

    def iter_queryset(self, queryset):
        if not isinstance(queryset, QuerySet):
            yield from queryset
        elif queryset._prefetch_related_lookups:
            # Django's queryset.iterator ignores prefetch_related which might result
            # in an excessive amount of db calls. Therefore we use pagination
            # as a work-around
            if not queryset.query.order_by:
                # Paginator() throws a warning if there is no sorting
                # attached to the queryset
                queryset = queryset.order_by("pk")
            paginator = Paginator(queryset, self.get_chunk_size())
            for index in range(paginator.num_pages):
                yield from paginator.get_page(index + 1)
        else:
            yield from queryset.iterator(chunk_size=self.get_chunk_size())

    def export(self, queryset=None, **kwargs):
        """
        Exports a resource.

        :param queryset: The queryset for export (optional).

        :returns: A ``tablib.Dataset``.
        """
        self.before_export(queryset, **kwargs)

        if queryset is None:
            queryset = self.get_queryset()
        queryset = self.filter_export(queryset, **kwargs)
        export_fields = kwargs.get("export_fields", None)
        headers = self.get_export_headers(selected_fields=export_fields)
        dataset = tablib.Dataset(headers=headers)

        for obj in self.iter_queryset(queryset):
            r = self.export_resource(obj, selected_fields=export_fields, **kwargs)
            dataset.append(r)

        self.after_export(queryset, dataset, **kwargs)

        return dataset

    def _select_field(self, target_field_name):
        # select field from fields based on either declared name or column name
        missing = object()
        field = self.fields.get(target_field_name, missing)
        if field is not missing:
            return field

        for field_name, field in self.fields.items():
            if target_field_name == field.column_name:
                return field
        # it should have been possible to identify the declared field
        # but warn if not
        warn(f"cannot identify field for export with name '{target_field_name}'")

    def _get_ordered_field_names(self, order_field):
        """
        Return a list of field names, respecting any defined ordering.
        """
        # get any declared 'order' fields
        order_fields = tuple(getattr(self._meta, order_field) or ())
        # get any defined fields
        defined_fields = order_fields + tuple(getattr(self._meta, "fields") or ())

        order = []
        [order.append(f) for f in defined_fields if f not in order]
        declared_fields = []
        for field_name, field in self.fields.items():
            if field_name not in order and field.column_name not in order:
                declared_fields.append(field_name)
        return tuple(order) + tuple(declared_fields)

    def _is_using_transactions(self, kwargs):
        return kwargs.get("using_transactions", False)

    def _is_dry_run(self, kwargs):
        return kwargs.get("dry_run", False)

    def _check_import_id_fields(self, headers):
        """
        Provides a safety check with a meaningful error message for cases where
        the ``import_id_fields`` declaration contains a field which is not in the
        dataset.  For most use-cases this is an error, so we detect and raise.
        There are conditions, such as 'dynamic fields' where this does not apply.
        See issue 1834 for more information.
        """
        import_id_fields = []
        missing_fields = []
        missing_headers = []

        if self.get_import_id_fields() == ["id"]:
            # this is the default case, so ok if not present
            return

        for field_name in self.get_import_id_fields():
            if field_name not in self.fields:
                missing_fields.append(field_name)
            else:
                import_id_fields.append(self.fields[field_name])

        if missing_fields:
            raise exceptions.FieldError(
                _(
                    "The following fields are declared in 'import_id_fields' but "
                    "are not present in the resource fields: %s"
                    % ", ".join(missing_fields)
                )
            )

        for field in import_id_fields:
            if not headers or field.column_name not in headers:
                # escape to be safe (exception could end up in logs)
                col = escape(field.column_name)
                missing_headers.append(col)

        if missing_headers:
            raise exceptions.FieldError(
                _(
                    "The following fields are declared in 'import_id_fields' but "
                    "are not present in the file headers: %s"
                    % ", ".join(missing_headers)
                )
            )


class ModelResource(Resource, metaclass=ModelDeclarativeMetaclass):
    """
    ModelResource is Resource subclass for handling Django models.
    """

    DEFAULT_RESOURCE_FIELD = Field

    WIDGETS_MAP = {
        "ManyToManyField": "get_m2m_widget",
        "OneToOneField": "get_fk_widget",
        "ForeignKey": "get_fk_widget",
        "CharField": widgets.CharWidget,
        "DecimalField": widgets.DecimalWidget,
        "DateTimeField": widgets.DateTimeWidget,
        "DateField": widgets.DateWidget,
        "TimeField": widgets.TimeWidget,
        "DurationField": widgets.DurationWidget,
        "FloatField": widgets.FloatWidget,
        "IntegerField": widgets.IntegerWidget,
        "PositiveIntegerField": widgets.IntegerWidget,
        "BigIntegerField": widgets.IntegerWidget,
        "PositiveBigIntegerField": widgets.IntegerWidget,
        "PositiveSmallIntegerField": widgets.IntegerWidget,
        "SmallIntegerField": widgets.IntegerWidget,
        "SmallAutoField": widgets.IntegerWidget,
        "AutoField": widgets.IntegerWidget,
        "BigAutoField": widgets.IntegerWidget,
        "NullBooleanField": widgets.BooleanWidget,
        "BooleanField": widgets.BooleanWidget,
        "JSONField": widgets.JSONWidget,
    }

    @classmethod
    def get_m2m_widget(cls, field):
        """
        Prepare widget for m2m field
        """
        return functools.partial(
            widgets.ManyToManyWidget, model=get_related_model(field)
        )

    @classmethod
    def get_fk_widget(cls, field):
        """
        Prepare widget for fk and o2o fields
        """

        model = get_related_model(field)

        use_natural_foreign_keys = (
            has_natural_foreign_key(model) and cls._meta.use_natural_foreign_keys
        )

        return functools.partial(
            widgets.ForeignKeyWidget,
            model=model,
            use_natural_foreign_keys=use_natural_foreign_keys,
        )

    @classmethod
    def widget_from_django_field(cls, f, default=widgets.Widget):
        """
        Returns the widget that would likely be associated with each
        Django type.

        Includes mapping of Postgres Array field. In the case that
        psycopg2 is not installed, we consume the error and process the field
        regardless.
        """
        result = default
        internal_type = ""
        if callable(getattr(f, "get_internal_type", None)):
            internal_type = f.get_internal_type()

        widget_result = cls.WIDGETS_MAP.get(internal_type)
        if widget_result is not None:
            result = widget_result
            if isinstance(result, str):
                result = getattr(cls, result)(f)
        else:
            # issue 1804
            # The field class may be in a third party library as a subclass
            # of a standard field class.
            # iterate base classes to determine the correct widget class to use.
            for base_class in f.__class__.__mro__:
                widget_result = cls.WIDGETS_MAP.get(base_class.__name__)
                if widget_result is not None:
                    result = widget_result
                    if isinstance(result, str):
                        result = getattr(cls, result)(f)
                    break

            try:
                from django.contrib.postgres.fields import ArrayField
            except ImportError:
                # ImportError: No module named psycopg2.extras
                class ArrayField:
                    pass

            if isinstance(f, ArrayField):
                return widgets.SimpleArrayWidget

        return result

    @classmethod
    def widget_kwargs_for_field(cls, field_name, django_field):
        """
        Returns widget kwargs for given field_name.
        """
        widget_kwargs = {}
        if cls._meta.widgets:
            cls_kwargs = cls._meta.widgets.get(field_name, {})
            widget_kwargs.update(cls_kwargs)
        if (
            issubclass(django_field.__class__, fields.CharField)
            and django_field.blank is True
        ):
            widget_kwargs.update({"coerce_to_string": True, "allow_blank": True})
        return widget_kwargs

    @classmethod
    def field_from_django_field(cls, field_name, django_field, readonly):
        """
        Returns a Resource Field instance for the given Django model field.
        """

        FieldWidget = cls.widget_from_django_field(django_field)
        widget_kwargs = cls.widget_kwargs_for_field(field_name, django_field)

        attribute = field_name
        column_name = field_name
        # To solve #974, #2107
        # Check if there's a custom widget configuration for this field
        has_custom_widget_declaration = (
            cls._meta.widgets and field_name in cls._meta.widgets
        )
        if (
            isinstance(django_field, ForeignKey)
            and "__" not in column_name
            and not cls._meta.use_natural_foreign_keys
            and not has_custom_widget_declaration
        ):
            attribute += "_id"
            widget_kwargs["key_is_id"] = True

        field = cls.DEFAULT_RESOURCE_FIELD(
            attribute=attribute,
            column_name=column_name,
            widget=FieldWidget(**widget_kwargs),
            readonly=readonly,
            default=django_field.default,
        )
        return field

    def get_queryset(self):
        """
        Returns a queryset of all objects for this model. Override this if you
        want to limit the returned queryset.
        """
        return self._meta.model.objects.all()

    def init_instance(self, row=None):
        """
        Initializes a new Django model.
        """
        return self._meta.model()

    def after_import(self, dataset, result, **kwargs):
        """
        Reset the SQL sequences after new objects are imported
        """
        # Adapted from django's loaddata
        dry_run = self._is_dry_run(kwargs)
        if not dry_run and any(
            r.import_type == RowResult.IMPORT_TYPE_NEW for r in result.rows
        ):
            db_connection = self.get_db_connection_name()
            connection = connections[db_connection]
            sequence_sql = connection.ops.sequence_reset_sql(
                no_style(), [self._meta.model]
            )
            if sequence_sql:
                cursor = connection.cursor()
                try:
                    for line in sequence_sql:
                        cursor.execute(line)
                finally:
                    cursor.close()

    @classmethod
    def get_display_name(cls):
        if hasattr(cls._meta, "name"):
            return cls._meta.name
        return cls.__name__


def modelresource_factory(
    model,
    resource_class=ModelResource,
    meta_options=None,
    custom_fields=None,
    dehydrate_methods=None,
):
    """
    Factory for creating ``ModelResource`` class for given Django model.
    This factory function creates a ``ModelResource`` class dynamically, with support
    for custom fields, methods.

    :param model: Django model class

    :param resource_class: Base resource class (default: ModelResource)

    :param meta_options: Meta options dictionary

    :param custom_fields: Dictionary mapping field names to Field object

    :param dehydrate_methods: Dictionary mapping field names
                              to dehydrate method (Callable)

    :returns: ModelResource class
    """

    def _create_dehydrate_func_wrapper(func):
        def wrapper(self, obj):
            return func(obj)

        return wrapper

    if meta_options is None:
        meta_options = {}

    if custom_fields is None:
        custom_fields = {}

    if dehydrate_methods is None:
        dehydrate_methods = {}

    for field_name, field in custom_fields.items():
        if not isinstance(field, Field):
            raise ValueError(
                f"custom_fields['{field_name}'] must be a Field instance, "
                f"got {type(field).__name__}"
            )

    meta_class_attrs = {**meta_options, "model": model}
    Meta = type("Meta", (object,), meta_class_attrs)

    resource_class_name = model.__name__ + "Resource"
    resource_class_attrs = {
        "Meta": Meta,
    }
    resource_class_attrs.update(custom_fields)

    for field_name, method in dehydrate_methods.items():
        if not callable(method):
            raise ValueError(
                f"dehydrate_methods['{field_name}'] must be callable, "
                f"got {type(method).__name__}"
            )

        method_name = f"dehydrate_{field_name}"
        resource_class_attrs[method_name] = _create_dehydrate_func_wrapper(method)

    metaclass = ModelDeclarativeMetaclass
    return metaclass(resource_class_name, (resource_class,), resource_class_attrs)
