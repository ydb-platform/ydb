import logging
import sys
import traceback
from collections import OrderedDict

from django.core.exceptions import NON_FIELD_ERRORS
from django.utils.encoding import force_str
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from tablib import Dataset

logger = logging.getLogger(__name__)


class Error:
    """
    Base class representing an Error arising from error during data import.
    """

    def __init__(self, error, row=None, number=None):
        """
        :param error: Instance of an Exception class.
        :param row: The row as a dict of fields and values (optional).
        :param number: The row number (optional).
        """
        self.error = error
        self.row = row
        self.number = number

    def __repr__(self):
        result = f"<Error: {self.error!r}"
        if self.row is not None:
            result += f" at row {self.row}"
        if self.number is not None:
            result += f" at number {self.number}"
        result += ">"
        return result

    @cached_property
    def traceback(self):
        if sys.version_info >= (3, 10):
            lines = traceback.format_exception(self.error)
        else:
            lines = traceback.format_exception(
                None, self.error, self.error.__traceback__
            )
        return "".join(lines)


class RowResult:
    """Container for values relating to a row import."""

    IMPORT_TYPE_UPDATE = "update"
    IMPORT_TYPE_NEW = "new"
    IMPORT_TYPE_DELETE = "delete"
    IMPORT_TYPE_SKIP = "skip"
    IMPORT_TYPE_ERROR = "error"
    IMPORT_TYPE_INVALID = "invalid"

    valid_import_types = frozenset(
        [
            IMPORT_TYPE_NEW,
            IMPORT_TYPE_UPDATE,
            IMPORT_TYPE_DELETE,
            IMPORT_TYPE_SKIP,
        ]
    )

    def __init__(self):
        #: An instance of :class:`~import_export.results.Error` which may have been
        #: raised during import.
        self.errors = []

        #: Contains any ValidationErrors which may have been raised during import.
        self.validation_error = None

        #: A HTML representation of the difference between the 'original' and
        #: 'updated' model instance.
        self.diff = None

        #: A string identifier which identifies what type of import was performed.
        self.import_type = None

        #: Retain the raw values associated with each imported row.
        self.row_values = {}

        #: The instance id (used in Admin UI)
        self.object_id = None

        #: The object representation (used in Admin UI)
        self.object_repr = None

        #: A reference to the model instance which was created, updated or deleted.
        self.instance = None

        #: A reference to the model instance before updates were applied.
        #: This value is only set for updates.
        self.original = None

    def is_update(self):
        """
        :return: ``True`` if import type is 'update', otherwise ``False``.
        """
        return self.import_type == RowResult.IMPORT_TYPE_UPDATE

    def is_new(self):
        """
        :return: ``True`` if import type is 'new', otherwise ``False``.
        """
        return self.import_type == RowResult.IMPORT_TYPE_NEW

    def is_delete(self):
        """
        :return: ``True`` if import type is 'delete', otherwise ``False``.
        """
        return self.import_type == RowResult.IMPORT_TYPE_DELETE

    def is_skip(self):
        """
        :return: ``True`` if import type is 'skip', otherwise ``False``.
        """
        return self.import_type == RowResult.IMPORT_TYPE_SKIP

    def is_error(self):
        """
        :return: ``True`` if import type is 'error', otherwise ``False``.
        """
        return self.import_type == RowResult.IMPORT_TYPE_ERROR

    def is_invalid(self):
        """
        :return: ``True`` if import type is 'invalid', otherwise ``False``.
        """
        return self.import_type == RowResult.IMPORT_TYPE_INVALID

    def is_valid(self):
        """
        :return: ``True`` if import type is not 'error' or 'invalid', otherwise
          ``False``.
        """
        return self.import_type in self.valid_import_types

    def add_instance_info(self, instance):
        if instance is not None:
            # Add object info to RowResult (e.g. for LogEntry)
            self.object_id = getattr(instance, "pk", None)
            try:
                self.object_repr = force_str(instance)
            except Exception as e:
                logger.debug(_("call to force_str() on instance failed: %s" % str(e)))


class InvalidRow:
    """A row that resulted in one or more ``ValidationError``
    being raised during import."""

    def __init__(self, number, validation_error, values):
        self.number = number
        self.error = validation_error
        self.values = values
        try:
            self.error_dict = validation_error.message_dict
        except AttributeError:
            self.error_dict = {NON_FIELD_ERRORS: validation_error.messages}

    @property
    def field_specific_errors(self):
        """Returns a dictionary of field-specific validation errors for this row."""
        return {
            key: value
            for key, value in self.error_dict.items()
            if key != NON_FIELD_ERRORS
        }

    @property
    def non_field_specific_errors(self):
        """Returns a list of non field-specific validation errors for this row."""
        return self.error_dict.get(NON_FIELD_ERRORS, [])

    @property
    def error_count(self):
        """Returns the total number of validation errors for this row."""
        count = 0
        for error_list in self.error_dict.values():
            count += len(error_list)
        return count

    def __repr__(self):
        return (
            f"<{type(self).__name__}(row={self.number}, "
            f"error={self.error!r}, "
            f"error_count={self.error_count})>"
        )


class ErrorRow:
    """A row that resulted in one or more errors being raised during import."""

    def __init__(self, number, errors):
        #: The row number
        self.number = number
        #: A list of errors associated with the row
        self.errors = errors


class Result:
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.base_errors = []
        self.diff_headers = []
        #: The rows associated with the result.
        self.rows = []
        #: The collection of rows which had validation errors.
        self.invalid_rows = []
        #: The collection of rows which had generic errors.
        self.error_rows = []
        #: A custom Dataset containing only failed rows and associated errors.
        self.failed_dataset = Dataset()
        self.totals = OrderedDict(
            [
                (RowResult.IMPORT_TYPE_NEW, 0),
                (RowResult.IMPORT_TYPE_UPDATE, 0),
                (RowResult.IMPORT_TYPE_DELETE, 0),
                (RowResult.IMPORT_TYPE_SKIP, 0),
                (RowResult.IMPORT_TYPE_ERROR, 0),
                (RowResult.IMPORT_TYPE_INVALID, 0),
            ]
        )
        self.total_rows = 0

    def valid_rows(self):
        return [r for r in self.rows if r.import_type in RowResult.valid_import_types]

    def append_row_result(self, row_result):
        self.rows.append(row_result)

    def append_base_error(self, error):
        self.base_errors.append(error)

    def add_dataset_headers(self, headers):
        headers = [] if not headers else headers
        self.failed_dataset.headers = headers + ["Error"]

    def append_failed_row(self, row, error):
        row_values = [v for (k, v) in row.items()]
        try:
            row_values.append(str(error.error))
        except AttributeError:
            row_values.append(str(error))
        self.failed_dataset.append(row_values)

    def append_invalid_row(self, number, row, validation_error):
        # NOTE: value order must match diff_headers order, so that row
        # values and column headers match in the UI when displayed
        values = tuple(row.get(col, "---") for col in self.diff_headers)
        self.invalid_rows.append(
            InvalidRow(number=number, validation_error=validation_error, values=values)
        )

    def append_error_row(self, number, row, errors):
        self.error_rows.append(ErrorRow(number=number, errors=errors))

    def increment_row_result_total(self, row_result):
        if row_result.import_type:
            self.totals[row_result.import_type] += 1

    def row_errors(self):
        return [(i + 1, row.errors) for i, row in enumerate(self.rows) if row.errors]

    def has_errors(self):
        """Returns a boolean indicating whether the import process resulted in
        any critical (non-validation) errors for this result."""
        return bool(self.base_errors or self.row_errors())

    def has_validation_errors(self):
        """Returns a boolean indicating whether the import process resulted in
        any validation errors for this result."""
        return bool(self.invalid_rows)

    def __iter__(self):
        return iter(self.rows)
