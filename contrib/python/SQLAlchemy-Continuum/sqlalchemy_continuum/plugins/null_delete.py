from ..operation import Operation
from ..utils import is_internal_column, versioned_column_properties
from .base import Plugin


class NullDeletePlugin(Plugin):
    def should_nullify_column(self, version_obj, prop):
        """
        Return whether or not given column of given version object should
        be nullified (set to None) at the end of the transaction.

        :param version_obj:
            Version object to check the attribute nullification
        :paremt prop:
            SQLAlchemy ColumnProperty object
        """
        return (
            version_obj.operation_type == Operation.DELETE
            and not prop.columns[0].primary_key
            and not is_internal_column(version_obj, prop.key)
        )

    def after_create_version_object(self, uow, parent_obj, version_obj):
        for prop in versioned_column_properties(parent_obj):
            if self.should_nullify_column(version_obj, prop):
                setattr(version_obj, prop.key, None)
