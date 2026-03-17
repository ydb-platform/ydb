"""
The PropertyModTrackerPlugin offers a way of efficiently tracking individual
property modifications. With PropertyModTrackerPlugin you can make efficient
queries such as:

Find all versions of model X where user updated the property A or property B.

Find all versions of model X where user didn't update property A.

PropertyModTrackerPlugin adds separate modified tracking column for each
versioned column. So for example if you have versioned model called Article
with columns `name` and `content`, this plugin would add two additional boolean
columns `name_mod` and `content_mod` for the version model. When user commits
transactions the plugin automatically updates these boolean columns.
"""

from copy import copy

import sqlalchemy as sa
from .._compat import has_changes

from ..utils import versioned_column_properties
from .base import Plugin


class PropertyModTrackerPlugin(Plugin):
    column_suffix = '_mod'

    def create_mod_column(self, column):
        return sa.Column(
            column.name + self.column_suffix,
            sa.Boolean,
            key=column.key + self.column_suffix,
            default=False,
            server_default=sa.sql.expression.false(),
            nullable=False,
        )

    def after_build_version_table_columns(self, table_builder, columns):
        # Only create modification tracking columns for tables that are
        # associated with actual model classes. In other words do not create
        # mod tracking columns for association tables.
        if table_builder.model:
            for column in table_builder.parent_table.c:
                if (
                    not table_builder.manager.is_excluded_column(
                        table_builder.model, column
                    )
                    and not column.primary_key
                ):
                    columns.append(self.create_mod_column(column))

    def after_create_version_object(self, uow, parent_obj, version_obj):
        session = sa.orm.object_session(parent_obj)
        is_deleted = parent_obj in session.deleted

        for prop in versioned_column_properties(parent_obj):
            if has_changes(parent_obj, prop.key) or is_deleted:
                setattr(version_obj, prop.key + self.column_suffix, True)

    def after_construct_changeset(self, version_obj, changeset):
        for key in copy(changeset).keys():
            if key.endswith(self.column_suffix):
                del changeset[key]
