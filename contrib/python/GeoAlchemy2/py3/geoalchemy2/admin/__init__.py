"""This module defines the functions used for administration tasks."""

from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Table
from sqlalchemy import event
from sqlalchemy.sql import func

from geoalchemy2.admin import dialects
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster


def select_dialect(dialect_name):
    """Select the dialect from its name."""
    known_dialects = {
        "geopackage": dialects.geopackage,
        "mysql": dialects.mysql,
        "mariadb": dialects.mariadb,
        "postgresql": dialects.postgresql,
        "sqlite": dialects.sqlite,
    }
    return known_dialects.get(dialect_name, dialects.common)


def setup_ddl_event_listeners():
    """Setup the DDL event listeners to automatically process spatial columns."""

    @event.listens_for(Table, "before_create")
    def before_create(table, bind, **kw):
        """Handle spatial indexes."""
        select_dialect(bind.dialect.name).before_create(table, bind, **kw)

    @event.listens_for(Table, "after_create")
    def after_create(table, bind, **kw):
        """Restore original column list including managed Geometry columns."""
        select_dialect(bind.dialect.name).after_create(table, bind, **kw)

    @event.listens_for(Table, "before_drop")
    def before_drop(table, bind, **kw):
        """Drop the managed Geometry columns."""
        select_dialect(bind.dialect.name).before_drop(table, bind, **kw)

    @event.listens_for(Table, "after_drop")
    def after_drop(table, bind, **kw):
        """Restore original column list including managed Geometry columns."""
        select_dialect(bind.dialect.name).after_drop(table, bind, **kw)

    @event.listens_for(Column, "after_parent_attach")
    def after_parent_attach(column, table):
        """Automatically add spatial indexes."""
        if not isinstance(table, Table):  # pragma: no cover
            # For old versions of SQLAlchemy, subqueries might trigger the after_parent_attach event
            # with a selectable as table, so we want to skip this case.
            return

        if not getattr(column.type, "nullable", True):
            column.nullable = column.type.nullable
        elif hasattr(column.type, "nullable"):
            column.type.nullable = column.nullable

        if not getattr(column.type, "spatial_index", False) and getattr(
            column.type, "use_N_D_index", False
        ):
            raise ArgumentError("Arg Error(use_N_D_index): spatial_index must be True")

        if not getattr(column.type, "spatial_index", False):
            # If the column is managed, the indexes are created after the table
            return

        try:
            if column.type._spatial_index_reflected:
                return
        except AttributeError:  # pragma: no cover
            pass

        kwargs = {
            "postgresql_using": "gist",
            "_column_flag": True,
        }
        col = column
        if _check_spatial_type(column.type, (Geometry, Geography)):
            if column.type.use_N_D_index:
                kwargs["postgresql_ops"] = {column.name: "gist_geometry_ops_nd"}
        elif _check_spatial_type(column.type, Raster):
            col = func.ST_ConvexHull(column)

        table.append_constraint(
            Index(
                _spatial_idx_name(table.name, column.name),
                col,
                **kwargs,
            )
        )

    @event.listens_for(Table, "column_reflect")
    def column_reflect(inspector, table, column_info):
        select_dialect(inspector.bind.dialect.name).reflect_geometry_column(
            inspector, table, column_info
        )


__all__ = [
    "dialects",
    "select_dialect",
    "setup_ddl_event_listeners",
]


def __dir__():
    return __all__
