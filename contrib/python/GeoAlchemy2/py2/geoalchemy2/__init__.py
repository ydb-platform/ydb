from .types import (  # NOQA
    Geometry,
    Geography,
    Raster
    )

from .elements import (  # NOQA
    WKTElement,
    WKBElement,
    RasterElement
    )

from .exc import ArgumentError

from . import functions  # NOQA
from . import types  # NOQA

import sqlalchemy
from sqlalchemy import Table, event
from sqlalchemy.sql import select, func, expression, text

from packaging import version

_SQLALCHEMY_VERSION_BEFORE_14 = version.parse(sqlalchemy.__version__) < version.parse("1.4")


def _format_select_args(*args):
    if _SQLALCHEMY_VERSION_BEFORE_14:
        return [args]
    else:
        return args


def _setup_ddl_event_listeners():
    @event.listens_for(Table, "before_create")
    def before_create(target, connection, **kw):
        dispatch("before-create", target, connection)

    @event.listens_for(Table, "after_create")
    def after_create(target, connection, **kw):
        dispatch("after-create", target, connection)

    @event.listens_for(Table, "before_drop")
    def before_drop(target, connection, **kw):
        dispatch("before-drop", target, connection)

    @event.listens_for(Table, "after_drop")
    def after_drop(target, connection, **kw):
        dispatch("after-drop", target, connection)

    def dispatch(event, table, bind):
        if event in ('before-create', 'before-drop'):
            # Filter Geometry columns from the table with management=True
            # Note: Geography and PostGIS >= 2.0 don't need this
            gis_cols = [c for c in table.c if
                        isinstance(c.type, Geometry) and
                        c.type.management is True]

            # Find all other columns that are not managed Geometries
            regular_cols = [x for x in table.c if x not in gis_cols]

            # Save original table column list for later
            table.info["_saved_columns"] = table.c

            # Temporarily patch a set of columns not including the
            # managed Geometry columns
            column_collection = expression.ColumnCollection()
            for col in regular_cols:
                column_collection.add(col)
            table.columns = column_collection

            if event == 'before-drop':
                # Drop the managed Geometry columns
                for c in gis_cols:
                    if bind.dialect.name == 'sqlite':
                        drop_func = 'DiscardGeometryColumn'
                    elif bind.dialect.name == 'postgresql':
                        drop_func = 'DropGeometryColumn'
                    else:
                        raise ArgumentError('dialect {} is not supported'.format(bind.dialect.name))
                    args = [table.schema] if table.schema else []
                    args.extend([table.name, c.name])

                    stmt = select(*_format_select_args(getattr(func, drop_func)(*args)))
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)

        elif event == 'after-create':
            # Restore original column list including managed Geometry columns
            table.columns = table.info.pop('_saved_columns')

            for c in table.c:
                # Add the managed Geometry columns with AddGeometryColumn()
                if isinstance(c.type, Geometry) and c.type.management is True:
                    args = [table.schema] if table.schema else []
                    args.extend([
                        table.name,
                        c.name,
                        c.type.srid,
                        c.type.geometry_type,
                        c.type.dimension
                    ])
                    if c.type.use_typmod is not None:
                        args.append(c.type.use_typmod)
                    if bind.dialect.name == 'sqlite':
                        args.append(not c.type.nullable)

                    stmt = select(*_format_select_args(func.AddGeometryColumn(*args)))
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)

                # Add spatial indices for the Geometry and Geography columns
                if isinstance(c.type, (Geometry, Geography)) and \
                        c.type.spatial_index is True:
                    if bind.dialect.name == 'sqlite':
                        stmt = select(*_format_select_args(func.CreateSpatialIndex(table.name,
                                                                                   c.name)))
                        stmt = stmt.execution_options(autocommit=True)
                        bind.execute(stmt)
                    elif bind.dialect.name == 'postgresql':

                        index_name = '"idx_{}_{}"'.format(table.name, c.name)

                        if c.type.use_N_D_index:
                            gis_column = '"{}" gist_geometry_ops_nd'.format(c.name)
                        else:
                            gis_column = '"{}"'.format(c.name)

                        if table.schema:
                            table_name = '"{}"."{}"'.format(table.schema, table.name)
                        else:
                            table_name = '"{}"'.format(table.name)

                        sql = "CREATE INDEX {} ON {} USING GIST ({})".format(index_name,
                                                                             table_name,
                                                                             gis_column)

                        q = text(sql)

                        bind.execute(q)
                    else:
                        raise ArgumentError('dialect {} is not supported'.format(bind.dialect.name))

                if isinstance(c.type, (Geometry, Geography)) and c.type.spatial_index is False and \
                        c.type.use_N_D_index is True:
                    raise ArgumentError('Arg Error(use_N_D_index): spatial_index must be True')

                # Add spatial indices for the Raster columns
                #
                # Note the use of ST_ConvexHull since most raster operators are
                # based on the convex hull of the rasters.
                if isinstance(c.type, Raster) and c.type.spatial_index is True:
                    if table.schema:
                        q = text('CREATE INDEX "idx_%s_%s" ON "%s"."%s" '
                                 'USING GIST (ST_ConvexHull("%s"))' %
                                 (table.name, c.name, table.schema,
                                  table.name, c.name))
                    else:
                        q = text('CREATE INDEX "idx_%s_%s" ON "%s" '
                                 'USING GIST (ST_ConvexHull("%s"))' %
                                 (table.name, c.name, table.name, c.name))
                    bind.execute(q)

        elif event == 'after-drop':
            # Restore original column list including managed Geometry columns
            table.columns = table.info.pop('_saved_columns')


_setup_ddl_event_listeners()

# Get version number
__version__ = "UNKNOWN VERSION"
try:
    from pkg_resources import get_distribution, DistributionNotFound
    try:
        __version__ = get_distribution('GeoAlchemy2').version
    except DistributionNotFound:  # pragma: no cover
        pass  # pragma: no cover
except ImportError:  # pragma: no cover
    pass  # pragma: no cover
