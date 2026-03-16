from sqlalchemy import Table as TableBase
from sqlalchemy.sql.base import (
    _bind_or_error, DialectKWArgs, Immutable
)
from sqlalchemy.sql.schema import SchemaItem
from sqlalchemy.sql.selectable import FromClause
from clickhouse_sqlalchemy.sql.selectable import (
    Join, Select
)
from . import ddl


class Table(TableBase):
    def drop(self, bind=None, checkfirst=False, if_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper,
                          self,
                          checkfirst=checkfirst, if_exists=if_exists)

    def join(self, right, onclause=None, isouter=False, full=False,
             type=None, strictness=None, distribution=None):
        return Join(self, right,
                    onclause=onclause, type=type,
                    isouter=isouter, full=full,
                    strictness=strictness, distribution=distribution)

    def select(self, whereclause=None, **params):
        return Select([self], whereclause, **params)

    @classmethod
    def _make_from_standard(cls, std_table, _extend_on=None):
        ch_table = cls(std_table.name, std_table.metadata)
        ch_table.schema = std_table.schema
        ch_table.fullname = std_table.fullname
        ch_table.implicit_returning = std_table.implicit_returning
        ch_table.comment = std_table.comment
        ch_table.info = std_table.info
        ch_table._prefixes = std_table._prefixes
        ch_table.dialect_options = std_table.dialect_options

        if _extend_on is None:
            ch_table._columns = std_table._columns

        return ch_table


class MaterializedView(DialectKWArgs, SchemaItem, Immutable, FromClause):
    __visit_name__ = 'materialized_view'

    def __init__(self, *args, **kwargs):
        pass

    @property
    def bind(self):
        return self.metadata.bind

    @property
    def metadata(self):
        return self.inner_table.metadata

    def __new__(cls, inner_model, selectable, if_not_exists=False,
                cluster=None, populate=False, use_to=None,
                mv_suffix='_mv', name=None):
        rv = object.__new__(cls)
        rv.__init__()

        rv.mv_selectable = selectable
        rv.inner_table = inner_model.__table__
        rv.if_not_exists = if_not_exists
        rv.cluster = cluster
        rv.populate = populate
        rv.to = use_to

        table = inner_model.__table__
        metadata = rv.inner_table.metadata

        if use_to:
            if name is None:
                name = table.name + mv_suffix
        else:
            name = table.name

        rv.name = name

        metadata.info.setdefault('mat_views', set()).add(name)
        if not hasattr(metadata, 'mat_views'):
            metadata.mat_views = {}
        metadata.mat_views[name] = rv

        table.info['mv_storage'] = True

        return rv

    def __repr__(self):
        args = [repr(self.name), repr(self.metadata)]

        if self.to:
            args += ['TO ' + repr(self.inner_table.name)]
        else:
            args += (
                [repr(x) for x in self.inner_table.columns]
                + [repr(self.inner_table.engine)]
                + ['%s=%s' % (k, repr(getattr(self, k))) for k in ['schema']]
            )

        args += ['AS ' + str(self.mv_selectable)]

        return 'MaterializedView(%s)' % ', '.join(args)

    def create(self, bind=None, checkfirst=False, if_not_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaGenerator, self, checkfirst=checkfirst,
                          if_not_exists=if_not_exists)

    def drop(self, bind=None, checkfirst=False, if_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper, self,
                          checkfirst=checkfirst, if_exists=if_exists)
