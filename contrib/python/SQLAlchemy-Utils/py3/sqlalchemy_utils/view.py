import sqlalchemy as sa
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement, PrimaryKeyConstraint
from sqlalchemy.sql.expression import ClauseElement, Executable

from sqlalchemy_utils.functions import get_columns


class CreateView(DDLElement):
    def __init__(self, name, selectable, materialized=False, replace=False):
        if materialized and replace:
            raise ValueError("Cannot use CREATE OR REPLACE with materialized views")
        self.name = name
        self.selectable = selectable
        self.materialized = materialized
        self.replace = replace


@compiler.compiles(CreateView)
def compile_create_materialized_view(element, compiler, **kw):
    return 'CREATE {}{}VIEW {} AS {}'.format(
        'OR REPLACE ' if element.replace else '',
        'MATERIALIZED ' if element.materialized else '',
        compiler.dialect.identifier_preparer.quote(element.name),
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


class DropView(DDLElement):
    def __init__(self, name, materialized=False, cascade=True):
        self.name = name
        self.materialized = materialized
        self.cascade = cascade


@compiler.compiles(DropView)
def compile_drop_materialized_view(element, compiler, **kw):
    return 'DROP {}VIEW IF EXISTS {} {}'.format(
        'MATERIALIZED ' if element.materialized else '',
        compiler.dialect.identifier_preparer.quote(element.name),
        'CASCADE' if element.cascade else ''
    )


def create_table_from_selectable(
    name,
    selectable,
    indexes=None,
    metadata=None,
    aliases=None,
    **kwargs
):
    if indexes is None:
        indexes = []
    if metadata is None:
        metadata = sa.MetaData()
    if aliases is None:
        aliases = {}
    args = [
        sa.Column(
            c.name,
            c.type,
            key=aliases.get(c.name, c.name),
            primary_key=c.primary_key
        )
        for c in get_columns(selectable)
    ] + indexes
    table = sa.Table(name, metadata, *args, **kwargs)

    if not any([c.primary_key for c in get_columns(selectable)]):
        table.append_constraint(
            PrimaryKeyConstraint(*[c.name for c in get_columns(selectable)])
        )
    return table


def create_materialized_view(
    name,
    selectable,
    metadata,
    indexes=None,
    aliases=None
):
    """ Create a view on a given metadata

    :param name: The name of the view to create.
    :param selectable: An SQLAlchemy selectable e.g. a select() statement.
    :param metadata:
        An SQLAlchemy Metadata instance that stores the features of the
        database being described.
    :param indexes: An optional list of SQLAlchemy Index instances.
    :param aliases:
        An optional dictionary containing with keys as column names and values
        as column aliases.

    Same as for ``create_view`` except that a ``CREATE MATERIALIZED VIEW``
    statement is emitted instead of a ``CREATE VIEW``.

    """
    table = create_table_from_selectable(
        name=name,
        selectable=selectable,
        indexes=indexes,
        metadata=None,
        aliases=aliases
    )

    sa.event.listen(
        metadata,
        'after_create',
        CreateView(name, selectable, materialized=True)
    )

    @sa.event.listens_for(metadata, 'after_create')
    def create_indexes(target, connection, **kw):
        for idx in table.indexes:
            idx.create(connection)

    sa.event.listen(
        metadata,
        'before_drop',
        DropView(name, materialized=True)
    )
    return table


def create_view(
    name,
    selectable,
    metadata,
    cascade_on_drop=True,
    replace=False,
):
    """ Create a view on a given metadata

    :param name: The name of the view to create.
    :param selectable: An SQLAlchemy selectable e.g. a select() statement.
    :param metadata:
        An SQLAlchemy Metadata instance that stores the features of the
        database being described.
    :param cascade_on_drop: If ``True`` the view will be dropped with
        ``CASCADE``, deleting all dependent objects as well.
    :param replace: If ``True`` the view will be created with ``OR REPLACE``,
        replacing an existing view with the same name.

    The process for creating a view is similar to the standard way that a
    table is constructed, except that a selectable is provided instead of
    a set of columns. The view is created once a ``CREATE`` statement is
    executed against the supplied metadata (e.g. ``metadata.create_all(..)``),
    and dropped when a ``DROP`` is executed against the metadata.

    To create a view that performs basic filtering on a table. ::

        metadata = MetaData()
        users = Table('users', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String),
                Column('fullname', String),
                Column('premium_user', Boolean, default=False),
            )

        premium_members = select(users).where(users.c.premium_user == True)
        # sqlalchemy 1.3:
        # premium_members = select([users]).where(users.c.premium_user == True)
        create_view('premium_users', premium_members, metadata)

        metadata.create_all(engine) # View is created at this point

    """
    table = create_table_from_selectable(
        name=name,
        selectable=selectable,
        metadata=None
    )

    sa.event.listen(
        metadata,
        'after_create',
        CreateView(name, selectable, replace=replace),
    )

    @sa.event.listens_for(metadata, 'after_create')
    def create_indexes(target, connection, **kw):
        for idx in table.indexes:
            idx.create(connection)

    sa.event.listen(
        metadata,
        'before_drop',
        DropView(name, cascade=cascade_on_drop)
    )
    return table


class RefreshMaterializedView(Executable, ClauseElement):
    inherit_cache = True

    def __init__(self, name, concurrently):
        self.name = name
        self.concurrently = concurrently


@compiler.compiles(RefreshMaterializedView)
def compile_refresh_materialized_view(element, compiler):
    return 'REFRESH MATERIALIZED VIEW {concurrently}{name}'.format(
        concurrently='CONCURRENTLY ' if element.concurrently else '',
        name=compiler.dialect.identifier_preparer.quote(element.name),
    )


def refresh_materialized_view(session, name, concurrently=False):
    """ Refreshes an already existing materialized view

    :param session: An SQLAlchemy Session instance.
    :param name: The name of the materialized view to refresh.
    :param concurrently:
        Optional flag that causes the ``CONCURRENTLY`` parameter
        to be specified when the materialized view is refreshed.
    """
    # Since session.execute() bypasses autoflush, we must manually flush in
    # order to include newly-created/modified objects in the refresh.
    session.flush()
    session.execute(RefreshMaterializedView(name, concurrently))
