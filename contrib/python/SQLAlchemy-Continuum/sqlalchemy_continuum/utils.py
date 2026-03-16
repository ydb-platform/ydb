from collections import defaultdict
from inspect import isclass
from itertools import chain

import sqlalchemy as sa
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.orm.util import AliasedClass
from ._compat import (
    get_primary_keys,
    identity,
    naturally_equivalent,
)

from .exc import ClassNotVersioned


def get_versioning_manager(obj_or_class):
    """
    Return the associated SQLAlchemy-Continuum VersioningManager for given
    SQLAlchemy declarative model class or object.

    :param obj_or_class: SQLAlchemy declarative model object or class
    """
    if isinstance(obj_or_class, AliasedClass):
        obj_or_class = sa.inspect(obj_or_class).mapper.class_
    cls = obj_or_class if isclass(obj_or_class) else obj_or_class.__class__
    try:
        return cls.__versioning_manager__
    except AttributeError:
        raise ClassNotVersioned(cls.__name__)


def option(obj_or_class, option_name):
    """
    Return the option value of given option for given versioned object or
    class.

    :param obj_or_class: SQLAlchemy declarative model object or class
    :param option_name: The name of an option to return
    """
    if isinstance(obj_or_class, AliasedClass):
        obj_or_class = sa.inspect(obj_or_class).mapper.class_
    cls = obj_or_class if isclass(obj_or_class) else obj_or_class.__class__
    if not hasattr(cls, '__versioned__'):
        cls = parent_class(cls)
    return get_versioning_manager(cls).option(cls, option_name)


def tx_column_name(obj):
    return option(obj, 'transaction_column_name')


def end_tx_column_name(obj):
    return option(obj, 'end_transaction_column_name')


def end_tx_attr(obj):
    return getattr(obj.__class__, end_tx_column_name(obj))


def parent_class(version_cls):
    """
    Return the parent class for given version model class.

    ::

        parent_class(ArticleVersion)  # Article class


    :param model: SQLAlchemy declarative version model class

    .. seealso:: :func:`version_class`
    """
    return get_versioning_manager(version_cls).parent_class_map[version_cls]


def transaction_class(cls):
    """
    Return the associated transaction class for given versioned SQLAlchemy
    declarative class or version class.

    ::

        from sqlalchemy_continuum import transaction_class


        transaction_class(Article)  # Transaction class

    :param cls: SQLAlchemy versioned declarative class or version model class
    """
    return get_versioning_manager(cls).transaction_cls


def version_obj(session, parent_obj):
    manager = get_versioning_manager(parent_obj)
    uow = manager.unit_of_work(session)
    for version_obj in uow.version_session:
        if parent_class(version_obj.__class__) == parent_obj.__class__ and identity(
            version_obj
        )[:-1] == identity(parent_obj):
            return version_obj


def version_class(model):
    """
    Return the version class for given SQLAlchemy declarative model class.

    ::

        version_class(Article)  # ArticleVersion class


    :param model: SQLAlchemy declarative model class

    .. seealso:: :func:`parent_class`
    """
    manager = get_versioning_manager(model)
    try:
        return manager.version_class_map[model]
    except KeyError:
        return model


def version_table(table):
    """
    Return associated version table for given SQLAlchemy Table object.

    :param table: SQLAlchemy Table object
    """
    if table.schema:
        return table.metadata.tables[table.schema + '.' + table.name + '_version']
    elif table.metadata.schema:
        return table.metadata.tables[
            table.metadata.schema + '.' + table.name + '_version'
        ]
    else:
        return table.metadata.tables[table.name + '_version']


def versioned_objects(session):
    """
    Return all versioned objects in given session.

    :param session: SQLAlchemy session object

    .. seealso:: :func:`is_versioned`
    """
    for obj in session:
        if is_versioned(obj):
            yield obj


def is_versioned(obj_or_class):
    """
    Return whether or not given object is versioned.

    ::

        is_versioned(Article)  # True

        article = Article()

        is_versioned(article)  # True


    :param obj_or_class:
        SQLAlchemy declarative model object or SQLAlchemy declarative model
        class.

    .. seealso:: :func:`versioned_objects`
    """
    try:
        return hasattr(obj_or_class, '__versioned__') and get_versioning_manager(
            obj_or_class
        ).option(obj_or_class, 'versioning')
    except ClassNotVersioned:
        return False


def versioned_column_properties(obj_or_class):
    """
    Return all versioned column properties for given versioned SQLAlchemy
    declarative model object.

    :param obj: SQLAlchemy declarative model object
    """
    manager = get_versioning_manager(obj_or_class)

    cls = obj_or_class if isclass(obj_or_class) else obj_or_class.__class__

    mapper = sa.inspect(cls)
    for key, column in mapper.columns.items():
        # Ignores non table columns
        if not is_table_column(column):
            continue

        if not manager.is_excluded_property(obj_or_class, key):
            yield getattr(mapper.attrs, key)


def versioned_relationships(obj, versioned_column_keys):
    """
    Return all versioned relationships for given versioned SQLAlchemy
    declarative model object.

    :param obj: SQLAlchemy declarative model object
    """
    for prop in sa.inspect(obj.__class__).relationships:
        if any(c.key in versioned_column_keys for c in prop.local_columns):
            yield prop


def vacuum(session, model, yield_per=1000):
    """
    When making structural changes to version tables (for example dropping
    columns) there are sometimes situations where some old version records
    become futile.

    Vacuum deletes all futile version rows which had no changes compared to
    previous version.


    ::


        from sqlalchemy_continuum import vacuum


        vacuum(session, User)  # vacuums user version


    :param session: SQLAlchemy session object
    :param model: SQLAlchemy declarative model class
    :param yield_per: how many rows to process at a time
    """
    version_cls = version_class(model)
    versions = defaultdict(list)

    query = (
        session.query(version_cls).order_by(
            option(version_cls, 'transaction_column_name')
        )
    ).yield_per(yield_per)

    primary_key_col = sa.inspection.inspect(model).primary_key[0].name

    for version in query:
        version_id = getattr(version, primary_key_col)
        if versions[version_id]:
            prev_version = versions[version_id][-1]
            if naturally_equivalent(prev_version, version):
                session.delete(version)
        else:
            versions[version_id].append(version)


def is_table_column(column):
    """
    Return wheter of not give field is a column over the database table.

    :param column: SQLAclhemy model field.
    :rtype: bool
    """
    return isinstance(column, sa.Column)


def is_internal_column(model, column_name):
    """
    Return whether or not given column of given SQLAlchemy declarative classs
    is considered an internal column (a column whose purpose is mainly
    for SA-Continuum's internal use).

    :param version_obj: SQLAlchemy declarative class
    :param column_name: Name of the column
    """
    return column_name in (
        option(model, 'transaction_column_name'),
        option(model, 'end_transaction_column_name'),
        option(model, 'operation_type_column_name'),
    )


def is_modified_or_deleted(obj):
    """
    Return whether or not some of the versioned properties of given SQLAlchemy
    declarative object have been modified or if the object has been deleted.

    :param obj: SQLAlchemy declarative model object
    """
    session = sa.orm.object_session(obj)
    return is_versioned(obj) and (
        is_modified(obj) or obj in chain(session.deleted, session.new)
    )


def is_modified(obj):
    """
    Return whether or not the versioned properties of given object have been
    modified.

    ::

        article = Article()

        is_modified(article)  # False

        article.name = 'Something'

        is_modified(article)  # True


    :param obj: SQLAlchemy declarative model object

    .. seealso:: :func:`is_modified_or_deleted`
    .. seealso:: :func:`is_session_modified`
    """
    column_names = sa.inspect(obj.__class__).columns.keys()
    versioned_column_keys = [prop.key for prop in versioned_column_properties(obj)]
    versioned_relationship_keys = [
        prop.key for prop in versioned_relationships(obj, versioned_column_keys)
    ]
    for key, attr in sa.inspect(obj).attrs.items():
        if key in column_names:
            if key not in versioned_column_keys:
                continue
            if attr.history.has_changes():
                return True
        if key in versioned_relationship_keys:
            if attr.history.has_changes():
                return True
    return False


def is_session_modified(session):
    """
    Return whether or not any of the versioned objects in given session have
    been either modified or deleted.

    :param session: SQLAlchemy session object

    .. seealso:: :func:`is_versioned`
    .. seealso:: :func:`versioned_objects`
    """
    return any(is_modified_or_deleted(obj) for obj in versioned_objects(session))


def count_versions(obj):
    """
    Return the number of versions given object has. This function works even
    when obj has `create_models` and `create_tables` versioned settings
    disabled.

    ::

        article = Article(name=u'Some article')

        count_versions(article)  # 0

        session.add(article)
        session.commit()

        count_versions(article)  # 1


    :param obj: SQLAlchemy declarative model object
    """
    session = sa.orm.object_session(obj)
    if session is None:
        # If object is transient, we assume it has no version history.
        return 0
    manager = get_versioning_manager(obj)
    table_name = manager.option(obj, 'table_name') % obj.__table__.name
    criteria = [f'{pk} = {getattr(obj, pk)!r}' for pk in get_primary_keys(obj)]
    query = sa.text(
        'SELECT COUNT(1) FROM {} WHERE {}'.format(table_name, ' AND '.join(criteria))
    )
    return session.execute(query).scalar()


def changeset(obj):
    """
    Return a humanized changeset for given SQLAlchemy declarative object. With
    this function you can easily check the changeset of given object in current
    transaction.

    ::


        from sqlalchemy_continuum import changeset


        article = Article(name=u'Some article')
        changeset(article)
        # {'name': [u'Some article', None]}

    :param obj: SQLAlchemy declarative model object
    """
    data = {}
    session = sa.orm.object_session(obj)
    if session and obj in session.deleted:
        columns = [
            c for c in sa.inspect(obj.__class__).columns.values() if is_table_column(c)
        ]

        for column in columns:
            if not column.primary_key:
                value = getattr(obj, column.key)
                if value is not None:
                    data[column.key] = [None, getattr(obj, column.key)]
    else:
        for prop in obj.__mapper__.iterate_properties:
            history = get_history(obj, prop.key)
            if history.has_changes():
                old_value = history.deleted[0] if history.deleted else None
                new_value = history.added[0] if history.added else None

                if new_value:
                    data[prop.key] = [new_value, old_value]
    return data


class VersioningClauseAdapter(sa.sql.visitors.ReplacingCloningVisitor):
    def replace(self, col):
        if isinstance(col, sa.Column):
            table = version_table(col.table)
            return table.c.get(col.key)


def adapt_columns(expr):
    return VersioningClauseAdapter().traverse(expr)
