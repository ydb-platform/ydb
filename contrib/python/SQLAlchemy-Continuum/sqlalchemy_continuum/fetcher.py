import operator
from typing import List, Optional, TypeVar

import sqlalchemy as sa
from ._compat import get_primary_keys, identity

from .utils import end_tx_column_name, tx_column_name

T = TypeVar('T')


def parent_identity(obj_or_class):
    return tuple(
        getattr(obj_or_class, column_key)
        for column_key in get_primary_keys(obj_or_class).keys()
        if column_key != tx_column_name(obj_or_class)
    )


def eqmap(callback, iterable):
    for a, b in zip(*map(callback, iterable)):
        yield a == b


def parent_criteria(obj, class_=None):
    if class_ is None:
        class_ = obj.__class__
    return eqmap(parent_identity, (class_, obj))


class VersionObjectFetcher:
    def __init__(self, manager):
        self.manager = manager

    def previous(self, obj):
        """
        Returns the previous version relative to this version in the version
        history. If current version is the first version this method returns
        None.
        """
        return self.previous_query(obj).first()

    def index(self, obj):
        """
        Return the index of this version in the version history.
        """
        session = sa.orm.object_session(obj)
        return session.execute(self._index_query(obj)).fetchone()[0]

    def next(self, obj):
        """
        Returns the next version relative to this version in the version
        history. If current version is the last version this method returns
        None.
        """
        return self.next_query(obj).first()

    def version_at(
        self, session, version_cls, primary_key_values: dict, transaction_id: int
    ):
        """
        Returns the version that was active at the given transaction_id.

        This is an efficient query that finds the version where:
        - transaction_id <= target_transaction_id
        - end_transaction_id > target_transaction_id OR end_transaction_id IS NULL

        For subquery strategy, it finds the version with the highest transaction_id
        that is <= target_transaction_id.

        :param session: SQLAlchemy session
        :param version_cls: The version class to query
        :param primary_key_values: Dict mapping primary key column names to values
        :param transaction_id: The transaction ID to query at
        :returns: The version object active at that transaction, or None
        """
        raise NotImplementedError('Subclasses must implement version_at')

    def all_versions(
        self,
        session,
        version_cls,
        primary_key_values: dict,
        limit: Optional[int] = None,
        offset: int = 0,
        desc: bool = True,
    ) -> List:
        """
        Efficiently fetch all versions for an entity in a single query.

        This avoids N+1 queries when iterating through version history
        by fetching all versions at once, sorted by transaction_id.

        :param session: SQLAlchemy session
        :param version_cls: The version class to query
        :param primary_key_values: Dict mapping primary key column names to values
        :param limit: Maximum number of versions to return (None for all)
        :param offset: Number of versions to skip
        :param desc: If True, return newest versions first
        :returns: List of version objects
        """
        tx_col = tx_column_name(version_cls)

        query = session.query(version_cls)

        # Add primary key filters
        for pk_name, pk_value in primary_key_values.items():
            query = query.filter(getattr(version_cls, pk_name) == pk_value)

        # Order by transaction_id
        if desc:
            query = query.order_by(getattr(version_cls, tx_col).desc())
        else:
            query = query.order_by(getattr(version_cls, tx_col).asc())

        # Apply pagination
        if offset:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)

        return query.all()

    def link_versions(self, versions: List, desc: bool = True) -> List:
        """
        Link a list of versions by setting _previous_cache and _next_cache
        attributes to avoid additional queries when accessing .previous or .next.

        This is useful after calling all_versions() to pre-populate the
        navigation cache.

        :param versions: List of version objects (must be from same entity)
        :param desc: If True, versions are ordered newest-first
        :returns: The same list with _previous_cache and _next_cache populated
        """
        if not versions:
            return versions

        # If descending order: versions[0] is newest, versions[-1] is oldest
        # If ascending order: versions[0] is oldest, versions[-1] is newest
        for i, version in enumerate(versions):
            if desc:
                # In descending order: next is at lower index, previous is at higher index
                version._next_cache = versions[i - 1] if i > 0 else None
                version._previous_cache = (
                    versions[i + 1] if i < len(versions) - 1 else None
                )
            else:
                # In ascending order: previous is at lower index, next is at higher index
                version._previous_cache = versions[i - 1] if i > 0 else None
                version._next_cache = versions[i + 1] if i < len(versions) - 1 else None
            # Mark cache as populated so .previous/.next use cached values
            version._cache_populated = True

        return versions

    def _transaction_id_subquery(self, obj, next_or_prev='next', alias=None):
        if next_or_prev == 'next':
            op = operator.gt
            func = sa.func.min
        else:
            op = operator.lt
            func = sa.func.max

        if alias is None:
            alias = sa.orm.aliased(obj.__class__)
            table = alias.__table__
            if hasattr(alias, 'c'):
                attrs = alias.c
            else:
                attrs = alias
        else:
            table = alias.original
            attrs = alias.c
        query = (
            sa.select(func(getattr(attrs, tx_column_name(obj))))
            .select_from(table)
            .where(
                sa.and_(
                    op(
                        getattr(attrs, tx_column_name(obj)),
                        getattr(obj, tx_column_name(obj)),
                    ),
                    *[
                        getattr(attrs, pk) == getattr(obj, pk)
                        for pk in get_primary_keys(obj.__class__)
                        if pk != tx_column_name(obj)
                    ],
                )
            )
            .correlate(table)
        )
        return query.scalar_subquery()

    def _next_prev_query(self, obj, next_or_prev='next'):
        session = sa.orm.object_session(obj)

        subquery = self._transaction_id_subquery(obj, next_or_prev=next_or_prev)
        subquery = subquery.scalar_subquery()

        return session.query(obj.__class__).filter(
            sa.and_(
                getattr(obj.__class__, tx_column_name(obj)) == subquery,
                *parent_criteria(obj),
            )
        )

    def _index_query(self, obj):
        """
        Returns the query needed for fetching the index of this record relative
        to version history.
        """
        alias = sa.orm.aliased(obj.__class__)

        subquery = (
            sa.select(sa.func.count())
            .select_from(alias.__table__)
            .where(
                getattr(alias, tx_column_name(obj)) < getattr(obj, tx_column_name(obj))
            )
            .correlate(alias.__table__)
            .label('position')
        )
        query = (
            sa.select(subquery)
            .select_from(obj.__table__)
            .where(sa.and_(*eqmap(identity, (obj.__class__, obj))))
            .order_by(getattr(obj.__class__, tx_column_name(obj)))
        )
        return query


class SubqueryFetcher(VersionObjectFetcher):
    def previous_query(self, obj):
        """
        Returns the query that fetches the previous version relative to this
        version in the version history.
        """
        return self._next_prev_query(obj, 'previous')

    def next_query(self, obj):
        """
        Returns the query that fetches the next version relative to this
        version in the version history.
        """
        return self._next_prev_query(obj, 'next')

    def version_at(
        self, session, version_cls, primary_key_values: dict, transaction_id: int
    ):
        """
        Returns the version that was active at the given transaction_id.

        For subquery strategy, finds the version with the highest transaction_id
        that is <= target_transaction_id.

        :param session: SQLAlchemy session
        :param version_cls: The version class to query
        :param primary_key_values: Dict mapping primary key column names to values
        :param transaction_id: The transaction ID to query at
        :returns: The version object active at that transaction, or None
        """
        tx_col = tx_column_name(version_cls)

        # Build primary key conditions
        pk_conditions = [
            getattr(version_cls, pk_name) == pk_value
            for pk_name, pk_value in primary_key_values.items()
        ]

        # Find the version with highest transaction_id <= target
        return (
            session.query(version_cls)
            .filter(
                sa.and_(getattr(version_cls, tx_col) <= transaction_id, *pk_conditions)
            )
            .order_by(getattr(version_cls, tx_col).desc())
            .first()
        )


class ValidityFetcher(VersionObjectFetcher):
    def next_query(self, obj):
        """
        Returns the query that fetches the next version relative to this
        version in the version history.
        """
        session = sa.orm.object_session(obj)

        return session.query(obj.__class__).filter(
            sa.and_(
                getattr(obj.__class__, tx_column_name(obj))
                == getattr(obj, end_tx_column_name(obj)),
                *parent_criteria(obj),
            )
        )

    def previous_query(self, obj):
        """
        Returns the query that fetches the previous version relative to this
        version in the version history.
        """
        session = sa.orm.object_session(obj)

        return session.query(obj.__class__).filter(
            sa.and_(
                getattr(obj.__class__, end_tx_column_name(obj))
                == getattr(obj, tx_column_name(obj)),
                *parent_criteria(obj),
            )
        )

    def version_at(
        self, session, version_cls, primary_key_values: dict, transaction_id: int
    ):
        """
        Returns the version that was active at the given transaction_id.

        For validity strategy, uses the efficient range query:
        - transaction_id <= target_transaction_id
        - end_transaction_id > target_transaction_id OR end_transaction_id IS NULL

        :param session: SQLAlchemy session
        :param version_cls: The version class to query
        :param primary_key_values: Dict mapping primary key column names to values
        :param transaction_id: The transaction ID to query at
        :returns: The version object active at that transaction, or None
        """
        tx_col = tx_column_name(version_cls)
        end_tx_col = end_tx_column_name(version_cls)

        # Build primary key conditions
        pk_conditions = [
            getattr(version_cls, pk_name) == pk_value
            for pk_name, pk_value in primary_key_values.items()
        ]

        # Find version where transaction_id <= target AND
        # (end_transaction_id > target OR end_transaction_id IS NULL)
        return (
            session.query(version_cls)
            .filter(
                sa.and_(
                    getattr(version_cls, tx_col) <= transaction_id,
                    sa.or_(
                        getattr(version_cls, end_tx_col) > transaction_id,
                        getattr(version_cls, end_tx_col).is_(None),
                    ),
                    *pk_conditions,
                )
            )
            .first()
        )
