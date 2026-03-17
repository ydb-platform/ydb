from typing import Dict, List, Optional

import sqlalchemy as sa

from .reverter import Reverter
from .utils import get_versioning_manager, is_internal_column, parent_class


class VersionClassBase:
    # Cache attributes for pre-loaded previous/next versions
    _previous_cache: Optional['VersionClassBase'] = None
    _next_cache: Optional['VersionClassBase'] = None
    _cache_populated: bool = False

    @property
    def previous(self):
        """
        Returns the previous version relative to this version in the version
        history. If current version is the first version this method returns
        None.

        If versions have been pre-fetched using `all_versions()` with
        `link_versions()`, this will use the cached value instead of
        making a database query.
        """
        # Check if cache was explicitly populated (even if previous is None)
        if hasattr(self, '_previous_cache') and self._cache_populated:
            return self._previous_cache
        return (
            get_versioning_manager(self)
            .fetcher(parent_class(self.__class__))
            .previous(self)
        )

    @property
    def next(self):
        """
        Returns the next version relative to this version in the version
        history. If current version is the last version this method returns
        None.

        If versions have been pre-fetched using `all_versions()` with
        `link_versions()`, this will use the cached value instead of
        making a database query.
        """
        # Check if cache was explicitly populated (even if next is None)
        if hasattr(self, '_next_cache') and self._cache_populated:
            return self._next_cache
        return (
            get_versioning_manager(self)
            .fetcher(parent_class(self.__class__))
            .next(self)
        )

    @property
    def index(self):
        """
        Return the index of this version in the version history.
        """
        return (
            get_versioning_manager(self)
            .fetcher(parent_class(self.__class__))
            .index(self)
        )

    @classmethod
    def version_at(
        cls, session, primary_key_values: Dict, transaction_id: int
    ) -> Optional['VersionClassBase']:
        """
        Efficiently retrieve the version that was active at a specific transaction.

        This is more efficient than iterating through versions manually, especially
        when using the validity strategy which can use range conditions.

        Example::

            # Get the version of Article #5 that was active at transaction #100
            version = ArticleVersion.version_at(
                session,
                {'id': 5},
                transaction_id=100
            )

        :param session: SQLAlchemy session
        :param primary_key_values: Dict mapping primary key column names to values
        :param transaction_id: The transaction ID to query at
        :returns: The version object active at that transaction, or None
        """
        manager = get_versioning_manager(cls)
        parent_cls = parent_class(cls)
        fetcher = manager.fetcher(parent_cls)
        return fetcher.version_at(session, cls, primary_key_values, transaction_id)

    @classmethod
    def all_versions(
        cls,
        session,
        primary_key_values: Dict,
        limit: Optional[int] = None,
        offset: int = 0,
        desc: bool = True,
        link: bool = True,
    ) -> List['VersionClassBase']:
        """
        Efficiently fetch all versions for an entity in a single query.

        This avoids N+1 queries when iterating through version history
        by fetching all versions at once. When `link=True`, the returned
        versions will have their `.previous` and `.next` properties
        pre-populated from the cache.

        Example::

            # Get all versions of Article #5, newest first
            versions = ArticleVersion.all_versions(
                session,
                {'id': 5},
                limit=10  # Only get the 10 most recent versions
            )

            # Iterate without N+1 queries
            for version in versions:
                print(version.changeset)
                print(version.previous)  # Uses cached value, no query

        :param session: SQLAlchemy session
        :param primary_key_values: Dict mapping primary key column names to values
        :param limit: Maximum number of versions to return (None for all)
        :param offset: Number of versions to skip
        :param desc: If True, return newest versions first (default)
        :param link: If True, pre-populate previous/next caches (default)
        :returns: List of version objects
        """
        manager = get_versioning_manager(cls)
        parent_cls = parent_class(cls)
        fetcher = manager.fetcher(parent_cls)

        versions = fetcher.all_versions(
            session, cls, primary_key_values, limit=limit, offset=offset, desc=desc
        )

        if link and versions:
            fetcher.link_versions(versions, desc=desc)

        return versions

    @property
    def changeset(self):
        """
        Return a dictionary of changed fields in this version with keys as
        field names and values as lists with first value as the old field value
        and second list value as the new value.
        """
        previous_version = self.previous
        data = {}

        for key in sa.inspect(self.__class__).columns.keys():
            if is_internal_column(self, key):
                continue
            if not previous_version:
                old = None
            else:
                old = getattr(previous_version, key)
            new = getattr(self, key)
            if old != new:
                data[key] = [old, new]

        manager = get_versioning_manager(self)
        manager.plugins.after_construct_changeset(self, data)
        return data

    def revert(self, relations=None):
        if relations is None:
            relations = []
        return Reverter(self, relations=relations)()
