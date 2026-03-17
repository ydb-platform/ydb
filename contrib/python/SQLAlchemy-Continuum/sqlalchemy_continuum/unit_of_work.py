from copy import copy

import sqlalchemy as sa
from ._compat import get_primary_keys, identity

from .operation import Operations
from .utils import (
    end_tx_column_name,
    is_session_modified,
    tx_column_name,
    version_class,
    versioned_column_properties,
)


class UnitOfWork:
    def __init__(self, manager):
        self.manager = manager
        self.reset()

    def reset(self, session=None):
        """
        Reset the internal state of this UnitOfWork object. Normally this is
        called after transaction has been committed or rolled back.
        """
        self.version_session = None
        self.current_transaction = None
        self.operations = Operations()
        self.pending_statements = []
        self.version_objs = {}

    def is_modified(self, session):
        """
        Return whether or not given session has been modified. Session has been
        modified if any versioned property of any version object in given
        session has been modified or if any of the plugins returns that
        session has been modified.

        :param session: SQLAlchemy session object
        """
        return is_session_modified(session) or any(
            self.manager.plugins.is_session_modified(session)
        )

    def process_before_flush(self, session):
        """
        Before flush processor for given session.

        This method creates a version session which is later on used for the
        creation of version objects. It also creates Transaction object for the
        current transaction and invokes before_flush template method on all
        plugins.

        If the given session had no relevant modifications regarding versioned
        objects this method does nothing.

        :param session: SQLAlchemy session object
        """
        if session == self.version_session:
            return

        if not self.is_modified(session):
            return

        if not self.version_session:
            self.version_session = sa.orm.session.Session(bind=session.connection())

        if not self.current_transaction:
            self.create_transaction(session)

        self.manager.plugins.before_flush(self, session)

    def process_after_flush(self, session):
        """
        After flush processor for given session.

        Creates version objects for all modified versioned parent objects that
        were affected during the flush phase.

        :param session: SQLAlchemy session object
        """
        if session == self.version_session:
            return

        if not self.current_transaction:
            return

        if not self.version_session:
            self.version_session = sa.orm.session.Session(bind=session.connection())

        self.make_versions(session)

    def transaction_args(self, session):
        args = {}
        for plugin in self.manager.plugins:
            args.update(plugin.transaction_args(self, session))
        return args

    def create_transaction(self, session):
        """
        Create transaction object for given SQLAlchemy session.

        :param session: SQLAlchemy session object
        """
        args = self.transaction_args(session)

        Transaction = self.manager.transaction_cls
        self.current_transaction = Transaction()

        for key, value in args.items():
            setattr(self.current_transaction, key, value)
        if not self.version_session:
            self.version_session = sa.orm.session.Session(bind=session.connection())
        self.version_session.add(self.current_transaction)
        self.version_session.flush()
        self.version_session.expunge(self.current_transaction)
        session.add(self.current_transaction)
        return self.current_transaction

    def get_or_create_version_object(self, target):
        """
        Return version object for given parent object. If no version object
        exists for given parent object, create one.

        :param target: Parent object to create the version object for
        """
        version_cls = version_class(target.__class__)
        version_id = identity(target) + (self.current_transaction.id,)
        version_key = (version_cls, version_id)

        if version_key not in self.version_objs:
            version_obj = version_cls()
            self.version_objs[version_key] = version_obj
            self.version_session.add(version_obj)
            tx_column = self.manager.option(target, 'transaction_column_name')
            setattr(version_obj, tx_column, self.current_transaction.id)
            return version_obj
        else:
            return self.version_objs[version_key]

    def process_operation(self, operation):
        """
        Process given operation object. The operation processing has x stages:

        1. Get or create a version object for given parent object
        2. Assign the operation type for this object
        3. Invoke listeners
        4. Update version validity in case validity strategy is used
        5. Mark operation as processed

        :param operation: Operation object
        """
        target = operation.target
        version_obj = self.get_or_create_version_object(target)
        version_obj.operation_type = operation.type
        self.assign_attributes(target, version_obj)

        self.manager.plugins.after_create_version_object(self, target, version_obj)
        if self.manager.option(target, 'strategy') == 'validity':
            self.update_version_validity(target, version_obj)
        operation.processed = True

    def create_version_objects(self, session):
        """
        Create version objects for given session based on operations collected
        by insert, update and deleted trackers.

        :param session: SQLAlchemy session object
        """
        if (
            not self.manager.options['versioning']
            or self.manager.options['native_versioning']
        ):
            return

        for _key, operation in copy(self.operations).items():
            if operation.processed:
                continue

            if not self.current_transaction:
                raise Exception('Current transaction not available.')
            self.process_operation(operation)

        self.version_session.flush()

    def version_validity_subquery(self, parent, version_obj, alias=None):
        """
        Return the subquery needed by :func:`update_version_validity`.

        This method is only used when using 'validity' versioning strategy.

        :param parent: SQLAlchemy declarative parent object
        :parem version_obj: SQLAlchemy declarative version object

        .. seealso:: :func:`update_version_validity`
        """
        fetcher = self.manager.fetcher(parent)
        sa.orm.object_session(version_obj)

        subquery = fetcher._transaction_id_subquery(
            version_obj, next_or_prev='prev', alias=alias
        )
        return subquery

    def update_version_validity(self, parent, version_obj):
        """
        Updates previous version object end_transaction_id based on given
        parent object and newly created version object.

        This method is only used when using 'validity' versioning strategy.

        :param parent: SQLAlchemy declarative parent object
        :parem version_obj: SQLAlchemy declarative version object

        .. seealso:: :func:`version_validity_subquery`
        """
        session = sa.orm.object_session(version_obj)

        for class_ in version_obj.__class__.__mro__:
            if class_ in self.manager.parent_class_map:
                subquery = self.version_validity_subquery(
                    parent, version_obj, alias=sa.orm.aliased(class_.__table__)
                )
                subquery = subquery.scalar_subquery()

                vobj_tx_col = getattr(class_, tx_column_name(version_obj))
                query = (
                    sa.select(class_)
                    .where(
                        vobj_tx_col == subquery,
                        *[
                            getattr(version_obj, pk) == getattr(class_.__table__.c, pk)
                            for pk in get_primary_keys(class_)
                            if pk != tx_column_name(class_)
                        ],
                    )
                    .execution_options(synchronize_session=False)
                )

                old_versions = session.scalars(query).all()
                for old_version in old_versions:
                    setattr(
                        old_version,
                        end_tx_column_name(version_obj),
                        self.current_transaction.id,
                    )

    def create_association_versions(self, session):
        """
        Creates association table version records for given session.

        :param session: SQLAlchemy session object
        """
        statements = copy(self.pending_statements)
        for stmt in statements:
            stmt = stmt.values(
                **{
                    self.manager.options[
                        'transaction_column_name'
                    ]: self.current_transaction.id
                }
            )
            session.execute(stmt)
        self.pending_statements = []

    def make_versions(self, session):
        """
        Create transaction, transaction changes records, version objects.

        :param session: SQLAlchemy session object
        """
        if not self.manager.options['versioning']:
            return

        if self.pending_statements:
            self.create_association_versions(session)

        if self.operations:
            self.manager.plugins.before_create_version_objects(self, session)
            self.create_version_objects(session)
            self.manager.plugins.after_create_version_objects(self, session)

    @property
    def has_changes(self):
        """
        Return whether or not this unit of work has changes.
        """
        return self.operations or self.pending_statements

    def assign_attributes(self, parent_obj, version_obj):
        """
        Assign attributes values from parent object to version object.

        :param parent_obj:
            Parent object to get the attribute values from
        :param version_obj:
            Version object to assign the attribute values to
        """
        for prop in versioned_column_properties(parent_obj):
            try:
                value = getattr(parent_obj, prop.key)
            except sa.orm.exc.ObjectDeletedError:
                value = None
            setattr(version_obj, prop.key, value)
