"""
TransactionChanges provides way of keeping track efficiently which declarative
models were changed in given transaction. This can be useful when transactions
need to be queried afterwards for problems such as:

1. Find all transactions which affected `User` model.

2. Find all transactions which didn't affect models `Entity` and `Event`.

The plugin works in two ways. On class instrumentation phase this plugin
creates a special transaction model called `TransactionChanges`. This model is
associated with table called `transaction_changes`, which has only only two
fields: transaction_id and entity_name. If for example transaction consisted
of saving 5 new User entities and 1 Article entity, two new rows would be
inserted into transaction_changes table.

================    =================
transaction_id          entity_name
----------------    -----------------
233678                  User
233678                  Article
================    =================
"""

import sqlalchemy as sa

from ..factory import ModelFactory
from .base import Plugin


class TransactionChangesBase:
    transaction_id = sa.Column(sa.BigInteger, primary_key=True)
    entity_name = sa.Column(sa.Unicode(255), primary_key=True)


class TransactionChangesFactory(ModelFactory):
    model_name = 'TransactionChanges'

    def create_class(self, manager):
        """
        Create TransactionChanges class.
        """

        class TransactionChanges(manager.declarative_base, TransactionChangesBase):
            __tablename__ = 'transaction_changes'

        TransactionChanges.transaction = sa.orm.relationship(
            manager.transaction_cls,
            backref=sa.orm.backref(
                'changes',
            ),
            primaryjoin=(
                f'{manager.transaction_cls.__name__}.id == TransactionChanges.transaction_id'
            ),
            foreign_keys=[TransactionChanges.transaction_id],
        )
        return TransactionChanges


class TransactionChangesPlugin(Plugin):
    objects = None

    def after_build_tx_class(self, manager):
        self.model_class = TransactionChangesFactory()(manager)

    def after_build_models(self, manager):
        self.model_class = TransactionChangesFactory()(manager)

    def before_create_version_objects(self, uow, session):
        for entity in uow.operations.entities:
            if not hasattr(entity, '__name__'):
                breakpoint()
            params = uow.current_transaction.id, str(entity.__name__)
            changes = session.get(self.model_class, params)
            if not changes:
                changes = self.model_class(
                    transaction_id=uow.current_transaction.id,
                    entity_name=str(entity.__name__),
                )
                session.add(changes)

    def clear(self):
        self.objects = None

    def after_rollback(self, uow, session):
        self.clear()

    def ater_commit(self, uow, session):
        self.clear()

    def after_version_class_built(self, parent_cls, version_cls):
        parent_cls.__versioned__['transaction_changes'] = self.model_class
