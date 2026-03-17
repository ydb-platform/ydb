"""
TransactionMetaPlugin offers a way of saving key-value data for transations.
You can use the plugin in same way as other plugins::


    meta_plugin = TransactionMetaPlugin()

    versioning_manager.plugins.append(meta_plugin)


TransactionMetaPlugin creates a simple model called TransactionMeta. This class
has three columns: transaction_id, key and value. TransactionMeta plugin also
creates an association proxy between TransactionMeta and Transaction classes
for easy dictionary based access of key-value pairs.

You can easily 'tag' transactions with certain key value pairs by giving these
keys and values to the meta property of Transaction class.


::


    from sqlalchemy_continuum import versioning_manager


    article = Article()
    session.add(article)

    uow = versioning_manager.unit_of_work(session)
    tx = uow.create_transaction(session)
    tx.meta = {u'some_key': u'some value'}
    session.commit()

    TransactionMeta = meta_plugin.model_class
    Transaction = versioning_manager.transaction_cls

    # find all transactions with 'article' tags
    query = (
        session.query(Transaction)
        .join(Transaction.meta_relation)
        .filter(
            db.and_(
                TransactionMeta.key == 'some_key',
                TransactionMeta.value == 'some value'
            )
        )
    )
"""

import sqlalchemy as sa
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection

from ..factory import ModelFactory
from .base import Plugin


class TransactionMetaBase:
    transaction_id = sa.Column(sa.BigInteger, primary_key=True)
    key = sa.Column(sa.Unicode(255), primary_key=True)
    value = sa.Column(sa.UnicodeText)


class TransactionMetaFactory(ModelFactory):
    model_name = 'TransactionMeta'

    def create_class(self, manager):
        """
        Create TransactionMeta class.
        """

        class TransactionMeta(manager.declarative_base, TransactionMetaBase):
            __tablename__ = 'transaction_meta'

        TransactionMeta.transaction = sa.orm.relationship(
            manager.transaction_cls,
            backref=sa.orm.backref(
                'meta_relation', collection_class=attribute_mapped_collection('key')
            ),
            primaryjoin=(
                f'{manager.transaction_cls.__name__}.id == TransactionMeta.transaction_id'
            ),
            foreign_keys=[TransactionMeta.transaction_id],
        )

        manager.transaction_cls.meta = association_proxy(
            'meta_relation',
            'value',
            creator=lambda key, value: TransactionMeta(key=key, value=value),
        )

        return TransactionMeta


class TransactionMetaPlugin(Plugin):
    def after_build_tx_class(self, manager):
        self.model_class = TransactionMetaFactory()(manager)
        manager.transaction_meta_cls = self.model_class

    def after_build_models(self, manager):
        self.model_class = TransactionMetaFactory()(manager)
        manager.transaction_meta_cls = self.model_class
