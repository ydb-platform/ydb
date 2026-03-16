import pytest

from umongo.frameworks import InstanceRegisterer
from umongo.builder import BaseBuilder
from umongo.instance import Instance
from umongo.document import DocumentImplementation
from umongo.exceptions import NoCompatibleInstanceError


def create_env(prefix):
    db_cls = type("f{prefix}DB", (), {})
    document_cls = type(f"{prefix}Document", (DocumentImplementation, ), {})
    builder_cls = type(f"{prefix}Builder", (BaseBuilder, ), {
        'BASE_DOCUMENT_CLS': document_cls,
    })
    instance_cls = type(f"{prefix}Instance", (Instance, ), {
        'BUILDER_CLS': builder_cls,
        'is_compatible_with': staticmethod(lambda db: isinstance(db, db_cls))
    })
    return db_cls, document_cls, builder_cls, instance_cls


class TestBuilder:

    def test_basic_builder_registerer(self):
        registerer = InstanceRegisterer()
        AlphaDB, _, _, AlphaInstance = create_env('Alpha')

        with pytest.raises(NoCompatibleInstanceError):
            registerer.find_from_db(AlphaDB())
        registerer.register(AlphaInstance)
        assert registerer.find_from_db(AlphaDB()) is AlphaInstance
        # Multiple registers does nothing
        registerer.register(AlphaInstance)
        registerer.unregister(AlphaInstance)
        with pytest.raises(NoCompatibleInstanceError):
            registerer.find_from_db(AlphaDB())

    def test_multi_builder(self):
        registerer = InstanceRegisterer()
        AlphaDB, _, _, AlphaInstance = create_env('Alpha')
        BetaDB, _, _, BetaInstance = create_env('Beta')

        registerer.register(AlphaInstance)
        assert registerer.find_from_db(AlphaDB()) is AlphaInstance
        with pytest.raises(NoCompatibleInstanceError):
            registerer.find_from_db(BetaDB())
        registerer.register(BetaInstance)
        assert registerer.find_from_db(BetaDB()) is BetaInstance
        assert registerer.find_from_db(AlphaDB()) is AlphaInstance

    def test_overload_builder(self):
        registerer = InstanceRegisterer()
        AlphaDB, _, _, AlphaInstance = create_env('Alpha')

        registerer.register(AlphaInstance)

        # Create a new builder compatible with AlphaDB
        class Alpha2Instance(AlphaInstance):
            pass

        registerer.register(Alpha2Instance)

        # Last registered builder should be tested first
        assert registerer.find_from_db(AlphaDB()) is Alpha2Instance
