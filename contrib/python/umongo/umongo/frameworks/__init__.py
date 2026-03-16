"""
Frameworks
==========
"""
from ..exceptions import NoCompatibleInstanceError
from .pymongo import PyMongoInstance


__all__ = (
    'InstanceRegisterer',

    'default_instance_registerer',
    'register_instance',
    'unregister_instance',
    'find_instance_from_db',

    'PyMongoInstance',
    'TxMongoInstance',
    'MotorAsyncIOInstance',
    'MongoMockInstance'
)


class InstanceRegisterer:

    def __init__(self):
        self.instances = []

    def register(self, instance):
        if instance not in self.instances:
            # Insert new item first to overload older compatible instances
            self.instances.insert(0, instance)

    def unregister(self, instance):
        # Basically only used for tests
        self.instances.remove(instance)

    def find_from_db(self, db):
        for instance in self.instances:
            if instance.is_compatible_with(db):
                return instance
        raise NoCompatibleInstanceError(
            'Cannot find a umongo instance compatible with %s' % type(db))


default_instance_registerer = InstanceRegisterer()
register_instance = default_instance_registerer.register
unregister_instance = default_instance_registerer.unregister
find_instance_from_db = default_instance_registerer.find_from_db


# Define instances for each driver

register_instance(PyMongoInstance)


try:
    from .txmongo import TxMongoInstance
    register_instance(TxMongoInstance)
except ImportError:  # pragma: no cover
    pass


try:
    from .motor_asyncio import MotorAsyncIOInstance
    register_instance(MotorAsyncIOInstance)
except ImportError:  # pragma: no cover
    pass


try:
    from .mongomock import MongoMockInstance
    register_instance(MongoMockInstance)
except ImportError:  # pragma: no cover
    pass
