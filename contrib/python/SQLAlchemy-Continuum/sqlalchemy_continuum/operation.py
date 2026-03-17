from collections import OrderedDict
from copy import copy

import sqlalchemy as sa
from ._compat import identity


class Operation:
    INSERT = 0
    UPDATE = 1
    DELETE = 2

    def __init__(self, target, type):
        self.target = target
        self.type = type
        self.processed = False

    def __eq__(self, other):
        return self.target == other.target and self.type == other.type

    def __ne__(self, other):
        return not (self == other)


class Operations:
    """
    A collection of operations
    """

    def __init__(self):
        self.objects = OrderedDict()

    def format_key(self, target):
        # We cannot use target._sa_instance_state.identity here since object's
        # identity is not yet updated at this phase
        return (target.__class__, identity(target))

    def __contains__(self, target):
        return self.format_key(target) in self.objects

    def __setitem__(self, key, operation):
        self.objects[key] = operation

    def __getitem__(self, key):
        return self.objects[key]

    def __delitem__(self, key):
        del self.objects[key]

    def __bool__(self):
        return bool(self.objects)

    def __nonzero__(self):
        return self.__bool__()

    def __repr__(self):
        return repr(self.objects)

    @property
    def entities(self):
        """
        Return a set of changed versioned entities for given session.

        :param session: SQLAlchemy session object
        """
        return {k[0] for k in self.objects}

    def items(self):
        return self.objects.items()

    def add(self, operation):
        self[self.format_key(operation.target)] = operation

    def add_insert(self, target):
        if target in self:
            # If the object is deleted and then inserted within the same
            # transaction we are actually dealing with an update.
            self.add(Operation(target, Operation.UPDATE))
        else:
            self.add(Operation(target, Operation.INSERT))

    def add_update(self, target):
        state_copy = copy(sa.inspect(target).committed_state)
        relationships = sa.inspect(target.__class__).relationships
        # Remove all ONETOMANY and MANYTOMANY relationships
        for rel_key, relationship in relationships.items():
            if relationship.direction.name in ['ONETOMANY', 'MANYTOMANY']:
                if rel_key in state_copy:
                    del state_copy[rel_key]

        if state_copy:
            self.add(Operation(target, Operation.UPDATE))

    def add_delete(self, target):
        self.add(Operation(target, Operation.DELETE))
