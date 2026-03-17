import sqlalchemy as sa

from .operation import Operation
from .utils import parent_class, versioned_column_properties


def first_level(paths):
    for path in paths:
        yield path.split('.')[0]


def subpaths(paths, name):
    for path in paths:
        parts = path.split('.')
        if len(parts) > 1 and parts[0] == name:
            yield '.'.join(parts[1:])


class ReverterException(Exception):
    pass


class Reverter:
    def __init__(self, obj, visited_objects=None, relations=None):
        if relations is None:
            relations = []
        self.visited_objects = visited_objects or []
        self.obj = obj
        self.version_parent = self.obj.version_parent
        self.parent_class = parent_class(self.obj.__class__)
        self.parent_mapper = sa.inspect(self.parent_class)
        self.session = sa.orm.object_session(self.obj)

        self.relations = list(relations)
        for path in relations:
            subpath = path.split('.')[0]
            if subpath not in self.parent_mapper.relationships:
                raise ReverterException(
                    f"Could not initialize Reverter. Class '{parent_class(self.obj.__class__).__name__}' does not have "
                    f"relationship '{subpath}'."
                )

    def revert_properties(self):
        for prop in versioned_column_properties(self.parent_class):
            setattr(self.version_parent, prop.key, getattr(self.obj, prop.key))

    def revert_association(self, prop):
        if prop.uselist:
            setattr(self.version_parent, prop.key, [])
            for child_obj in getattr(self.obj, prop.key):
                value = self.revert_child(child_obj, prop)
                if value:
                    getattr(self.version_parent, prop.key).append(value)
        else:
            setattr(self.version_parent, prop.key, None)
            value = getattr(self.obj, prop.key)
            value = self.revert_child(value, prop)
            if value:
                setattr(self.version_parent, prop.key, value)

    def revert_relationship(self, prop):
        if prop.secondary is not None:
            self.revert_association(prop)
        else:
            if prop.uselist:
                values = []
                for child_obj in getattr(self.obj, prop.key):
                    value = self.revert_child(child_obj, prop)
                    if value:
                        values.append(value)

                for value in getattr(self.version_parent, prop.key, []):
                    if value not in values:
                        self.session.delete(value)
            else:
                self.revert_child(getattr(self.obj, prop.key), prop)

    def revert_child(self, child, prop):
        return self.__class__(
            child,
            visited_objects=self.visited_objects,
            relations=subpaths(self.relations, prop.key),
        )()

    def revert_relationships(self):
        for prop in self.parent_mapper.iterate_properties:
            if isinstance(prop, sa.orm.RelationshipProperty):
                if prop.key in ['versions', 'transaction']:
                    continue

                if prop.key not in first_level(self.relations):
                    continue

                self.revert_relationship(prop)

    def __call__(self):
        if self.obj in self.visited_objects:
            return (
                None
                if self.obj.operation_type == Operation.DELETE
                else self.version_parent
            )

        if self.obj.operation_type == Operation.DELETE:
            self.session.delete(self.version_parent)
            return

        self.visited_objects.append(self.obj)

        # Check if parent object has been deleted
        if self.version_parent is None:
            self.version_parent = parent_class(self.obj.__class__)()

        # Before reifying relations we need to reify object properties. This
        # is needed because reifying relations might need to flush the session
        # which leads to errors when sqlalchemy tries to insert null values
        # into parent object (if parent object has not null constraints).
        self.revert_properties()
        self.revert_relationships()
        self.session.add(self.version_parent)

        return self.version_parent
