from office365.entity import Entity


class DirectoryDefinition(Entity):
    """
    Provides the synchronization engine information about a directory and its objects. This resource tells the
    synchronization engine, for example, that the directory has objects named user and group, which attributes are
    supported for those objects, and the types for those attributes. In order for the object and attribute to
    participate in synchronization rules and object mappings, they must be defined as part of the directory definition.

    Directory definitions are updated as part of the synchronization schema.
    """
