from office365.entity import Entity


class AppScope(Entity):
    """
    The scope of a role assignment determines the set of resources for which the principal has been granted access.
    An app scope is a scope defined and understood by a specific application, unlike directory scopes that are
    shared scopes stored in the directory and understood by multiple applications.

    This may be in both the following principal and scope scenarios:

    A single principal and a single scope
    Multiple principals and multiple scopes.
    """
