from office365.entity import Entity


class Person(Entity):
    """Represents an aggregation of information about a person from across mail and contacts.
    People can be local contacts or your organization's directory, and people from recent communications
    (such as email)."""
