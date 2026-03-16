from office365.entity import Entity


class Artifact(Entity):
    """Represents an abstract entity found online by Microsoft security services."""

    @property
    def entity_type_name(self):
        return "microsoft.graph.security.artifact"
