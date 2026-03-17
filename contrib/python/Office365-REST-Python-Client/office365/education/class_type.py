from office365.entity import Entity


class EducationClass(Entity):
    """
    Represents a class within a school. The educationClass resource corresponds to the Microsoft 365 group
    and shares the same ID. Students are regular members of the class, and teachers are owners and have appropriate
    rights. For Office experiences to work correctly, teachers must be members of both the teachers
    and members collections.
    """
