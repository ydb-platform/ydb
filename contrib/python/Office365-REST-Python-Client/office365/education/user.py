from office365.entity import Entity


class EducationUser(Entity):
    """A user in the system. This is an education-specific variant of the user with the same id that
    Microsoft Graph will return from the non-education-specific /users endpoint. This object provides a targeted
    subset of properties from the core user object and adds a set of education-specific properties such as primaryRole,
    student, and teacher data."""

    pass
