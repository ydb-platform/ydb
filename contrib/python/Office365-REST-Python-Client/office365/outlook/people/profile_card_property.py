from office365.entity import Entity


class ProfileCardProperty(Entity):
    """
    Represents an attribute of a user on the Microsoft 365 profile card for an organization to surface in a shared,
    people experience.

    The attribute can be an Microsoft Entra ID built-in attribute, such as Alias or UserPrincipalName,
    or it can be a custom attribute. For a custom attribute, an administrator can define an en-us default display name
    String and a set of alternative translations for the languages supported in their organization.

    For more information about how to add properties to the profile card for an organization, see Add or remove
    custom attributes on a profile card using the profile card API.
    """
