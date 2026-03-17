from office365.entity import Entity


class UserConsentRequest(Entity):
    """
    Represents the details of the consent request a user creates when they request to access an app or to grant
    permissions to an app. The details include justification for requesting access, the status of the request,
    and the approval details.

    The user can create a consent request when an app or a permission requires admin authorization and only when the
    admin consent workflow is enabled.
    """
