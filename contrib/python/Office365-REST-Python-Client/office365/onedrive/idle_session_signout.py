from office365.runtime.client_value import ClientValue


class IdleSessionSignOut(ClientValue):
    """Represents the idle session sign-out policy settings for SharePoint."""

    def __init__(
        self,
        is_enabled=None,
        sign_out_after_in_seconds=None,
        warn_after_in_seconds=None,
    ):
        self.isEnabled = is_enabled
        self.signOutAfterInSeconds = sign_out_after_in_seconds
        self.warnAfterInSeconds = warn_after_in_seconds
