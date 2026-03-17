from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class AppliedConditionalAccessPolicy(ClientValue):
    """Indicates the attributes related to applied conditional access policy or policies that are triggered
    by a sign-in activity.

    The data in this object is returned only for callers with privileges to read conditional access data.
    For more information, see Permissions for viewing applied conditional access (CA) policies in sign-ins.
    """

    def __init__(
        self,
        display_name=None,
        enforced_grant_controls=None,
        enforced_session_controls=None,
        id_=None,
        result=None,
    ):
        """
        :param str display_name: Refers to the name of the conditional access policy
        :param list[str] enforced_grant_controls: Refers to the grant controls enforced by the conditional access policy
        :param list[str] enforced_session_controls: Refers to the session controls enforced by the conditional
            access policy
        """
        self.displayName = display_name
        self.enforcedGrantControls = StringCollection(enforced_grant_controls)
        self.enforcedSessionControls = StringCollection(enforced_session_controls)
        self.id = id_
        self.result = result
