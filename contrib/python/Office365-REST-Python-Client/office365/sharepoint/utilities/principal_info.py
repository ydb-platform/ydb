from office365.runtime.client_value import ClientValue
from office365.runtime.odata.type import ODataType
from office365.sharepoint.principal.type import PrincipalType


class PrincipalInfo(ClientValue):
    """Provides access to information about a principal."""

    def __init__(
        self,
        principal_id=None,
        display_name=None,
        email=None,
        login_name=None,
        department=None,
        job_title=None,
        principal_type=None,
    ):
        """
        :param str principal_id: Specifies an identifier for the principal. It MUST be -1 if the principal
            does not belong to the current site.
        :param str display_name: Specifies the display name of the principal.
        :param str email: Specifies the e-mail address of the principal.
        :param str department: Specifies the department name of the principal.
        :param str job_title: Specifies the job title of the principal.
        :param str login_name: Specifies the login name of the principal.
        :param int principal_type: Specifies the principal type.
        """
        self.PrincipalId = principal_id
        self.DisplayName = display_name
        self.Email = email
        self.LoginName = login_name
        self.Department = department
        self.JobTitle = job_title
        self.PrincipalType = principal_type

    @property
    def entity_type_name(self):
        return "SP.Utilities.PrincipalInfo"

    @property
    def principal_type_name(self):
        return ODataType.resolve_enum_key(PrincipalType, self.PrincipalType)

    def __str__(self):
        return "{0}: {1}".format(self.principal_type_name, self.DisplayName)
