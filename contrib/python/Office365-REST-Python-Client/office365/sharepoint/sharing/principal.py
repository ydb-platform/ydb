from office365.runtime.client_value import ClientValue


class Principal(ClientValue):
    """Principal class is a representation of an identity (user/group)."""

    def __init__(
        self,
        id_=None,
        directory_object_id=None,
        email=None,
        expiration=None,
        is_active=None,
        is_external=None,
        job_title=None,
        login_name=None,
        name=None,
    ):
        """
        :param int id_: Id of the Principal in SharePoint's UserInfo List.
        :param str directory_object_id:
        :param str email: Email address of the Principal.
        :param str expiration:
        :param bool is_active: Boolean value representing if the Principal is Active.
        :param bool is_external: Boolean value representing if the Principal is an external user.
        :param str job_title: The Job Title of the Principal.
        :param str login_name: LoginName of the Principal.
        :param str name: Name of the Principal.
        """
        self.id = id_
        self.directoryObjectId = directory_object_id
        self.email = email
        self.expiration = expiration
        self.isActive = is_active
        self.isExternal = is_external
        self.jobTitle = job_title
        self.loginName = login_name
        self.name = name

    @property
    def entity_type_name(self):
        return "SP.Sharing.Principal"
