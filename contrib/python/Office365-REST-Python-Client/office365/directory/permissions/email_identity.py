from office365.directory.permissions.identity import Identity


class EmailIdentity(Identity):
    """Represents the email identity of a user."""

    def __init__(self, id_=None, email=None, display_name=None):
        """
        :param str email:
        """
        super(EmailIdentity, self).__init__(display_name, id_)
        self.email = email
