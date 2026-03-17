from office365.runtime.client_value import ClientValue


class SmtpServer(ClientValue):
    def __init__(self, value=None, is_readonly=None):
        """
        :param str value:
        :param bool is_readonly:
        """
        self.Value = value
        self.IsReadOnly = is_readonly
