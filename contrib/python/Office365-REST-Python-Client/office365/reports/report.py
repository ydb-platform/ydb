from office365.runtime.client_value import ClientValue


class Report(ClientValue):
    def __init__(self, content=None):
        """
        Returns the content appropriate for the context

        :param str content: Report content; details vary by report type.
        """
        super(Report, self).__init__()
        self.content = content
