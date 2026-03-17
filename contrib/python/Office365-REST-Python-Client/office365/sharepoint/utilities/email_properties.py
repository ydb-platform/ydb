from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class EmailProperties(ClientValue):
    def __init__(
        self,
        body,
        subject,
        to,
        from_address=None,
        cc=None,
        bcc=None,
        additional_headers=None,
    ):
        """
        Specifies the definition of the email to send which includes both the message fields and body

        :param str body: Specifies the message body to send.
        :param str subject: Specifies the Subject field of the e-mail.
        :param list[str] to: Specifies the To field of the email.
        :param str or None from_address: Specifies the From field of the email.
        :param list[str] or None cc: Specifies the carbon copy (cc) recipients of the email.
        :param list[str] or None bcc: Specifies the blind carbon copy (bcc) recipients of the email
        :param dict or None additional_headers:
        """
        super(EmailProperties, self).__init__()
        self.Body = body
        self.Subject = subject
        self.From = from_address
        self.To = ClientValueCollection(str, to)
        self.CC = ClientValueCollection(str, cc)
        self.BCC = ClientValueCollection(str, bcc)
        self.AdditionalHeaders = additional_headers

    @property
    def entity_type_name(self):
        return "SP.Utilities.EmailProperties"
