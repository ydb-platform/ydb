from office365.runtime.client_value import ClientValue


class ShareLinkRequest(ClientValue):
    """Represents a request for the retrieval or creation of a tokenized sharing link."""

    def __init__(
        self,
        link_kind=None,
        expiration=None,
        people_picker_input=None,
        settings=None,
        create_link=True,
    ):
        """
        :param int or None link_kind: The kind of the tokenized sharing link to be created/updated or retrieved.
        :param datetime or None expiration: A date/time string for which the format conforms to the ISO 8601:2004(E)
            complete representation for calendar date and time of day and which represents the time and date of expiry
            for the tokenized sharing link. Both the minutes and hour value MUST be specified for the difference
            between the local and UTC time. Midnight is represented as 00:00:00. A null value indicates no expiry.
            This value is only applicable to tokenized sharing links that are anonymous access links.
        :param str people_picker_input: A string of JSON serialized data representing users in people picker format.
            This value specifies a list of identities that will be pre-granted access through the tokenized sharing
            link and optionally sent an e-mail notification.
            If this value is null or empty, no identities will be will be pre-granted access through the tokenized
            sharing link and no notification email will be sent.
        :param office365.sharepoint.sharing.share_link_settings.ShareLinkSettings or None settings: The settings for
            the tokenized sharing link to be created/updated.
        :param bool create_link: Indicates whether the operation attempts to create the tokenized sharing link based
            on the requested settings if it does not currently exist.
            If set to true, the operation will attempt to retrieve an existing tokenized sharing link that matches
            the requested settings and failing that will attempt to create a new tokenized sharing link based on the
            requested settings. If false, the operation will attempt to retrieve an existing tokenized sharing link
            that matches the requested settings and failing that will terminate the operation.
        """
        self.linkKind = link_kind
        self.expiration = expiration
        self.peoplePickerInput = people_picker_input
        self.settings = settings
        self.createLink = create_link
        super(ShareLinkRequest, self).__init__()

    @property
    def entity_type_name(self):
        return "SP.Sharing.ShareLinkRequest"
