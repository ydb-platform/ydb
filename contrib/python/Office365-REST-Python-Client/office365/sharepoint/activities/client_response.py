from office365.runtime.client_value import ClientValue


class ActivityClientResponse(ClientValue):
    """"""

    def __init__(self, id_, message=None, server_id=None, status=None):
        # type: (str, str, str, int) -> None
        """ """
        self.id = id_
        self.message = message
        self.serverId = server_id
        self.status = status

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.ActivityClientResponse"
