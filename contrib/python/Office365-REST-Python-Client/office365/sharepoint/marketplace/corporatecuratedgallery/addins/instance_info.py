from office365.runtime.client_value import ClientValue


class SPAddinInstanceInfo(ClientValue):

    def __init__(
        self,
        app_identifier=None,
        app_instance_id=None,
        tenant_app_data=None,
        tenant_app_data_update_time=None,
        title=None,
    ):
        """
        :param str app_identifier:
        :param str app_instance_id:
        :param str tenant_app_data:
        :param datetime.datetime tenant_app_data_update_time:
        :param str title:
        """
        self.appIdentifier = app_identifier
        self.appInstanceId = app_instance_id
        self.tenantAppData = tenant_app_data
        self.tenantAppDataUpdateTime = tenant_app_data_update_time
        self.title = title

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Marketplace.CorporateCuratedGallery.SPAddinInstanceInfo"
