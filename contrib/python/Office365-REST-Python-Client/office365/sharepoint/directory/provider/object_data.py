from office365.runtime.client_value import ClientValue
from office365.sharepoint.directory.provider.alternate_id_data import AlternateIdData
from office365.sharepoint.directory.provider.session_data import DirectorySessionData


class DirectoryObjectData(ClientValue):
    """"""

    def __init__(
        self,
        alternate_id=AlternateIdData(),
        attribute_expiration_times=None,
        change_marker=None,
        directory_object_sub_type=None,
        directory_object_type=None,
        directory_session_data=DirectorySessionData(),
        id_=None,
        is_new=None,
        last_modified_time=None,
        tenant_context_id=None,
        version=None,
    ):
        self.AlternateId = alternate_id
        self.AttributeExpirationTimes = attribute_expiration_times
        self.ChangeMarker = change_marker
        self.DirectoryObjectSubType = directory_object_sub_type
        self.DirectoryObjectType = directory_object_type
        self.DirectorySessionData = directory_session_data
        self.Id = id_
        self.IsNew = is_new
        self.LastModifiedTime = last_modified_time
        self.TenantContextId = tenant_context_id
        self.Version = version

    @property
    def entity_type_name(self):
        return "SP.Directory.Provider.DirectoryObjectData"
