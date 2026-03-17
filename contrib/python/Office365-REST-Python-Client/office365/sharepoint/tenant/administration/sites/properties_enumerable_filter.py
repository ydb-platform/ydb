from office365.runtime.client_value import ClientValue


class SitePropertiesEnumerableFilter(ClientValue):
    def __init__(
        self,
        _filter,
        start_index=None,
        include_detail=None,
        include_personal_site=None,
        group_id_defined=None,
        template=None,
        archived_by=None,
        archived_time=None,
        archive_status=None,
    ):
        """
        :param str _filter:
        :param str start_index:
        :param bool include_detail:
        :param int include_personal_site:
        :param int group_id_defined:
        :param str template:
        """
        super(SitePropertiesEnumerableFilter, self).__init__()
        self.ArchivedBy = archived_by
        self.Filter = _filter
        self.GroupIdDefined = group_id_defined
        self.IncludeDetail = include_detail
        self.IncludePersonalSite = include_personal_site
        self.StartIndex = start_index
        self.Template = template
        self.ArchivedTime = archived_time
        self.ArchiveStatus = archive_status

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SPOSitePropertiesEnumerableFilter"
