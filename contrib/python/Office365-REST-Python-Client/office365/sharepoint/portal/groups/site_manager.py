from office365.runtime.client_object import ClientObject
from office365.runtime.client_result import ClientResult
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.portal.channels.info_collection import ChannelInfoCollection
from office365.sharepoint.portal.groups.creation_context import GroupCreationContext
from office365.sharepoint.portal.groups.creation_information import (
    GroupCreationInformation,
)
from office365.sharepoint.portal.groups.site_info import GroupSiteInfo
from office365.sharepoint.portal.teams.recent_and_joined_response import (
    RecentAndJoinedTeamsResponse,
)


class GroupSiteManager(ClientObject):
    """Management of Group Sites in SharePoint. Group Sites, also known as Microsoft 365 Groups,
    provide collaboration spaces that integrate with various Microsoft 365 services like Teams, Outlook, and Planner.
    """

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath("GroupSiteManager")
        super(GroupSiteManager, self).__init__(context, resource_path)

    def can_user_create_group(self):
        """Determines if the current user can create group site"""
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "CanUserCreateGroup", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_group_for_site(
        self, display_name, alias, is_public=None, optional_params=None
    ):
        """
        Create a modern site

        :param str display_name:
        :param str alias:
        :param bool or None is_public:
        :param office365.sharepoint.portal.group_creation_params.GroupCreationParams or None optional_params:
        """
        payload = {
            "displayName": display_name,
            "alias": alias,
            "isPublic": is_public,
            "optionalParams": optional_params,
        }
        return_type = ClientResult(self.context, GroupSiteInfo())
        qry = ServiceOperationQuery(
            self, "CreateGroupForSite", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_group_ex(self, display_name, alias, is_public, optional_params=None):
        """
        Creates a modern site

        :param str display_name:
        :param str alias:
        :param bool is_public:
        :param office365.sharepoint.portal.group_creation_params.GroupCreationParams or None optional_params:
        """
        payload = GroupCreationInformation(
            display_name, alias, is_public, optional_params
        )
        return_type = ClientResult(self.context, GroupSiteInfo())
        qry = ServiceOperationQuery(
            self, "CreateGroupEx", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def delete(self, site_url):
        """
        Deletes a SharePoint Team site

        :type site_url: str
        """
        payload = {"siteUrl": site_url}
        qry = ServiceOperationQuery(self, "Delete", None, payload)
        self.context.add_query(qry)
        return self

    def ensure_team_for_group(self):
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "EnsureTeamForGroup", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_group_creation_context(self):
        """ """
        return_type = ClientResult(self.context, GroupCreationContext())
        qry = ServiceOperationQuery(
            self, "GetGroupCreationContext", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_status(self, group_id):
        # type: (str) -> ClientResult[GroupSiteInfo]
        """Get the status of a SharePoint site"""
        return_type = ClientResult(self.context, GroupSiteInfo())
        qry = ServiceOperationQuery(
            self, "GetSiteStatus", None, {"groupId": group_id}, None, return_type
        )

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.method = HttpMethod.Get
            request.url += "?groupId='{0}'".format(group_id)

        self.context.add_query(qry).before_query_execute(_construct_request)
        return return_type

    def get_current_user_joined_teams(
        self, get_logo_data=False, force_cache_update=False
    ):
        """
        Get the teams in Microsoft Teams that the current user is a direct member of.
        :type get_logo_data: bool
        :type force_cache_update: bool
        """
        result = ClientResult(self.context, str())
        payload = {"getLogoData": get_logo_data, "forceCacheUpdate": force_cache_update}
        qry = ServiceOperationQuery(
            self, "GetCurrentUserJoinedTeams", None, payload, None, result
        )
        self.context.add_query(qry)
        return result

    def get_current_user_shared_channel_member_groups(self):
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self,
            "GetCurrentUserSharedChannelMemberGroups",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_team_channels(self, team_id, use_staging_endpoint=False):
        """
        Retrieves the channels associated with a specific Microsoft 365 Group (or Team)

        :param str team_id:
        :param bool use_staging_endpoint:
        """
        return_type = ClientResult(self.context)
        payload = {"teamId": team_id, "useStagingEndpoint": use_staging_endpoint}
        qry = ServiceOperationQuery(
            self, "GetTeamChannels", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_team_channels_direct(self, team_id):
        """
        :param str team_id:
        """
        return_type = ClientResult(self.context, str())
        payload = {
            "teamId": team_id,
        }
        qry = ServiceOperationQuery(
            self, "GetTeamChannelsDirect", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_team_channels_with_site_url(self, site_url):
        """
        Returns a list of team channels associated with a Microsoft 365 Group.
        :param str site_url:
        """
        return_type = ClientResult(self.context, ChannelInfoCollection())
        payload = {
            "siteUrl": site_url,
        }
        qry = ServiceOperationQuery(
            self, "GetTeamChannelsWithSiteUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def notebook(self, group_id):
        """
        :param str group_id:
        """
        return_type = ClientResult(self.context, str())
        payload = {"groupId": group_id}
        qry = ServiceOperationQuery(self, "Notebook", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def recent_and_joined_teams(
        self,
        include_recent=None,
        include_teams=None,
        include_pinned=None,
        existing_joined_teams_data=None,
    ):
        """
        Retrieves a list of teams that a user has recently accessed or joined

        :param bool include_recent:
        :param bool include_teams:
        :param bool include_pinned:
        :param str existing_joined_teams_data:
        """
        return_type = ClientResult(self.context, RecentAndJoinedTeamsResponse())
        payload = {
            "includeRecent": include_recent,
            "includeTeams": include_teams,
            "includePinned": include_pinned,
            "existingJoinedTeamsData": existing_joined_teams_data,
        }
        qry = ServiceOperationQuery(
            self, "RecentAndJoinedTeams", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.GroupSiteManager"
