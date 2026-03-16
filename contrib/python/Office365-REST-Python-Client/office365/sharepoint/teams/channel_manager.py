from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.sites.team_site_data import TeamSiteData
from office365.sharepoint.teams.channel import TeamChannel


class TeamChannelManager(Entity):
    """This class is a placeholder for all TeamChannel related methods."""

    @staticmethod
    def add_team_channel(
        context, channel_url, private_channel=False, private_channel_group_owner=None
    ):
        """
        Create Team Channel based folder with specific prodID.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str channel_url:  Team channel URL to be stored in the folder metadata.
        :param bool private_channel:
        :param str private_channel_group_owner:
        """
        manager = TeamChannelManager(context)
        payload = {
            "teamChannelUrl": channel_url,
            "privateChannel": private_channel,
            "privateChannelGroupOwner": private_channel_group_owner,
        }
        return_type = TeamChannel(context)
        qry = ServiceOperationQuery(
            manager, "AddTeamChannel", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_team_site_data(context, ignore_validation=True):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param bool ignore_validation:
        """
        payload = {
            "ignoreValidation": ignore_validation,
        }
        return_type = TeamSiteData(context)
        qry = ServiceOperationQuery(
            TeamChannelManager(context),
            "GetTeamSiteData",
            None,
            payload,
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def save_conversations(
        context, list_url, list_item_id, updated_conversations_object
    ):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str list_url:
        :param int list_item_id:
        :param str updated_conversations_object:
        """
        payload = {
            "listUrl": list_url,
            "listItemId": list_item_id,
            "updatedConversationsObject": updated_conversations_object,
        }
        binding_type = TeamChannelManager(context)
        qry = ServiceOperationQuery(
            binding_type, "SaveConversations", None, payload, is_static=True
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def sync_teamsite_settings(context):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        """
        return_type = TeamSiteData(context)
        qry = ServiceOperationQuery(
            TeamChannelManager(context),
            "SyncTeamSiteSettings",
            return_type=return_type,
            is_static=True,
        )
        context.add_query(qry)
        return return_type
