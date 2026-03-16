from typing import TYPE_CHECKING

from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity import Entity

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class MigrationUrlParser(Entity):
    """"""

    def __init__(
        self,
        context,
        user_input_destination_url,
        retrive_all_lists,
        retrieve_all_lists_sub_folders,
        force_my_site_default_list,
        migration_type,
        current_context_site_subscription_id,
    ):
        # type: (ClientContext, str, bool, bool, bool, str, str) -> None
        static_path = ServiceOperationPath(
            "SP.AppContextSite",
            {
                "userInputDestinationUrl": user_input_destination_url,
                "retriveAllLists": retrive_all_lists,
                "retrieveAllListsSubFolders": retrieve_all_lists_sub_folders,
                "forceMySiteDefaultList": force_my_site_default_list,
                "migrationType": migration_type,
                "currentContextSiteSubscriptionId": current_context_site_subscription_id,
            },
        )
        super(MigrationUrlParser, self).__init__(context, static_path)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Common.MigrationUrlParser"
