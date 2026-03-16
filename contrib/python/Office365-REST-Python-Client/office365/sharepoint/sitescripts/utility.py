from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.sitedesigns.metadata import SiteDesignMetadata
from office365.sharepoint.sitedesigns.principal import SiteDesignPrincipal
from office365.sharepoint.sitedesigns.task import SiteDesignTask
from office365.sharepoint.sitescripts.action_result import SiteScriptActionResult
from office365.sharepoint.sitescripts.metadata import SiteScriptMetadata
from office365.sharepoint.sitescripts.serialization_result import (
    SiteScriptSerializationResult,
)


class SiteScriptUtility(Entity):
    """Use class to automate provisioning new or existing modern
    SharePoint sites that use your own custom configurations.
    """

    def __init__(self, context):
        path = ResourcePath(
            "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteScriptUtility"
        )
        super(SiteScriptUtility, self).__init__(context, path)

    @staticmethod
    def create_list_design(context, info):
        """
        Creates a new list design available to users when they create a new list from the SharePoint start page.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param office365.sharepoint.sitedesigns.creation_info.SiteDesignCreationInfo info:
        """
        return_type = ClientResult(context, SiteDesignMetadata())
        utility = SiteScriptUtility(context)
        payload = {"info": info}
        qry = ServiceOperationQuery(
            utility, "CreateListDesign", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_list_designs(context, store=None):
        """
        Gets a list designs.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str store:
        """
        return_type = ClientResult(context, SiteDesignMetadata())
        utility = SiteScriptUtility(context)
        payload = {"store": store}
        qry = ServiceOperationQuery(
            utility, "GetListDesigns", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def add_site_design_task(context, web_url, site_design_id):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str web_url:
        :param str site_design_id: The ID of the site design to apply
        """
        return_type = ClientResult(context, SiteDesignTask())
        utility = SiteScriptUtility(context)
        payload = {"webUrl": web_url, "siteDesignId": site_design_id}
        qry = ServiceOperationQuery(
            utility, "AddSiteDesignTask", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_script_from_list(context, list_url, options=None, return_type=None):
        """
        Creates site script syntax from an existing SharePoint list.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str list_url:  URL of the list.
        :param dict or None options:
        :param ClientResult return_type:  Return type
        """
        if return_type is None:
            return_type = ClientResult(context)
        payload = {"listUrl": list_url, "options": options}
        utility = SiteScriptUtility(context)
        qry = ServiceOperationQuery(
            utility, "GetSiteScriptFromList", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_script_from_web(context, web_url, info=None, return_type=None):
        """
        Creates site script syntax from an existing SharePoint site.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str web_url:  URL of the web.
        :param SiteScriptSerializationInfo or None info:
        :param ClientResult return_type:  Return type
        """
        if return_type is None:
            return_type = ClientResult(context, SiteScriptSerializationResult())
        payload = {"webUrl": web_url, "info": info}
        utility = SiteScriptUtility(context)
        qry = ServiceOperationQuery(
            utility, "GetSiteScriptFromWeb", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def create_site_script(context, title, description, content):
        """
        Creates a new site script.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str title:
        :param str description:
        :param dict content:
        """
        return_type = ClientResult(context, SiteScriptMetadata())
        utility = SiteScriptUtility(context)
        params = {
            "Title": title,
            "Description": description,
        }
        qry = ServiceOperationQuery(
            utility, "CreateSiteScript", params, content, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def delete_site_script(context, _id):
        """
        Deletes a site script.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str _id:

        """
        utility = SiteScriptUtility(context)
        payload = {
            "id": _id,
        }
        qry = ServiceOperationQuery(
            utility, "DeleteSiteScript", None, payload, None, None
        )
        qry.static = True
        context.add_query(qry)
        return utility

    @staticmethod
    def get_site_scripts(context, store=None):
        """
        Gets a list of information on all existing site scripts.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str store:
        """
        return_type = ClientResult(context, ClientValueCollection(SiteScriptMetadata))
        utility = SiteScriptUtility(context)
        payload = {"store": store}
        qry = ServiceOperationQuery(
            utility, "GetSiteScripts", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def execute_site_script_action(context, action_definition):
        """
        Gets a list of information on all existing site scripts.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str action_definition:
        """
        return_type = ClientResult(
            context, ClientValueCollection(SiteScriptActionResult)
        )
        utility = SiteScriptUtility(context)
        payload = {"actionDefinition": action_definition}
        qry = ServiceOperationQuery(
            utility, "ExecuteSiteScriptAction", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def create_site_design(context, info):
        """
        Creates a new site design available to users when they create a new site from the SharePoint start page.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param office365.sharepoint.sitedesigns.creation_info.SiteDesignCreationInfo info:
        """
        return_type = ClientResult(context, SiteDesignMetadata())
        utility = SiteScriptUtility(context)
        payload = {"info": info}
        qry = ServiceOperationQuery(
            utility, "CreateSiteDesign", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def update_site_design(context, update_info):
        """
        Updates a site design with new values.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param SiteDesignMetadata update_info:
        """
        return_type = ClientResult(context, SiteDesignMetadata())
        utility = SiteScriptUtility(context)
        payload = {"updateInfo": update_info}
        qry = ServiceOperationQuery(
            utility, "UpdateSiteDesign", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_designs(context, include_untargeted=True, store=None):
        """
        Gets a list of information on existing site designs.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param bool include_untargeted:
        :param int or None store:
        """
        return_type = ClientResult(context, ClientValueCollection(SiteDesignMetadata))
        utility = SiteScriptUtility(context)
        payload = {"includeUntargeted": include_untargeted, "store": store}
        qry = ServiceOperationQuery(
            utility, "GetSiteDesigns", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_design_stages(context, site_design_id):
        """
        Gets a list of site design stages.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str site_design_id:
        """
        return_type = ClientResult(context)
        utility = SiteScriptUtility(context)
        qry = ServiceOperationQuery(
            utility, "GetSiteDesignStages", [site_design_id], None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_design_metadata(context, _id, store=None):
        """
        Gets information about a specific site script.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str _id:
        :param str store:
        """
        return_type = ClientResult(context, SiteDesignMetadata())
        utility = SiteScriptUtility(context)
        payload = {"id": _id, "store": store}
        qry = ServiceOperationQuery(
            utility, "GetSiteDesignMetadata", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_site_design_rights(context, id_):
        """
        Gets a list of principals that have access to a site design.

        :param str id_:
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context

        """
        return_type = EntityCollection(context, SiteDesignPrincipal)
        utility = SiteScriptUtility(context)
        qry = ServiceOperationQuery(
            utility, "GetSiteDesignRights", [id_], None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def grant_site_design_rights(context, _id, principal_names, granted_rights):
        """Grants access to a site design for one or more principals.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client
        :param str _id:
        :param list[str] principal_names:
        :param long granted_rights:
        """
        utility = SiteScriptUtility(context)
        payload = {
            "id": _id,
            "principalNames": ClientValueCollection(str, principal_names),
            "grantedRights": granted_rights,
        }
        qry = ServiceOperationQuery(utility, "GrantSiteDesignRights", None, payload)
        qry.static = True
        context.add_query(qry)
        return utility

    @staticmethod
    def delete_site_design(context, _id):
        """
        Deletes a site design.

        :type _id: str
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client
        """
        utility = SiteScriptUtility(context)
        qry = ServiceOperationQuery(utility, "DeleteSiteDesign", [_id])
        qry.static = True
        context.add_query(qry)
        return utility

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteScriptUtility"
