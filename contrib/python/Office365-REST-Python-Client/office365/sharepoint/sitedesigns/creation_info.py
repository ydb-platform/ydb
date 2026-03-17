import uuid

from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class SiteDesignCreationInfo(ClientValue):
    def __init__(
        self,
        _id=None,
        title=None,
        description=None,
        web_template=None,
        site_script_ids=None,
        design_package_id=None,
    ):
        """
        :param str or None _id: The ID of the site design to apply.
        :param str or None title: The display name of the site design.
        :param str or None description: The display description of site design.
        :param str or None web_template: Identifies which base template to add the design to. Use the value 64 for the
            Team site template, and the value 68 for the Communication site template.
        :param list[UUID] or None site_script_ids: A list of one or more site scripts. Each is identified by an ID.
            The scripts will run in the order listed.
        :param str or None design_package_id:
        """
        self.Id = _id
        self.Title = title
        self.Description = description
        self.WebTemplate = web_template
        self.SiteScriptIds = ClientValueCollection(uuid.UUID, site_script_ids)
        self.DesignPackageId = design_package_id

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteDesignCreationInfo"
