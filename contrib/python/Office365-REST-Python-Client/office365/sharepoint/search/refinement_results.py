from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.refiner.refiner import Refiner


class RefinementResults(ClientValue):
    """
    The RefinementResults table contains refinement results that apply to the search query.
    """

    def __init__(
        self,
        group_template_id=None,
        item_template_id=None,
        refiners=None,
        properties=None,
        result_title=None,
        result_title_url=None,
    ):
        """
        :param str group_template_id:  Specifies the identifier of the layout template that specifies how the results
            returned will be arranged.
        :param str item_template_id: Specifies the identifier of the layout template that specifies how the result
            item will be displayed
        :param list[Refiner] refiners: Contains the list of refiners
        :param dict properties: Specifies a property bag of key value pairs.
        :param str result_title: Specifies the title associated with results for the transformed query by query
             rule action. MUST NOT be more than 64 characters in length.
        :param str result_title_url: pecifies the URL to be linked to the ResultTitle. MUST NOT be more than
             2048 characters in length.
        """
        self.GroupTemplateId = group_template_id
        self.ItemTemplateId = item_template_id
        self.Refiners = ClientValueCollection(Refiner, refiners)
        self.Properties = properties
        self.ResultTitle = result_title
        self.ResultTitleUrl = result_title_url

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.RefinementResults"
