from office365.runtime.client_value import ClientValue


class SearchHit(ClientValue):
    def __init__(
        self, content_source=None, summary=None, resource=None, result_template_id=None
    ):
        """
        Represents a single result within the list of search results.
        :param str content_source:
        :param str summary: A summary of the result, if a summary is available.
        :param office365.entity.Entity resource: The underlying Microsoft Graph representation of the search result.
        :param str result_template_id: ID of the result template used to render the search result. This ID must map to
            a display layout in the resultTemplates dictionary that is also included in the searchResponse.
        """
        self.contentSource = content_source
        self.summary = summary
        self.resource = resource
        self.resultTemplateId = result_template_id
