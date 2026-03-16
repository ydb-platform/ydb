from office365.runtime.client_value import ClientValue


class SubwebQuery(ClientValue):
    """Defines a query to specify which child Web sites to return from a Web site."""

    def __init__(self, configuration_filter=-1, web_template_filter=-1):
        super(SubwebQuery, self).__init__()
        """Specifies the site definition."""
        self.WebTemplateFilter = web_template_filter
        """ A 16-bit integer that specifies the identifier of a configuration"""
        self.ConfigurationFilter = configuration_filter
