from office365.runtime.client_value import ClientValue


class SiteScriptActionResult(ClientValue):
    def __init__(self, outcome_text=None, target=None):
        """
        :param str outcome_text:
        :param str target:
        """
        self.OutcomeText = outcome_text
        self.Target = target

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteScriptActionResult"
