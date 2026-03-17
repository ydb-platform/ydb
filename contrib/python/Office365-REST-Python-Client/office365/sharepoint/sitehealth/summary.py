from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.sitehealth.result import SiteHealthResult


class SiteHealthSummary(Entity):
    """Specifies a summary of the results of running a set of site collection sitehealth rules."""

    @property
    def failed_error_count(self):
        # type: () -> Optional[int]
        """Specifies the number of site collection sitehealth rules that failed with an error."""
        return self.properties.get("FailedErrorCount", None)

    @property
    def failed_warning_count(self):
        # type: () -> Optional[int]
        """Specifies the number of site collection sitehealth rules that failed with a warning."""
        return self.properties.get("FailedWarningCount", None)

    @property
    def passed_count(self):
        # type: () -> Optional[int]
        """Specifies the number of site collection sitehealth rules that passed."""
        return self.properties.get("PassedCount", None)

    @property
    def results(self):
        """Specifies a list of site collection sitehealth rule results, one for each site collection sitehealth rule that
        was run."""
        return self.properties.get("Results", ClientValueCollection(SiteHealthResult))

    @property
    def entity_type_name(self):
        return "SP.SiteHealth.SiteHealthSummary"
