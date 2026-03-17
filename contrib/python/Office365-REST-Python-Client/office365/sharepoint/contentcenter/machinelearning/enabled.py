from typing import Optional

from office365.sharepoint.entity import Entity


class SPMachineLearningEnabled(Entity):
    @property
    def is_syntex_payg_enabled(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("IsSyntexPAYGEnabled", None)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.ContentCenter.SPMachineLearningEnabled"
