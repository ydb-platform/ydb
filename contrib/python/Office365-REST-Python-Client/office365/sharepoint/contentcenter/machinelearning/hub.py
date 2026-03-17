from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.compliance.tag import ComplianceTag
from office365.sharepoint.contentcenter.machinelearning.enabled import (
    SPMachineLearningEnabled,
)
from office365.sharepoint.contentcenter.machinelearning.models.collection import (
    SPMachineLearningModelCollection,
)
from office365.sharepoint.contentcenter.machinelearning.samples.collection import (
    SPMachineLearningSampleCollection,
)
from office365.sharepoint.contentcenter.syntex_models_landing_info import (
    SyntexModelsLandingInfo,
)
from office365.sharepoint.entity import Entity


class SPMachineLearningHub(Entity):
    def get_by_content_type_id(self, content_type_id):
        """
        :param str content_type_id:
        """
        return_type = SyntexModelsLandingInfo(self.context)
        payload = {"contentTypeId": content_type_id}
        qry = ServiceOperationQuery(
            self, "GetByContentTypeId", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_models(
        self,
        list_id=None,
        model_types=None,
        publication_types=None,
        include_management_not_allowed_models=None,
    ):
        """
        :param str list_id:
        :param int model_types:
        :param int publication_types:
        :param bool include_management_not_allowed_models:
        """
        return_type = SPMachineLearningModelCollection(self.context)
        payload = {
            "listId": list_id,
            "modelTypes": model_types,
            "publicationTypes": publication_types,
            "includeManagementNotAllowedModels": include_management_not_allowed_models,
        }
        qry = ServiceOperationQuery(self, "GetModels", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_retention_labels(self):
        return_type = ClientResult(self.context, ClientValueCollection(ComplianceTag))
        qry = ServiceOperationQuery(
            self, "GetRetentionLabels", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "MachineLearningEnabled": self.machine_learning_enabled,
            }
            default_value = property_mapping.get(name, None)
        return super(SPMachineLearningHub, self).get_property(name, default_value)

    @property
    def is_default_content_center(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("IsDefaultContentCenter", None)

    @property
    def machine_learning_capture_enabled(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("MachineLearningCaptureEnabled", None)

    @property
    def machine_learning_enabled(self):
        return self.properties.get(
            "MachineLearningEnabled",
            SPMachineLearningEnabled(
                self.context, ResourcePath("MachineLearningEnabled", self.resource_path)
            ),
        )

    @property
    def models(self):
        return self.properties.get(
            "Models",
            SPMachineLearningModelCollection(
                self.context, ResourcePath("Models", self.resource_path)
            ),
        )

    @property
    def samples(self):
        return self.properties.get(
            "Samples",
            SPMachineLearningSampleCollection(
                self.context, ResourcePath("Samples", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.ContentCenter.SPMachineLearningHub"
