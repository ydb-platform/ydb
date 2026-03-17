# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class Activity:
    """For activities, this contains the activity recorded for the action. For comments, this contains
    the comment details. For transition requests, this contains the transition request details."""

    activity_type: Optional[ActivityType] = None

    comment: Optional[str] = None
    """User-provided comment associated with the activity, comment, or transition request."""

    creation_timestamp: Optional[int] = None
    """Creation time of the object, as a Unix timestamp in milliseconds."""

    from_stage: Optional[str] = None
    """Source stage of the transition (if the activity is stage transition related). Valid values are:
    
    * `None`: The initial stage of a model version.
    
    * `Staging`: Staging or pre-production stage.
    
    * `Production`: Production stage.
    
    * `Archived`: Archived stage."""

    id: Optional[str] = None
    """Unique identifier for the object."""

    last_updated_timestamp: Optional[int] = None
    """Time of the object at last update, as a Unix timestamp in milliseconds."""

    system_comment: Optional[str] = None
    """Comment made by system, for example explaining an activity of type `SYSTEM_TRANSITION`. It
    usually describes a side effect, such as a version being archived as part of another version's
    stage transition, and may not be returned for some activity types."""

    to_stage: Optional[str] = None
    """Target stage of the transition (if the activity is stage transition related). Valid values are:
    
    * `None`: The initial stage of a model version.
    
    * `Staging`: Staging or pre-production stage.
    
    * `Production`: Production stage.
    
    * `Archived`: Archived stage."""

    user_id: Optional[str] = None
    """The username of the user that created the object."""

    def as_dict(self) -> dict:
        """Serializes the Activity into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activity_type is not None:
            body["activity_type"] = self.activity_type.value
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.from_stage is not None:
            body["from_stage"] = self.from_stage
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.system_comment is not None:
            body["system_comment"] = self.system_comment
        if self.to_stage is not None:
            body["to_stage"] = self.to_stage
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Activity into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activity_type is not None:
            body["activity_type"] = self.activity_type
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.from_stage is not None:
            body["from_stage"] = self.from_stage
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.system_comment is not None:
            body["system_comment"] = self.system_comment
        if self.to_stage is not None:
            body["to_stage"] = self.to_stage
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Activity:
        """Deserializes the Activity from a dictionary."""
        return cls(
            activity_type=_enum(d, "activity_type", ActivityType),
            comment=d.get("comment", None),
            creation_timestamp=d.get("creation_timestamp", None),
            from_stage=d.get("from_stage", None),
            id=d.get("id", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            system_comment=d.get("system_comment", None),
            to_stage=d.get("to_stage", None),
            user_id=d.get("user_id", None),
        )


class ActivityAction(Enum):
    """An action that a user (with sufficient permissions) could take on an activity or comment.

    For activities, valid values are: * `APPROVE_TRANSITION_REQUEST`: Approve a transition request

    * `REJECT_TRANSITION_REQUEST`: Reject a transition request

    * `CANCEL_TRANSITION_REQUEST`: Cancel (delete) a transition request

    For comments, valid values are: * `EDIT_COMMENT`: Edit the comment

    * `DELETE_COMMENT`: Delete the comment"""

    APPROVE_TRANSITION_REQUEST = "APPROVE_TRANSITION_REQUEST"
    CANCEL_TRANSITION_REQUEST = "CANCEL_TRANSITION_REQUEST"
    DELETE_COMMENT = "DELETE_COMMENT"
    EDIT_COMMENT = "EDIT_COMMENT"
    REJECT_TRANSITION_REQUEST = "REJECT_TRANSITION_REQUEST"


class ActivityType(Enum):
    """Type of activity. Valid values are: * `APPLIED_TRANSITION`: User applied the corresponding stage
    transition.

    * `REQUESTED_TRANSITION`: User requested the corresponding stage transition.

    * `CANCELLED_REQUEST`: User cancelled an existing transition request.

    * `APPROVED_REQUEST`: User approved the corresponding stage transition.

    * `REJECTED_REQUEST`: User rejected the coressponding stage transition.

    * `SYSTEM_TRANSITION`: For events performed as a side effect, such as archiving existing model
    versions in a stage."""

    APPLIED_TRANSITION = "APPLIED_TRANSITION"
    APPROVED_REQUEST = "APPROVED_REQUEST"
    CANCELLED_REQUEST = "CANCELLED_REQUEST"
    NEW_COMMENT = "NEW_COMMENT"
    REJECTED_REQUEST = "REJECTED_REQUEST"
    REQUESTED_TRANSITION = "REQUESTED_TRANSITION"
    SYSTEM_TRANSITION = "SYSTEM_TRANSITION"


@dataclass
class ApproveTransitionRequestResponse:
    activity: Optional[Activity] = None
    """New activity generated as a result of this operation."""

    def as_dict(self) -> dict:
        """Serializes the ApproveTransitionRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activity:
            body["activity"] = self.activity.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ApproveTransitionRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activity:
            body["activity"] = self.activity
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ApproveTransitionRequestResponse:
        """Deserializes the ApproveTransitionRequestResponse from a dictionary."""
        return cls(activity=_from_dict(d, "activity", Activity))


@dataclass
class AuthConfig:
    uc_service_credential_name: Optional[str] = None
    """Name of the Unity Catalog service credential. This value will be set under the option
    databricks.serviceCredential"""

    def as_dict(self) -> dict:
        """Serializes the AuthConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.uc_service_credential_name is not None:
            body["uc_service_credential_name"] = self.uc_service_credential_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AuthConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.uc_service_credential_name is not None:
            body["uc_service_credential_name"] = self.uc_service_credential_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AuthConfig:
        """Deserializes the AuthConfig from a dictionary."""
        return cls(uc_service_credential_name=d.get("uc_service_credential_name", None))


@dataclass
class BackfillSource:
    delta_table_source: Optional[DeltaTableSource] = None
    """The Delta table source containing the historic data to backfill. Only the delta table name is
    used for backfill, the entity columns and timeseries column are ignored as they are defined by
    the associated KafkaSource."""

    def as_dict(self) -> dict:
        """Serializes the BackfillSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.delta_table_source:
            body["delta_table_source"] = self.delta_table_source.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BackfillSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.delta_table_source:
            body["delta_table_source"] = self.delta_table_source
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BackfillSource:
        """Deserializes the BackfillSource from a dictionary."""
        return cls(delta_table_source=_from_dict(d, "delta_table_source", DeltaTableSource))


@dataclass
class BatchCreateMaterializedFeaturesResponse:
    materialized_features: Optional[List[MaterializedFeature]] = None
    """The created materialized features with assigned IDs."""

    def as_dict(self) -> dict:
        """Serializes the BatchCreateMaterializedFeaturesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.materialized_features:
            body["materialized_features"] = [v.as_dict() for v in self.materialized_features]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BatchCreateMaterializedFeaturesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.materialized_features:
            body["materialized_features"] = self.materialized_features
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BatchCreateMaterializedFeaturesResponse:
        """Deserializes the BatchCreateMaterializedFeaturesResponse from a dictionary."""
        return cls(materialized_features=_repeated_dict(d, "materialized_features", MaterializedFeature))


@dataclass
class ColumnIdentifier:
    variant_expr_path: str
    """String representation of the column name or variant expression path. For nested fields, the leaf
    value is what will be present in materialized tables and expected to match at query time. For
    example, the leaf node of value:trip_details.location_details.pickup_zip is pickup_zip."""

    def as_dict(self) -> dict:
        """Serializes the ColumnIdentifier into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.variant_expr_path is not None:
            body["variant_expr_path"] = self.variant_expr_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ColumnIdentifier into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.variant_expr_path is not None:
            body["variant_expr_path"] = self.variant_expr_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ColumnIdentifier:
        """Deserializes the ColumnIdentifier from a dictionary."""
        return cls(variant_expr_path=d.get("variant_expr_path", None))


class CommentActivityAction(Enum):
    """An action that a user (with sufficient permissions) could take on an activity or comment.

    For activities, valid values are: * `APPROVE_TRANSITION_REQUEST`: Approve a transition request

    * `REJECT_TRANSITION_REQUEST`: Reject a transition request

    * `CANCEL_TRANSITION_REQUEST`: Cancel (delete) a transition request

    For comments, valid values are: * `EDIT_COMMENT`: Edit the comment

    * `DELETE_COMMENT`: Delete the comment"""

    APPROVE_TRANSITION_REQUEST = "APPROVE_TRANSITION_REQUEST"
    CANCEL_TRANSITION_REQUEST = "CANCEL_TRANSITION_REQUEST"
    DELETE_COMMENT = "DELETE_COMMENT"
    EDIT_COMMENT = "EDIT_COMMENT"
    REJECT_TRANSITION_REQUEST = "REJECT_TRANSITION_REQUEST"


@dataclass
class CommentObject:
    """For activities, this contains the activity recorded for the action. For comments, this contains
    the comment details. For transition requests, this contains the transition request details."""

    available_actions: Optional[List[CommentActivityAction]] = None
    """Array of actions on the activity allowed for the current viewer."""

    comment: Optional[str] = None
    """User-provided comment associated with the activity, comment, or transition request."""

    creation_timestamp: Optional[int] = None
    """Creation time of the object, as a Unix timestamp in milliseconds."""

    id: Optional[str] = None
    """Unique identifier for the object."""

    last_updated_timestamp: Optional[int] = None
    """Time of the object at last update, as a Unix timestamp in milliseconds."""

    user_id: Optional[str] = None
    """The username of the user that created the object."""

    def as_dict(self) -> dict:
        """Serializes the CommentObject into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.available_actions:
            body["available_actions"] = [v.value for v in self.available_actions]
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CommentObject into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.available_actions:
            body["available_actions"] = self.available_actions
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CommentObject:
        """Deserializes the CommentObject from a dictionary."""
        return cls(
            available_actions=_repeated_enum(d, "available_actions", CommentActivityAction),
            comment=d.get("comment", None),
            creation_timestamp=d.get("creation_timestamp", None),
            id=d.get("id", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            user_id=d.get("user_id", None),
        )


@dataclass
class ContinuousWindow:
    window_duration: str
    """The duration of the continuous window (must be positive)."""

    offset: Optional[str] = None
    """The offset of the continuous window (must be non-positive)."""

    def as_dict(self) -> dict:
        """Serializes the ContinuousWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.offset is not None:
            body["offset"] = self.offset
        if self.window_duration is not None:
            body["window_duration"] = self.window_duration
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ContinuousWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.offset is not None:
            body["offset"] = self.offset
        if self.window_duration is not None:
            body["window_duration"] = self.window_duration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ContinuousWindow:
        """Deserializes the ContinuousWindow from a dictionary."""
        return cls(offset=d.get("offset", None), window_duration=d.get("window_duration", None))


@dataclass
class CreateCommentResponse:
    comment: Optional[CommentObject] = None
    """New comment object"""

    def as_dict(self) -> dict:
        """Serializes the CreateCommentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment:
            body["comment"] = self.comment.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateCommentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment:
            body["comment"] = self.comment
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateCommentResponse:
        """Deserializes the CreateCommentResponse from a dictionary."""
        return cls(comment=_from_dict(d, "comment", CommentObject))


@dataclass
class CreateExperimentResponse:
    experiment_id: Optional[str] = None
    """Unique identifier for the experiment."""

    def as_dict(self) -> dict:
        """Serializes the CreateExperimentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateExperimentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateExperimentResponse:
        """Deserializes the CreateExperimentResponse from a dictionary."""
        return cls(experiment_id=d.get("experiment_id", None))


@dataclass
class CreateForecastingExperimentResponse:
    experiment_id: Optional[str] = None
    """The unique ID of the created forecasting experiment"""

    def as_dict(self) -> dict:
        """Serializes the CreateForecastingExperimentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateForecastingExperimentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateForecastingExperimentResponse:
        """Deserializes the CreateForecastingExperimentResponse from a dictionary."""
        return cls(experiment_id=d.get("experiment_id", None))


@dataclass
class CreateLoggedModelResponse:
    model: Optional[LoggedModel] = None
    """The newly created logged model."""

    def as_dict(self) -> dict:
        """Serializes the CreateLoggedModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model:
            body["model"] = self.model.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateLoggedModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model:
            body["model"] = self.model
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateLoggedModelResponse:
        """Deserializes the CreateLoggedModelResponse from a dictionary."""
        return cls(model=_from_dict(d, "model", LoggedModel))


@dataclass
class CreateMaterializedFeatureRequest:
    materialized_feature: MaterializedFeature
    """The materialized feature to create."""

    def as_dict(self) -> dict:
        """Serializes the CreateMaterializedFeatureRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.materialized_feature:
            body["materialized_feature"] = self.materialized_feature.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateMaterializedFeatureRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.materialized_feature:
            body["materialized_feature"] = self.materialized_feature
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateMaterializedFeatureRequest:
        """Deserializes the CreateMaterializedFeatureRequest from a dictionary."""
        return cls(materialized_feature=_from_dict(d, "materialized_feature", MaterializedFeature))


@dataclass
class CreateModelResponse:
    registered_model: Optional[Model] = None

    def as_dict(self) -> dict:
        """Serializes the CreateModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.registered_model:
            body["registered_model"] = self.registered_model.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.registered_model:
            body["registered_model"] = self.registered_model
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateModelResponse:
        """Deserializes the CreateModelResponse from a dictionary."""
        return cls(registered_model=_from_dict(d, "registered_model", Model))


@dataclass
class CreateModelVersionResponse:
    model_version: Optional[ModelVersion] = None
    """Return new version number generated for this model in registry."""

    def as_dict(self) -> dict:
        """Serializes the CreateModelVersionResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_version:
            body["model_version"] = self.model_version.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateModelVersionResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_version:
            body["model_version"] = self.model_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateModelVersionResponse:
        """Deserializes the CreateModelVersionResponse from a dictionary."""
        return cls(model_version=_from_dict(d, "model_version", ModelVersion))


@dataclass
class CreateRunResponse:
    run: Optional[Run] = None
    """The newly created run."""

    def as_dict(self) -> dict:
        """Serializes the CreateRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.run:
            body["run"] = self.run.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.run:
            body["run"] = self.run
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateRunResponse:
        """Deserializes the CreateRunResponse from a dictionary."""
        return cls(run=_from_dict(d, "run", Run))


@dataclass
class CreateTransitionRequestResponse:
    request: Optional[TransitionRequest] = None
    """New activity generated for stage transition request."""

    def as_dict(self) -> dict:
        """Serializes the CreateTransitionRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.request:
            body["request"] = self.request.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateTransitionRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.request:
            body["request"] = self.request
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateTransitionRequestResponse:
        """Deserializes the CreateTransitionRequestResponse from a dictionary."""
        return cls(request=_from_dict(d, "request", TransitionRequest))


@dataclass
class CreateWebhookResponse:
    webhook: Optional[RegistryWebhook] = None

    def as_dict(self) -> dict:
        """Serializes the CreateWebhookResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.webhook:
            body["webhook"] = self.webhook.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateWebhookResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.webhook:
            body["webhook"] = self.webhook
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateWebhookResponse:
        """Deserializes the CreateWebhookResponse from a dictionary."""
        return cls(webhook=_from_dict(d, "webhook", RegistryWebhook))


@dataclass
class DataSource:
    delta_table_source: Optional[DeltaTableSource] = None

    kafka_source: Optional[KafkaSource] = None

    def as_dict(self) -> dict:
        """Serializes the DataSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.delta_table_source:
            body["delta_table_source"] = self.delta_table_source.as_dict()
        if self.kafka_source:
            body["kafka_source"] = self.kafka_source.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DataSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.delta_table_source:
            body["delta_table_source"] = self.delta_table_source
        if self.kafka_source:
            body["kafka_source"] = self.kafka_source
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DataSource:
        """Deserializes the DataSource from a dictionary."""
        return cls(
            delta_table_source=_from_dict(d, "delta_table_source", DeltaTableSource),
            kafka_source=_from_dict(d, "kafka_source", KafkaSource),
        )


@dataclass
class Dataset:
    """Dataset. Represents a reference to data used for training, testing, or evaluation during the
    model development process."""

    name: str
    """The name of the dataset. E.g. “my.uc.table@2” “nyc-taxi-dataset”, “fantastic-elk-3”"""

    digest: str
    """Dataset digest, e.g. an md5 hash of the dataset that uniquely identifies it within datasets of
    the same name."""

    source_type: str
    """The type of the dataset source, e.g. ‘databricks-uc-table’, ‘DBFS’, ‘S3’, ..."""

    source: str
    """Source information for the dataset. Note that the source may not exactly reproduce the dataset
    if it was transformed / modified before use with MLflow."""

    profile: Optional[str] = None
    """The profile of the dataset. Summary statistics for the dataset, such as the number of rows in a
    table, the mean / std / mode of each column in a table, or the number of elements in an array."""

    schema: Optional[str] = None
    """The schema of the dataset. E.g., MLflow ColSpec JSON for a dataframe, MLflow TensorSpec JSON for
    an ndarray, or another schema format."""

    def as_dict(self) -> dict:
        """Serializes the Dataset into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.digest is not None:
            body["digest"] = self.digest
        if self.name is not None:
            body["name"] = self.name
        if self.profile is not None:
            body["profile"] = self.profile
        if self.schema is not None:
            body["schema"] = self.schema
        if self.source is not None:
            body["source"] = self.source
        if self.source_type is not None:
            body["source_type"] = self.source_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Dataset into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.digest is not None:
            body["digest"] = self.digest
        if self.name is not None:
            body["name"] = self.name
        if self.profile is not None:
            body["profile"] = self.profile
        if self.schema is not None:
            body["schema"] = self.schema
        if self.source is not None:
            body["source"] = self.source
        if self.source_type is not None:
            body["source_type"] = self.source_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Dataset:
        """Deserializes the Dataset from a dictionary."""
        return cls(
            digest=d.get("digest", None),
            name=d.get("name", None),
            profile=d.get("profile", None),
            schema=d.get("schema", None),
            source=d.get("source", None),
            source_type=d.get("source_type", None),
        )


@dataclass
class DatasetInput:
    """DatasetInput. Represents a dataset and input tags."""

    dataset: Dataset
    """The dataset being used as a Run input."""

    tags: Optional[List[InputTag]] = None
    """A list of tags for the dataset input, e.g. a “context” tag with value “training”"""

    def as_dict(self) -> dict:
        """Serializes the DatasetInput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dataset:
            body["dataset"] = self.dataset.as_dict()
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DatasetInput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dataset:
            body["dataset"] = self.dataset
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DatasetInput:
        """Deserializes the DatasetInput from a dictionary."""
        return cls(dataset=_from_dict(d, "dataset", Dataset), tags=_repeated_dict(d, "tags", InputTag))


@dataclass
class DeleteCommentResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteCommentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteCommentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteCommentResponse:
        """Deserializes the DeleteCommentResponse from a dictionary."""
        return cls()


@dataclass
class DeleteExperimentResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteExperimentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteExperimentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteExperimentResponse:
        """Deserializes the DeleteExperimentResponse from a dictionary."""
        return cls()


@dataclass
class DeleteLoggedModelResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteLoggedModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteLoggedModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteLoggedModelResponse:
        """Deserializes the DeleteLoggedModelResponse from a dictionary."""
        return cls()


@dataclass
class DeleteLoggedModelTagResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteLoggedModelTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteLoggedModelTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteLoggedModelTagResponse:
        """Deserializes the DeleteLoggedModelTagResponse from a dictionary."""
        return cls()


@dataclass
class DeleteModelResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteModelResponse:
        """Deserializes the DeleteModelResponse from a dictionary."""
        return cls()


@dataclass
class DeleteModelTagResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteModelTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteModelTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteModelTagResponse:
        """Deserializes the DeleteModelTagResponse from a dictionary."""
        return cls()


@dataclass
class DeleteModelVersionResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteModelVersionResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteModelVersionResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteModelVersionResponse:
        """Deserializes the DeleteModelVersionResponse from a dictionary."""
        return cls()


@dataclass
class DeleteModelVersionTagResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteModelVersionTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteModelVersionTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteModelVersionTagResponse:
        """Deserializes the DeleteModelVersionTagResponse from a dictionary."""
        return cls()


@dataclass
class DeleteRunResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteRunResponse:
        """Deserializes the DeleteRunResponse from a dictionary."""
        return cls()


@dataclass
class DeleteRunsResponse:
    runs_deleted: Optional[int] = None
    """The number of runs deleted."""

    def as_dict(self) -> dict:
        """Serializes the DeleteRunsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.runs_deleted is not None:
            body["runs_deleted"] = self.runs_deleted
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteRunsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.runs_deleted is not None:
            body["runs_deleted"] = self.runs_deleted
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteRunsResponse:
        """Deserializes the DeleteRunsResponse from a dictionary."""
        return cls(runs_deleted=d.get("runs_deleted", None))


@dataclass
class DeleteTagResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteTagResponse:
        """Deserializes the DeleteTagResponse from a dictionary."""
        return cls()


@dataclass
class DeleteTransitionRequestResponse:
    activity: Optional[Activity] = None
    """New activity generated as a result of this operation."""

    def as_dict(self) -> dict:
        """Serializes the DeleteTransitionRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activity:
            body["activity"] = self.activity.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteTransitionRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activity:
            body["activity"] = self.activity
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteTransitionRequestResponse:
        """Deserializes the DeleteTransitionRequestResponse from a dictionary."""
        return cls(activity=_from_dict(d, "activity", Activity))


@dataclass
class DeleteWebhookResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteWebhookResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteWebhookResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteWebhookResponse:
        """Deserializes the DeleteWebhookResponse from a dictionary."""
        return cls()


@dataclass
class DeltaTableSource:
    full_name: str
    """The full three-part (catalog, schema, table) name of the Delta table."""

    entity_columns: List[str]
    """The entity columns of the Delta table."""

    timeseries_column: str
    """The timeseries column of the Delta table."""

    def as_dict(self) -> dict:
        """Serializes the DeltaTableSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entity_columns:
            body["entity_columns"] = [v for v in self.entity_columns]
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.timeseries_column is not None:
            body["timeseries_column"] = self.timeseries_column
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeltaTableSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entity_columns:
            body["entity_columns"] = self.entity_columns
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.timeseries_column is not None:
            body["timeseries_column"] = self.timeseries_column
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeltaTableSource:
        """Deserializes the DeltaTableSource from a dictionary."""
        return cls(
            entity_columns=d.get("entity_columns", None),
            full_name=d.get("full_name", None),
            timeseries_column=d.get("timeseries_column", None),
        )


@dataclass
class Experiment:
    """An experiment and its metadata."""

    artifact_location: Optional[str] = None
    """Location where artifacts for the experiment are stored."""

    creation_time: Optional[int] = None
    """Creation time"""

    experiment_id: Optional[str] = None
    """Unique identifier for the experiment."""

    last_update_time: Optional[int] = None
    """Last update time"""

    lifecycle_stage: Optional[str] = None
    """Current life cycle stage of the experiment: "active" or "deleted". Deleted experiments are not
    returned by APIs."""

    name: Optional[str] = None
    """Human readable name that identifies the experiment."""

    tags: Optional[List[ExperimentTag]] = None
    """Tags: Additional metadata key-value pairs."""

    def as_dict(self) -> dict:
        """Serializes the Experiment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifact_location is not None:
            body["artifact_location"] = self.artifact_location
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.last_update_time is not None:
            body["last_update_time"] = self.last_update_time
        if self.lifecycle_stage is not None:
            body["lifecycle_stage"] = self.lifecycle_stage
        if self.name is not None:
            body["name"] = self.name
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Experiment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifact_location is not None:
            body["artifact_location"] = self.artifact_location
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.last_update_time is not None:
            body["last_update_time"] = self.last_update_time
        if self.lifecycle_stage is not None:
            body["lifecycle_stage"] = self.lifecycle_stage
        if self.name is not None:
            body["name"] = self.name
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Experiment:
        """Deserializes the Experiment from a dictionary."""
        return cls(
            artifact_location=d.get("artifact_location", None),
            creation_time=d.get("creation_time", None),
            experiment_id=d.get("experiment_id", None),
            last_update_time=d.get("last_update_time", None),
            lifecycle_stage=d.get("lifecycle_stage", None),
            name=d.get("name", None),
            tags=_repeated_dict(d, "tags", ExperimentTag),
        )


@dataclass
class ExperimentAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[ExperimentPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the ExperimentAccessControlRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExperimentAccessControlRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExperimentAccessControlRequest:
        """Deserializes the ExperimentAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", ExperimentPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class ExperimentAccessControlResponse:
    all_permissions: Optional[List[ExperimentPermission]] = None
    """All permissions."""

    display_name: Optional[str] = None
    """Display name of the user or service principal."""

    group_name: Optional[str] = None
    """name of the group"""

    service_principal_name: Optional[str] = None
    """Name of the service principal."""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the ExperimentAccessControlResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = [v.as_dict() for v in self.all_permissions]
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExperimentAccessControlResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = self.all_permissions
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExperimentAccessControlResponse:
        """Deserializes the ExperimentAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", ExperimentPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class ExperimentPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[ExperimentPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the ExperimentPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExperimentPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExperimentPermission:
        """Deserializes the ExperimentPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", ExperimentPermissionLevel),
        )


class ExperimentPermissionLevel(Enum):
    """Permission level"""

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_READ = "CAN_READ"


@dataclass
class ExperimentPermissions:
    access_control_list: Optional[List[ExperimentAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExperimentPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExperimentPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExperimentPermissions:
        """Deserializes the ExperimentPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", ExperimentAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class ExperimentPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[ExperimentPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the ExperimentPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExperimentPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExperimentPermissionsDescription:
        """Deserializes the ExperimentPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None),
            permission_level=_enum(d, "permission_level", ExperimentPermissionLevel),
        )


@dataclass
class ExperimentTag:
    """A tag for an experiment."""

    key: Optional[str] = None
    """The tag key."""

    value: Optional[str] = None
    """The tag value."""

    def as_dict(self) -> dict:
        """Serializes the ExperimentTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExperimentTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExperimentTag:
        """Deserializes the ExperimentTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class Feature:
    full_name: str
    """The full three-part name (catalog, schema, name) of the feature."""

    source: DataSource
    """The data source of the feature."""

    inputs: List[str]
    """The input columns from which the feature is computed."""

    function: Function
    """The function by which the feature is computed."""

    description: Optional[str] = None
    """The description of the feature."""

    filter_condition: Optional[str] = None
    """The filter condition applied to the source data before aggregation."""

    lineage_context: Optional[LineageContext] = None
    """WARNING: This field is primarily intended for internal use by Databricks systems and is
    automatically populated when features are created through Databricks notebooks or jobs. Users
    should not manually set this field as incorrect values may lead to inaccurate lineage tracking
    or unexpected behavior. This field will be set by feature-engineering client and should be left
    unset by SDK and terraform users."""

    time_window: Optional[TimeWindow] = None
    """The time window in which the feature is computed."""

    def as_dict(self) -> dict:
        """Serializes the Feature into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.filter_condition is not None:
            body["filter_condition"] = self.filter_condition
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.function:
            body["function"] = self.function.as_dict()
        if self.inputs:
            body["inputs"] = [v for v in self.inputs]
        if self.lineage_context:
            body["lineage_context"] = self.lineage_context.as_dict()
        if self.source:
            body["source"] = self.source.as_dict()
        if self.time_window:
            body["time_window"] = self.time_window.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Feature into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.filter_condition is not None:
            body["filter_condition"] = self.filter_condition
        if self.full_name is not None:
            body["full_name"] = self.full_name
        if self.function:
            body["function"] = self.function
        if self.inputs:
            body["inputs"] = self.inputs
        if self.lineage_context:
            body["lineage_context"] = self.lineage_context
        if self.source:
            body["source"] = self.source
        if self.time_window:
            body["time_window"] = self.time_window
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Feature:
        """Deserializes the Feature from a dictionary."""
        return cls(
            description=d.get("description", None),
            filter_condition=d.get("filter_condition", None),
            full_name=d.get("full_name", None),
            function=_from_dict(d, "function", Function),
            inputs=d.get("inputs", None),
            lineage_context=_from_dict(d, "lineage_context", LineageContext),
            source=_from_dict(d, "source", DataSource),
            time_window=_from_dict(d, "time_window", TimeWindow),
        )


@dataclass
class FeatureLineage:
    feature_specs: Optional[List[FeatureLineageFeatureSpec]] = None
    """List of feature specs that contain this feature."""

    models: Optional[List[FeatureLineageModel]] = None
    """List of Unity Catalog models that were trained on this feature."""

    online_features: Optional[List[FeatureLineageOnlineFeature]] = None
    """List of online features that use this feature as source."""

    def as_dict(self) -> dict:
        """Serializes the FeatureLineage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.feature_specs:
            body["feature_specs"] = [v.as_dict() for v in self.feature_specs]
        if self.models:
            body["models"] = [v.as_dict() for v in self.models]
        if self.online_features:
            body["online_features"] = [v.as_dict() for v in self.online_features]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FeatureLineage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.feature_specs:
            body["feature_specs"] = self.feature_specs
        if self.models:
            body["models"] = self.models
        if self.online_features:
            body["online_features"] = self.online_features
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FeatureLineage:
        """Deserializes the FeatureLineage from a dictionary."""
        return cls(
            feature_specs=_repeated_dict(d, "feature_specs", FeatureLineageFeatureSpec),
            models=_repeated_dict(d, "models", FeatureLineageModel),
            online_features=_repeated_dict(d, "online_features", FeatureLineageOnlineFeature),
        )


@dataclass
class FeatureLineageFeatureSpec:
    name: Optional[str] = None
    """The full name of the feature spec in Unity Catalog."""

    def as_dict(self) -> dict:
        """Serializes the FeatureLineageFeatureSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FeatureLineageFeatureSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FeatureLineageFeatureSpec:
        """Deserializes the FeatureLineageFeatureSpec from a dictionary."""
        return cls(name=d.get("name", None))


@dataclass
class FeatureLineageModel:
    name: Optional[str] = None
    """The full name of the model in Unity Catalog."""

    version: Optional[int] = None
    """The version of the model."""

    def as_dict(self) -> dict:
        """Serializes the FeatureLineageModel into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FeatureLineageModel into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FeatureLineageModel:
        """Deserializes the FeatureLineageModel from a dictionary."""
        return cls(name=d.get("name", None), version=d.get("version", None))


@dataclass
class FeatureLineageOnlineFeature:
    feature_name: Optional[str] = None
    """The name of the online feature (column name)."""

    table_name: Optional[str] = None
    """The full name of the online table in Unity Catalog."""

    def as_dict(self) -> dict:
        """Serializes the FeatureLineageOnlineFeature into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.feature_name is not None:
            body["feature_name"] = self.feature_name
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FeatureLineageOnlineFeature into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.feature_name is not None:
            body["feature_name"] = self.feature_name
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FeatureLineageOnlineFeature:
        """Deserializes the FeatureLineageOnlineFeature from a dictionary."""
        return cls(feature_name=d.get("feature_name", None), table_name=d.get("table_name", None))


@dataclass
class FeatureList:
    """Feature list wrap all the features for a model version"""

    features: Optional[List[LinkedFeature]] = None

    def as_dict(self) -> dict:
        """Serializes the FeatureList into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.features:
            body["features"] = [v.as_dict() for v in self.features]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FeatureList into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.features:
            body["features"] = self.features
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FeatureList:
        """Deserializes the FeatureList from a dictionary."""
        return cls(features=_repeated_dict(d, "features", LinkedFeature))


@dataclass
class FeatureTag:
    """Represents a tag on a feature in a feature table."""

    key: str

    value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the FeatureTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FeatureTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FeatureTag:
        """Deserializes the FeatureTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class FileInfo:
    """Metadata of a single artifact file or directory."""

    file_size: Optional[int] = None
    """The size in bytes of the file. Unset for directories."""

    is_dir: Optional[bool] = None
    """Whether the path is a directory."""

    path: Optional[str] = None
    """The path relative to the root artifact directory run."""

    def as_dict(self) -> dict:
        """Serializes the FileInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_size is not None:
            body["file_size"] = self.file_size
        if self.is_dir is not None:
            body["is_dir"] = self.is_dir
        if self.path is not None:
            body["path"] = self.path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_size is not None:
            body["file_size"] = self.file_size
        if self.is_dir is not None:
            body["is_dir"] = self.is_dir
        if self.path is not None:
            body["path"] = self.path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileInfo:
        """Deserializes the FileInfo from a dictionary."""
        return cls(file_size=d.get("file_size", None), is_dir=d.get("is_dir", None), path=d.get("path", None))


@dataclass
class FinalizeLoggedModelResponse:
    model: Optional[LoggedModel] = None
    """The updated logged model."""

    def as_dict(self) -> dict:
        """Serializes the FinalizeLoggedModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model:
            body["model"] = self.model.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FinalizeLoggedModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model:
            body["model"] = self.model
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FinalizeLoggedModelResponse:
        """Deserializes the FinalizeLoggedModelResponse from a dictionary."""
        return cls(model=_from_dict(d, "model", LoggedModel))


@dataclass
class ForecastingExperiment:
    """Represents a forecasting experiment with its unique identifier, URL, and state."""

    experiment_id: Optional[str] = None
    """The unique ID for the forecasting experiment."""

    experiment_page_url: Optional[str] = None
    """The URL to the forecasting experiment page."""

    state: Optional[ForecastingExperimentState] = None
    """The current state of the forecasting experiment."""

    def as_dict(self) -> dict:
        """Serializes the ForecastingExperiment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.experiment_page_url is not None:
            body["experiment_page_url"] = self.experiment_page_url
        if self.state is not None:
            body["state"] = self.state.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ForecastingExperiment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.experiment_page_url is not None:
            body["experiment_page_url"] = self.experiment_page_url
        if self.state is not None:
            body["state"] = self.state
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ForecastingExperiment:
        """Deserializes the ForecastingExperiment from a dictionary."""
        return cls(
            experiment_id=d.get("experiment_id", None),
            experiment_page_url=d.get("experiment_page_url", None),
            state=_enum(d, "state", ForecastingExperimentState),
        )


class ForecastingExperimentState(Enum):

    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"


@dataclass
class Function:
    function_type: FunctionFunctionType
    """The type of the function."""

    extra_parameters: Optional[List[FunctionExtraParameter]] = None
    """Extra parameters for parameterized functions."""

    def as_dict(self) -> dict:
        """Serializes the Function into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.extra_parameters:
            body["extra_parameters"] = [v.as_dict() for v in self.extra_parameters]
        if self.function_type is not None:
            body["function_type"] = self.function_type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Function into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.extra_parameters:
            body["extra_parameters"] = self.extra_parameters
        if self.function_type is not None:
            body["function_type"] = self.function_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Function:
        """Deserializes the Function from a dictionary."""
        return cls(
            extra_parameters=_repeated_dict(d, "extra_parameters", FunctionExtraParameter),
            function_type=_enum(d, "function_type", FunctionFunctionType),
        )


@dataclass
class FunctionExtraParameter:
    key: str
    """The name of the parameter."""

    value: str
    """The value of the parameter."""

    def as_dict(self) -> dict:
        """Serializes the FunctionExtraParameter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FunctionExtraParameter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FunctionExtraParameter:
        """Deserializes the FunctionExtraParameter from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


class FunctionFunctionType(Enum):

    APPROX_COUNT_DISTINCT = "APPROX_COUNT_DISTINCT"
    APPROX_PERCENTILE = "APPROX_PERCENTILE"
    AVG = "AVG"
    COUNT = "COUNT"
    FIRST = "FIRST"
    LAST = "LAST"
    MAX = "MAX"
    MIN = "MIN"
    STDDEV_POP = "STDDEV_POP"
    STDDEV_SAMP = "STDDEV_SAMP"
    SUM = "SUM"
    VAR_POP = "VAR_POP"
    VAR_SAMP = "VAR_SAMP"


@dataclass
class GetExperimentByNameResponse:
    experiment: Optional[Experiment] = None
    """Experiment details."""

    def as_dict(self) -> dict:
        """Serializes the GetExperimentByNameResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiment:
            body["experiment"] = self.experiment.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetExperimentByNameResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiment:
            body["experiment"] = self.experiment
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetExperimentByNameResponse:
        """Deserializes the GetExperimentByNameResponse from a dictionary."""
        return cls(experiment=_from_dict(d, "experiment", Experiment))


@dataclass
class GetExperimentPermissionLevelsResponse:
    permission_levels: Optional[List[ExperimentPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetExperimentPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetExperimentPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetExperimentPermissionLevelsResponse:
        """Deserializes the GetExperimentPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", ExperimentPermissionsDescription))


@dataclass
class GetExperimentResponse:
    experiment: Optional[Experiment] = None
    """Experiment details."""

    def as_dict(self) -> dict:
        """Serializes the GetExperimentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiment:
            body["experiment"] = self.experiment.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetExperimentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiment:
            body["experiment"] = self.experiment
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetExperimentResponse:
        """Deserializes the GetExperimentResponse from a dictionary."""
        return cls(experiment=_from_dict(d, "experiment", Experiment))


@dataclass
class GetLatestVersionsResponse:
    model_versions: Optional[List[ModelVersion]] = None
    """Latest version models for each requests stage. Only return models with current `READY` status.
    If no `stages` provided, returns the latest version for each stage, including `"None"`."""

    def as_dict(self) -> dict:
        """Serializes the GetLatestVersionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_versions:
            body["model_versions"] = [v.as_dict() for v in self.model_versions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetLatestVersionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_versions:
            body["model_versions"] = self.model_versions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetLatestVersionsResponse:
        """Deserializes the GetLatestVersionsResponse from a dictionary."""
        return cls(model_versions=_repeated_dict(d, "model_versions", ModelVersion))


@dataclass
class GetLoggedModelResponse:
    model: Optional[LoggedModel] = None
    """The retrieved logged model."""

    def as_dict(self) -> dict:
        """Serializes the GetLoggedModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model:
            body["model"] = self.model.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetLoggedModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model:
            body["model"] = self.model
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetLoggedModelResponse:
        """Deserializes the GetLoggedModelResponse from a dictionary."""
        return cls(model=_from_dict(d, "model", LoggedModel))


@dataclass
class GetMetricHistoryResponse:
    metrics: Optional[List[Metric]] = None
    """All logged values for this metric if `max_results` is not specified in the request or if the
    total count of metrics returned is less than the service level pagination threshold. Otherwise,
    this is one page of results."""

    next_page_token: Optional[str] = None
    """A token that can be used to issue a query for the next page of metric history values. A missing
    token indicates that no additional metrics are available to fetch."""

    def as_dict(self) -> dict:
        """Serializes the GetMetricHistoryResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metrics:
            body["metrics"] = [v.as_dict() for v in self.metrics]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetMetricHistoryResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metrics:
            body["metrics"] = self.metrics
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetMetricHistoryResponse:
        """Deserializes the GetMetricHistoryResponse from a dictionary."""
        return cls(metrics=_repeated_dict(d, "metrics", Metric), next_page_token=d.get("next_page_token", None))


@dataclass
class GetModelResponse:
    registered_model_databricks: Optional[ModelDatabricks] = None

    def as_dict(self) -> dict:
        """Serializes the GetModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.registered_model_databricks:
            body["registered_model_databricks"] = self.registered_model_databricks.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.registered_model_databricks:
            body["registered_model_databricks"] = self.registered_model_databricks
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetModelResponse:
        """Deserializes the GetModelResponse from a dictionary."""
        return cls(registered_model_databricks=_from_dict(d, "registered_model_databricks", ModelDatabricks))


@dataclass
class GetModelVersionDownloadUriResponse:
    artifact_uri: Optional[str] = None
    """URI corresponding to where artifacts for this model version are stored."""

    def as_dict(self) -> dict:
        """Serializes the GetModelVersionDownloadUriResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifact_uri is not None:
            body["artifact_uri"] = self.artifact_uri
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetModelVersionDownloadUriResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifact_uri is not None:
            body["artifact_uri"] = self.artifact_uri
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetModelVersionDownloadUriResponse:
        """Deserializes the GetModelVersionDownloadUriResponse from a dictionary."""
        return cls(artifact_uri=d.get("artifact_uri", None))


@dataclass
class GetModelVersionResponse:
    model_version: Optional[ModelVersion] = None

    def as_dict(self) -> dict:
        """Serializes the GetModelVersionResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_version:
            body["model_version"] = self.model_version.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetModelVersionResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_version:
            body["model_version"] = self.model_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetModelVersionResponse:
        """Deserializes the GetModelVersionResponse from a dictionary."""
        return cls(model_version=_from_dict(d, "model_version", ModelVersion))


@dataclass
class GetRegisteredModelPermissionLevelsResponse:
    permission_levels: Optional[List[RegisteredModelPermissionsDescription]] = None
    """Specific permission levels"""

    def as_dict(self) -> dict:
        """Serializes the GetRegisteredModelPermissionLevelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = [v.as_dict() for v in self.permission_levels]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetRegisteredModelPermissionLevelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_levels:
            body["permission_levels"] = self.permission_levels
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetRegisteredModelPermissionLevelsResponse:
        """Deserializes the GetRegisteredModelPermissionLevelsResponse from a dictionary."""
        return cls(permission_levels=_repeated_dict(d, "permission_levels", RegisteredModelPermissionsDescription))


@dataclass
class GetRunResponse:
    run: Optional[Run] = None
    """Run metadata (name, start time, etc) and data (metrics, params, and tags)."""

    def as_dict(self) -> dict:
        """Serializes the GetRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.run:
            body["run"] = self.run.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.run:
            body["run"] = self.run
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetRunResponse:
        """Deserializes the GetRunResponse from a dictionary."""
        return cls(run=_from_dict(d, "run", Run))


@dataclass
class HttpUrlSpec:
    url: str
    """External HTTPS URL called on event trigger (by using a POST request)."""

    authorization: Optional[str] = None
    """Value of the authorization header that should be sent in the request sent by the wehbook. It
    should be of the form `"<auth type> <credentials>"`. If set to an empty string, no authorization
    header will be included in the request."""

    enable_ssl_verification: Optional[bool] = None
    """Enable/disable SSL certificate validation. Default is true. For self-signed certificates, this
    field must be false AND the destination server must disable certificate validation as well. For
    security purposes, it is encouraged to perform secret validation with the HMAC-encoded portion
    of the payload and acknowledge the risk associated with disabling hostname validation whereby it
    becomes more likely that requests can be maliciously routed to an unintended host."""

    secret: Optional[str] = None
    """Shared secret required for HMAC encoding payload. The HMAC-encoded payload will be sent in the
    header as: { "X-Databricks-Signature": $encoded_payload }."""

    def as_dict(self) -> dict:
        """Serializes the HttpUrlSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.authorization is not None:
            body["authorization"] = self.authorization
        if self.enable_ssl_verification is not None:
            body["enable_ssl_verification"] = self.enable_ssl_verification
        if self.secret is not None:
            body["secret"] = self.secret
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the HttpUrlSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.authorization is not None:
            body["authorization"] = self.authorization
        if self.enable_ssl_verification is not None:
            body["enable_ssl_verification"] = self.enable_ssl_verification
        if self.secret is not None:
            body["secret"] = self.secret
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> HttpUrlSpec:
        """Deserializes the HttpUrlSpec from a dictionary."""
        return cls(
            authorization=d.get("authorization", None),
            enable_ssl_verification=d.get("enable_ssl_verification", None),
            secret=d.get("secret", None),
            url=d.get("url", None),
        )


@dataclass
class HttpUrlSpecWithoutSecret:
    enable_ssl_verification: Optional[bool] = None
    """Enable/disable SSL certificate validation. Default is true. For self-signed certificates, this
    field must be false AND the destination server must disable certificate validation as well. For
    security purposes, it is encouraged to perform secret validation with the HMAC-encoded portion
    of the payload and acknowledge the risk associated with disabling hostname validation whereby it
    becomes more likely that requests can be maliciously routed to an unintended host."""

    url: Optional[str] = None
    """External HTTPS URL called on event trigger (by using a POST request)."""

    def as_dict(self) -> dict:
        """Serializes the HttpUrlSpecWithoutSecret into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.enable_ssl_verification is not None:
            body["enable_ssl_verification"] = self.enable_ssl_verification
        if self.url is not None:
            body["url"] = self.url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the HttpUrlSpecWithoutSecret into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.enable_ssl_verification is not None:
            body["enable_ssl_verification"] = self.enable_ssl_verification
        if self.url is not None:
            body["url"] = self.url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> HttpUrlSpecWithoutSecret:
        """Deserializes the HttpUrlSpecWithoutSecret from a dictionary."""
        return cls(enable_ssl_verification=d.get("enable_ssl_verification", None), url=d.get("url", None))


@dataclass
class InputTag:
    """Tag for a dataset input."""

    key: str
    """The tag key."""

    value: str
    """The tag value."""

    def as_dict(self) -> dict:
        """Serializes the InputTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the InputTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> InputTag:
        """Deserializes the InputTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class JobContext:
    job_id: Optional[int] = None
    """The job ID where this API invoked."""

    job_run_id: Optional[int] = None
    """The job run ID where this API was invoked."""

    def as_dict(self) -> dict:
        """Serializes the JobContext into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobContext into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.job_run_id is not None:
            body["job_run_id"] = self.job_run_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobContext:
        """Deserializes the JobContext from a dictionary."""
        return cls(job_id=d.get("job_id", None), job_run_id=d.get("job_run_id", None))


@dataclass
class JobSpec:
    job_id: str
    """ID of the job that the webhook runs."""

    access_token: str
    """The personal access token used to authorize webhook's job runs."""

    workspace_url: Optional[str] = None
    """URL of the workspace containing the job that this webhook runs. If not specified, the job’s
    workspace URL is assumed to be the same as the workspace where the webhook is created."""

    def as_dict(self) -> dict:
        """Serializes the JobSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_token is not None:
            body["access_token"] = self.access_token
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.workspace_url is not None:
            body["workspace_url"] = self.workspace_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_token is not None:
            body["access_token"] = self.access_token
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.workspace_url is not None:
            body["workspace_url"] = self.workspace_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobSpec:
        """Deserializes the JobSpec from a dictionary."""
        return cls(
            access_token=d.get("access_token", None),
            job_id=d.get("job_id", None),
            workspace_url=d.get("workspace_url", None),
        )


@dataclass
class JobSpecWithoutSecret:
    job_id: Optional[str] = None
    """ID of the job that the webhook runs."""

    workspace_url: Optional[str] = None
    """URL of the workspace containing the job that this webhook runs. If not specified, the job’s
    workspace URL is assumed to be the same as the workspace where the webhook is created."""

    def as_dict(self) -> dict:
        """Serializes the JobSpecWithoutSecret into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.workspace_url is not None:
            body["workspace_url"] = self.workspace_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the JobSpecWithoutSecret into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_id is not None:
            body["job_id"] = self.job_id
        if self.workspace_url is not None:
            body["workspace_url"] = self.workspace_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JobSpecWithoutSecret:
        """Deserializes the JobSpecWithoutSecret from a dictionary."""
        return cls(job_id=d.get("job_id", None), workspace_url=d.get("workspace_url", None))


@dataclass
class KafkaConfig:
    name: str
    """Name that uniquely identifies this Kafka config within the metastore. This will be the
    identifier used from the Feature object to reference these configs for a feature. Can be
    distinct from topic name."""

    bootstrap_servers: str
    """A comma-separated list of host/port pairs pointing to Kafka cluster."""

    subscription_mode: SubscriptionMode
    """Options to configure which Kafka topics to pull data from."""

    auth_config: AuthConfig
    """Authentication configuration for connection to topics."""

    backfill_source: Optional[BackfillSource] = None
    """A user-provided and managed source for backfilling data. Historical data is used when creating a
    training set from streaming features linked to this Kafka config. In the future, a separate
    table will be maintained by Databricks for forward filling data. The schema for this source must
    match exactly that of the key and value schemas specified for this Kafka config."""

    extra_options: Optional[Dict[str, str]] = None
    """Catch-all for miscellaneous options. Keys should be source options or Kafka consumer options
    (kafka.*)"""

    key_schema: Optional[SchemaConfig] = None
    """Schema configuration for extracting message keys from topics. At least one of key_schema and
    value_schema must be provided."""

    value_schema: Optional[SchemaConfig] = None
    """Schema configuration for extracting message values from topics. At least one of key_schema and
    value_schema must be provided."""

    def as_dict(self) -> dict:
        """Serializes the KafkaConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.auth_config:
            body["auth_config"] = self.auth_config.as_dict()
        if self.backfill_source:
            body["backfill_source"] = self.backfill_source.as_dict()
        if self.bootstrap_servers is not None:
            body["bootstrap_servers"] = self.bootstrap_servers
        if self.extra_options:
            body["extra_options"] = self.extra_options
        if self.key_schema:
            body["key_schema"] = self.key_schema.as_dict()
        if self.name is not None:
            body["name"] = self.name
        if self.subscription_mode:
            body["subscription_mode"] = self.subscription_mode.as_dict()
        if self.value_schema:
            body["value_schema"] = self.value_schema.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the KafkaConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.auth_config:
            body["auth_config"] = self.auth_config
        if self.backfill_source:
            body["backfill_source"] = self.backfill_source
        if self.bootstrap_servers is not None:
            body["bootstrap_servers"] = self.bootstrap_servers
        if self.extra_options:
            body["extra_options"] = self.extra_options
        if self.key_schema:
            body["key_schema"] = self.key_schema
        if self.name is not None:
            body["name"] = self.name
        if self.subscription_mode:
            body["subscription_mode"] = self.subscription_mode
        if self.value_schema:
            body["value_schema"] = self.value_schema
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> KafkaConfig:
        """Deserializes the KafkaConfig from a dictionary."""
        return cls(
            auth_config=_from_dict(d, "auth_config", AuthConfig),
            backfill_source=_from_dict(d, "backfill_source", BackfillSource),
            bootstrap_servers=d.get("bootstrap_servers", None),
            extra_options=d.get("extra_options", None),
            key_schema=_from_dict(d, "key_schema", SchemaConfig),
            name=d.get("name", None),
            subscription_mode=_from_dict(d, "subscription_mode", SubscriptionMode),
            value_schema=_from_dict(d, "value_schema", SchemaConfig),
        )


@dataclass
class KafkaSource:
    name: str
    """Name of the Kafka source, used to identify it. This is used to look up the corresponding
    KafkaConfig object. Can be distinct from topic name."""

    entity_column_identifiers: List[ColumnIdentifier]
    """The entity column identifiers of the Kafka source."""

    timeseries_column_identifier: ColumnIdentifier
    """The timeseries column identifier of the Kafka source."""

    def as_dict(self) -> dict:
        """Serializes the KafkaSource into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entity_column_identifiers:
            body["entity_column_identifiers"] = [v.as_dict() for v in self.entity_column_identifiers]
        if self.name is not None:
            body["name"] = self.name
        if self.timeseries_column_identifier:
            body["timeseries_column_identifier"] = self.timeseries_column_identifier.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the KafkaSource into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entity_column_identifiers:
            body["entity_column_identifiers"] = self.entity_column_identifiers
        if self.name is not None:
            body["name"] = self.name
        if self.timeseries_column_identifier:
            body["timeseries_column_identifier"] = self.timeseries_column_identifier
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> KafkaSource:
        """Deserializes the KafkaSource from a dictionary."""
        return cls(
            entity_column_identifiers=_repeated_dict(d, "entity_column_identifiers", ColumnIdentifier),
            name=d.get("name", None),
            timeseries_column_identifier=_from_dict(d, "timeseries_column_identifier", ColumnIdentifier),
        )


@dataclass
class LineageContext:
    """Lineage context information for tracking where an API was invoked. This will allow us to track
    lineage, which currently uses caller entity information for use across the Lineage Client and
    Observability in Lumberjack."""

    job_context: Optional[JobContext] = None
    """Job context information including job ID and run ID."""

    notebook_id: Optional[int] = None
    """The notebook ID where this API was invoked."""

    def as_dict(self) -> dict:
        """Serializes the LineageContext into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.job_context:
            body["job_context"] = self.job_context.as_dict()
        if self.notebook_id is not None:
            body["notebook_id"] = self.notebook_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LineageContext into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.job_context:
            body["job_context"] = self.job_context
        if self.notebook_id is not None:
            body["notebook_id"] = self.notebook_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LineageContext:
        """Deserializes the LineageContext from a dictionary."""
        return cls(job_context=_from_dict(d, "job_context", JobContext), notebook_id=d.get("notebook_id", None))


@dataclass
class LinkedFeature:
    """Feature for model version. ([ML-57150] Renamed from Feature to LinkedFeature)"""

    feature_name: Optional[str] = None
    """Feature name"""

    feature_table_id: Optional[str] = None
    """Feature table id"""

    feature_table_name: Optional[str] = None
    """Feature table name"""

    def as_dict(self) -> dict:
        """Serializes the LinkedFeature into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.feature_name is not None:
            body["feature_name"] = self.feature_name
        if self.feature_table_id is not None:
            body["feature_table_id"] = self.feature_table_id
        if self.feature_table_name is not None:
            body["feature_table_name"] = self.feature_table_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LinkedFeature into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.feature_name is not None:
            body["feature_name"] = self.feature_name
        if self.feature_table_id is not None:
            body["feature_table_id"] = self.feature_table_id
        if self.feature_table_name is not None:
            body["feature_table_name"] = self.feature_table_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LinkedFeature:
        """Deserializes the LinkedFeature from a dictionary."""
        return cls(
            feature_name=d.get("feature_name", None),
            feature_table_id=d.get("feature_table_id", None),
            feature_table_name=d.get("feature_table_name", None),
        )


@dataclass
class ListArtifactsResponse:
    files: Optional[List[FileInfo]] = None
    """The file location and metadata for artifacts."""

    next_page_token: Optional[str] = None
    """The token that can be used to retrieve the next page of artifact results."""

    root_uri: Optional[str] = None
    """The root artifact directory for the run."""

    def as_dict(self) -> dict:
        """Serializes the ListArtifactsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.files:
            body["files"] = [v.as_dict() for v in self.files]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.root_uri is not None:
            body["root_uri"] = self.root_uri
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListArtifactsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.files:
            body["files"] = self.files
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.root_uri is not None:
            body["root_uri"] = self.root_uri
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListArtifactsResponse:
        """Deserializes the ListArtifactsResponse from a dictionary."""
        return cls(
            files=_repeated_dict(d, "files", FileInfo),
            next_page_token=d.get("next_page_token", None),
            root_uri=d.get("root_uri", None),
        )


@dataclass
class ListExperimentsResponse:
    experiments: Optional[List[Experiment]] = None
    """Paginated Experiments beginning with the first item on the requested page."""

    next_page_token: Optional[str] = None
    """Token that can be used to retrieve the next page of experiments. Empty token means no more
    experiment is available for retrieval."""

    def as_dict(self) -> dict:
        """Serializes the ListExperimentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiments:
            body["experiments"] = [v.as_dict() for v in self.experiments]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExperimentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiments:
            body["experiments"] = self.experiments
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExperimentsResponse:
        """Deserializes the ListExperimentsResponse from a dictionary."""
        return cls(
            experiments=_repeated_dict(d, "experiments", Experiment), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListFeatureTagsResponse:
    """Response message for ListFeatureTag."""

    feature_tags: Optional[List[FeatureTag]] = None

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of results for this query."""

    def as_dict(self) -> dict:
        """Serializes the ListFeatureTagsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.feature_tags:
            body["feature_tags"] = [v.as_dict() for v in self.feature_tags]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListFeatureTagsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.feature_tags:
            body["feature_tags"] = self.feature_tags
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListFeatureTagsResponse:
        """Deserializes the ListFeatureTagsResponse from a dictionary."""
        return cls(
            feature_tags=_repeated_dict(d, "feature_tags", FeatureTag), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListFeaturesResponse:
    features: Optional[List[Feature]] = None
    """List of features."""

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of results for this query."""

    def as_dict(self) -> dict:
        """Serializes the ListFeaturesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.features:
            body["features"] = [v.as_dict() for v in self.features]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListFeaturesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.features:
            body["features"] = self.features
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListFeaturesResponse:
        """Deserializes the ListFeaturesResponse from a dictionary."""
        return cls(features=_repeated_dict(d, "features", Feature), next_page_token=d.get("next_page_token", None))


@dataclass
class ListKafkaConfigsResponse:
    kafka_configs: List[KafkaConfig]
    """List of Kafka configs. Schemas are not included in the response."""

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of results for this query."""

    def as_dict(self) -> dict:
        """Serializes the ListKafkaConfigsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.kafka_configs:
            body["kafka_configs"] = [v.as_dict() for v in self.kafka_configs]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListKafkaConfigsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.kafka_configs:
            body["kafka_configs"] = self.kafka_configs
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListKafkaConfigsResponse:
        """Deserializes the ListKafkaConfigsResponse from a dictionary."""
        return cls(
            kafka_configs=_repeated_dict(d, "kafka_configs", KafkaConfig),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListMaterializedFeaturesResponse:
    materialized_features: Optional[List[MaterializedFeature]] = None
    """List of materialized features."""

    next_page_token: Optional[str] = None
    """Pagination token to request the next page of results for this query."""

    def as_dict(self) -> dict:
        """Serializes the ListMaterializedFeaturesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.materialized_features:
            body["materialized_features"] = [v.as_dict() for v in self.materialized_features]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListMaterializedFeaturesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.materialized_features:
            body["materialized_features"] = self.materialized_features
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListMaterializedFeaturesResponse:
        """Deserializes the ListMaterializedFeaturesResponse from a dictionary."""
        return cls(
            materialized_features=_repeated_dict(d, "materialized_features", MaterializedFeature),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListModelsResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request next page of models for the same query."""

    registered_models: Optional[List[Model]] = None

    def as_dict(self) -> dict:
        """Serializes the ListModelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.registered_models:
            body["registered_models"] = [v.as_dict() for v in self.registered_models]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListModelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.registered_models:
            body["registered_models"] = self.registered_models
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListModelsResponse:
        """Deserializes the ListModelsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            registered_models=_repeated_dict(d, "registered_models", Model),
        )


@dataclass
class ListOnlineStoresResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request the next page of results for this query."""

    online_stores: Optional[List[OnlineStore]] = None
    """List of online stores."""

    def as_dict(self) -> dict:
        """Serializes the ListOnlineStoresResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.online_stores:
            body["online_stores"] = [v.as_dict() for v in self.online_stores]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListOnlineStoresResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.online_stores:
            body["online_stores"] = self.online_stores
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListOnlineStoresResponse:
        """Deserializes the ListOnlineStoresResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            online_stores=_repeated_dict(d, "online_stores", OnlineStore),
        )


@dataclass
class ListRegistryWebhooks:
    next_page_token: Optional[str] = None
    """Token that can be used to retrieve the next page of artifact results"""

    webhooks: Optional[List[RegistryWebhook]] = None
    """Array of registry webhooks."""

    def as_dict(self) -> dict:
        """Serializes the ListRegistryWebhooks into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.webhooks:
            body["webhooks"] = [v.as_dict() for v in self.webhooks]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListRegistryWebhooks into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.webhooks:
            body["webhooks"] = self.webhooks
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListRegistryWebhooks:
        """Deserializes the ListRegistryWebhooks from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), webhooks=_repeated_dict(d, "webhooks", RegistryWebhook)
        )


@dataclass
class ListTransitionRequestsResponse:
    requests: Optional[List[Activity]] = None
    """Array of open transition requests."""

    def as_dict(self) -> dict:
        """Serializes the ListTransitionRequestsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.requests:
            body["requests"] = [v.as_dict() for v in self.requests]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListTransitionRequestsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.requests:
            body["requests"] = self.requests
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListTransitionRequestsResponse:
        """Deserializes the ListTransitionRequestsResponse from a dictionary."""
        return cls(requests=_repeated_dict(d, "requests", Activity))


@dataclass
class LogBatchResponse:
    def as_dict(self) -> dict:
        """Serializes the LogBatchResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogBatchResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogBatchResponse:
        """Deserializes the LogBatchResponse from a dictionary."""
        return cls()


@dataclass
class LogInputsResponse:
    def as_dict(self) -> dict:
        """Serializes the LogInputsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogInputsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogInputsResponse:
        """Deserializes the LogInputsResponse from a dictionary."""
        return cls()


@dataclass
class LogLoggedModelParamsRequestResponse:
    def as_dict(self) -> dict:
        """Serializes the LogLoggedModelParamsRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogLoggedModelParamsRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogLoggedModelParamsRequestResponse:
        """Deserializes the LogLoggedModelParamsRequestResponse from a dictionary."""
        return cls()


@dataclass
class LogMetricResponse:
    def as_dict(self) -> dict:
        """Serializes the LogMetricResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogMetricResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogMetricResponse:
        """Deserializes the LogMetricResponse from a dictionary."""
        return cls()


@dataclass
class LogModelResponse:
    def as_dict(self) -> dict:
        """Serializes the LogModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogModelResponse:
        """Deserializes the LogModelResponse from a dictionary."""
        return cls()


@dataclass
class LogOutputsResponse:
    def as_dict(self) -> dict:
        """Serializes the LogOutputsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogOutputsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogOutputsResponse:
        """Deserializes the LogOutputsResponse from a dictionary."""
        return cls()


@dataclass
class LogParamResponse:
    def as_dict(self) -> dict:
        """Serializes the LogParamResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LogParamResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogParamResponse:
        """Deserializes the LogParamResponse from a dictionary."""
        return cls()


@dataclass
class LoggedModel:
    """A logged model message includes logged model attributes, tags, registration info, params, and
    linked run metrics."""

    data: Optional[LoggedModelData] = None
    """The params and metrics attached to the logged model."""

    info: Optional[LoggedModelInfo] = None
    """The logged model attributes such as model ID, status, tags, etc."""

    def as_dict(self) -> dict:
        """Serializes the LoggedModel into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.data:
            body["data"] = self.data.as_dict()
        if self.info:
            body["info"] = self.info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LoggedModel into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.data:
            body["data"] = self.data
        if self.info:
            body["info"] = self.info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LoggedModel:
        """Deserializes the LoggedModel from a dictionary."""
        return cls(data=_from_dict(d, "data", LoggedModelData), info=_from_dict(d, "info", LoggedModelInfo))


@dataclass
class LoggedModelData:
    """A LoggedModelData message includes logged model params and linked metrics."""

    metrics: Optional[List[Metric]] = None
    """Performance metrics linked to the model."""

    params: Optional[List[LoggedModelParameter]] = None
    """Immutable string key-value pairs of the model."""

    def as_dict(self) -> dict:
        """Serializes the LoggedModelData into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metrics:
            body["metrics"] = [v.as_dict() for v in self.metrics]
        if self.params:
            body["params"] = [v.as_dict() for v in self.params]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LoggedModelData into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metrics:
            body["metrics"] = self.metrics
        if self.params:
            body["params"] = self.params
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LoggedModelData:
        """Deserializes the LoggedModelData from a dictionary."""
        return cls(
            metrics=_repeated_dict(d, "metrics", Metric), params=_repeated_dict(d, "params", LoggedModelParameter)
        )


@dataclass
class LoggedModelInfo:
    """A LoggedModelInfo includes logged model attributes, tags, and registration info."""

    artifact_uri: Optional[str] = None
    """The URI of the directory where model artifacts are stored."""

    creation_timestamp_ms: Optional[int] = None
    """The timestamp when the model was created in milliseconds since the UNIX epoch."""

    creator_id: Optional[int] = None
    """The ID of the user or principal that created the model."""

    experiment_id: Optional[str] = None
    """The ID of the experiment that owns the model."""

    last_updated_timestamp_ms: Optional[int] = None
    """The timestamp when the model was last updated in milliseconds since the UNIX epoch."""

    model_id: Optional[str] = None
    """The unique identifier for the logged model."""

    model_type: Optional[str] = None
    """The type of model, such as ``"Agent"``, ``"Classifier"``, ``"LLM"``."""

    name: Optional[str] = None
    """The name of the model."""

    source_run_id: Optional[str] = None
    """The ID of the run that created the model."""

    status: Optional[LoggedModelStatus] = None
    """The status of whether or not the model is ready for use."""

    status_message: Optional[str] = None
    """Details on the current model status."""

    tags: Optional[List[LoggedModelTag]] = None
    """Mutable string key-value pairs set on the model."""

    def as_dict(self) -> dict:
        """Serializes the LoggedModelInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifact_uri is not None:
            body["artifact_uri"] = self.artifact_uri
        if self.creation_timestamp_ms is not None:
            body["creation_timestamp_ms"] = self.creation_timestamp_ms
        if self.creator_id is not None:
            body["creator_id"] = self.creator_id
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.last_updated_timestamp_ms is not None:
            body["last_updated_timestamp_ms"] = self.last_updated_timestamp_ms
        if self.model_id is not None:
            body["model_id"] = self.model_id
        if self.model_type is not None:
            body["model_type"] = self.model_type
        if self.name is not None:
            body["name"] = self.name
        if self.source_run_id is not None:
            body["source_run_id"] = self.source_run_id
        if self.status is not None:
            body["status"] = self.status.value
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LoggedModelInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifact_uri is not None:
            body["artifact_uri"] = self.artifact_uri
        if self.creation_timestamp_ms is not None:
            body["creation_timestamp_ms"] = self.creation_timestamp_ms
        if self.creator_id is not None:
            body["creator_id"] = self.creator_id
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.last_updated_timestamp_ms is not None:
            body["last_updated_timestamp_ms"] = self.last_updated_timestamp_ms
        if self.model_id is not None:
            body["model_id"] = self.model_id
        if self.model_type is not None:
            body["model_type"] = self.model_type
        if self.name is not None:
            body["name"] = self.name
        if self.source_run_id is not None:
            body["source_run_id"] = self.source_run_id
        if self.status is not None:
            body["status"] = self.status
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LoggedModelInfo:
        """Deserializes the LoggedModelInfo from a dictionary."""
        return cls(
            artifact_uri=d.get("artifact_uri", None),
            creation_timestamp_ms=d.get("creation_timestamp_ms", None),
            creator_id=d.get("creator_id", None),
            experiment_id=d.get("experiment_id", None),
            last_updated_timestamp_ms=d.get("last_updated_timestamp_ms", None),
            model_id=d.get("model_id", None),
            model_type=d.get("model_type", None),
            name=d.get("name", None),
            source_run_id=d.get("source_run_id", None),
            status=_enum(d, "status", LoggedModelStatus),
            status_message=d.get("status_message", None),
            tags=_repeated_dict(d, "tags", LoggedModelTag),
        )


@dataclass
class LoggedModelParameter:
    """Parameter associated with a LoggedModel."""

    key: Optional[str] = None
    """The key identifying this param."""

    value: Optional[str] = None
    """The value of this param."""

    def as_dict(self) -> dict:
        """Serializes the LoggedModelParameter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LoggedModelParameter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LoggedModelParameter:
        """Deserializes the LoggedModelParameter from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


class LoggedModelStatus(Enum):
    """A LoggedModelStatus enum value represents the status of a logged model."""

    LOGGED_MODEL_PENDING = "LOGGED_MODEL_PENDING"
    LOGGED_MODEL_READY = "LOGGED_MODEL_READY"
    LOGGED_MODEL_UPLOAD_FAILED = "LOGGED_MODEL_UPLOAD_FAILED"


@dataclass
class LoggedModelTag:
    """Tag for a LoggedModel."""

    key: Optional[str] = None
    """The tag key."""

    value: Optional[str] = None
    """The tag value."""

    def as_dict(self) -> dict:
        """Serializes the LoggedModelTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the LoggedModelTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LoggedModelTag:
        """Deserializes the LoggedModelTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class MaterializedFeature:
    """A materialized feature represents a feature that is continuously computed and stored."""

    feature_name: str
    """The full name of the feature in Unity Catalog."""

    cron_schedule: Optional[str] = None
    """The quartz cron expression that defines the schedule of the materialization pipeline. The
    schedule is evaluated in the UTC timezone."""

    last_materialization_time: Optional[str] = None
    """The timestamp when the pipeline last ran and updated the materialized feature values. If the
    pipeline has not run yet, this field will be null."""

    materialized_feature_id: Optional[str] = None
    """Unique identifier for the materialized feature."""

    offline_store_config: Optional[OfflineStoreConfig] = None

    online_store_config: Optional[OnlineStoreConfig] = None

    pipeline_schedule_state: Optional[MaterializedFeaturePipelineScheduleState] = None
    """The schedule state of the materialization pipeline."""

    table_name: Optional[str] = None
    """The fully qualified Unity Catalog path to the table containing the materialized feature (Delta
    table or Lakebase table). Output only."""

    def as_dict(self) -> dict:
        """Serializes the MaterializedFeature into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cron_schedule is not None:
            body["cron_schedule"] = self.cron_schedule
        if self.feature_name is not None:
            body["feature_name"] = self.feature_name
        if self.last_materialization_time is not None:
            body["last_materialization_time"] = self.last_materialization_time
        if self.materialized_feature_id is not None:
            body["materialized_feature_id"] = self.materialized_feature_id
        if self.offline_store_config:
            body["offline_store_config"] = self.offline_store_config.as_dict()
        if self.online_store_config:
            body["online_store_config"] = self.online_store_config.as_dict()
        if self.pipeline_schedule_state is not None:
            body["pipeline_schedule_state"] = self.pipeline_schedule_state.value
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MaterializedFeature into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cron_schedule is not None:
            body["cron_schedule"] = self.cron_schedule
        if self.feature_name is not None:
            body["feature_name"] = self.feature_name
        if self.last_materialization_time is not None:
            body["last_materialization_time"] = self.last_materialization_time
        if self.materialized_feature_id is not None:
            body["materialized_feature_id"] = self.materialized_feature_id
        if self.offline_store_config:
            body["offline_store_config"] = self.offline_store_config
        if self.online_store_config:
            body["online_store_config"] = self.online_store_config
        if self.pipeline_schedule_state is not None:
            body["pipeline_schedule_state"] = self.pipeline_schedule_state
        if self.table_name is not None:
            body["table_name"] = self.table_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MaterializedFeature:
        """Deserializes the MaterializedFeature from a dictionary."""
        return cls(
            cron_schedule=d.get("cron_schedule", None),
            feature_name=d.get("feature_name", None),
            last_materialization_time=d.get("last_materialization_time", None),
            materialized_feature_id=d.get("materialized_feature_id", None),
            offline_store_config=_from_dict(d, "offline_store_config", OfflineStoreConfig),
            online_store_config=_from_dict(d, "online_store_config", OnlineStoreConfig),
            pipeline_schedule_state=_enum(d, "pipeline_schedule_state", MaterializedFeaturePipelineScheduleState),
            table_name=d.get("table_name", None),
        )


class MaterializedFeaturePipelineScheduleState(Enum):

    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    SNAPSHOT = "SNAPSHOT"


@dataclass
class Metric:
    """Metric associated with a run, represented as a key-value pair."""

    dataset_digest: Optional[str] = None
    """The dataset digest of the dataset associated with the metric, e.g. an md5 hash of the dataset
    that uniquely identifies it within datasets of the same name."""

    dataset_name: Optional[str] = None
    """The name of the dataset associated with the metric. E.g. “my.uc.table@2”
    “nyc-taxi-dataset”, “fantastic-elk-3”"""

    key: Optional[str] = None
    """The key identifying the metric."""

    model_id: Optional[str] = None
    """The ID of the logged model or registered model version associated with the metric, if
    applicable."""

    run_id: Optional[str] = None
    """The ID of the run containing the metric."""

    step: Optional[int] = None
    """The step at which the metric was logged."""

    timestamp: Optional[int] = None
    """The timestamp at which the metric was recorded."""

    value: Optional[float] = None
    """The value of the metric."""

    def as_dict(self) -> dict:
        """Serializes the Metric into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dataset_digest is not None:
            body["dataset_digest"] = self.dataset_digest
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        if self.key is not None:
            body["key"] = self.key
        if self.model_id is not None:
            body["model_id"] = self.model_id
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.step is not None:
            body["step"] = self.step
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Metric into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dataset_digest is not None:
            body["dataset_digest"] = self.dataset_digest
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        if self.key is not None:
            body["key"] = self.key
        if self.model_id is not None:
            body["model_id"] = self.model_id
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.step is not None:
            body["step"] = self.step
        if self.timestamp is not None:
            body["timestamp"] = self.timestamp
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Metric:
        """Deserializes the Metric from a dictionary."""
        return cls(
            dataset_digest=d.get("dataset_digest", None),
            dataset_name=d.get("dataset_name", None),
            key=d.get("key", None),
            model_id=d.get("model_id", None),
            run_id=d.get("run_id", None),
            step=d.get("step", None),
            timestamp=d.get("timestamp", None),
            value=d.get("value", None),
        )


@dataclass
class Model:
    creation_timestamp: Optional[int] = None
    """Timestamp recorded when this `registered_model` was created."""

    description: Optional[str] = None
    """Description of this `registered_model`."""

    last_updated_timestamp: Optional[int] = None
    """Timestamp recorded when metadata for this `registered_model` was last updated."""

    latest_versions: Optional[List[ModelVersion]] = None
    """Collection of latest model versions for each stage. Only contains models with current `READY`
    status."""

    name: Optional[str] = None
    """Unique name for the model."""

    tags: Optional[List[ModelTag]] = None
    """Tags: Additional metadata key-value pairs for this `registered_model`."""

    user_id: Optional[str] = None
    """User that created this `registered_model`"""

    def as_dict(self) -> dict:
        """Serializes the Model into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.description is not None:
            body["description"] = self.description
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.latest_versions:
            body["latest_versions"] = [v.as_dict() for v in self.latest_versions]
        if self.name is not None:
            body["name"] = self.name
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Model into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.description is not None:
            body["description"] = self.description
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.latest_versions:
            body["latest_versions"] = self.latest_versions
        if self.name is not None:
            body["name"] = self.name
        if self.tags:
            body["tags"] = self.tags
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Model:
        """Deserializes the Model from a dictionary."""
        return cls(
            creation_timestamp=d.get("creation_timestamp", None),
            description=d.get("description", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            latest_versions=_repeated_dict(d, "latest_versions", ModelVersion),
            name=d.get("name", None),
            tags=_repeated_dict(d, "tags", ModelTag),
            user_id=d.get("user_id", None),
        )


@dataclass
class ModelDatabricks:
    creation_timestamp: Optional[int] = None
    """Creation time of the object, as a Unix timestamp in milliseconds."""

    description: Optional[str] = None
    """User-specified description for the object."""

    id: Optional[str] = None
    """Unique identifier for the object."""

    last_updated_timestamp: Optional[int] = None
    """Last update time of the object, as a Unix timestamp in milliseconds."""

    latest_versions: Optional[List[ModelVersion]] = None
    """Array of model versions, each the latest version for its stage."""

    name: Optional[str] = None
    """Name of the model."""

    permission_level: Optional[PermissionLevel] = None
    """Permission level granted for the requesting user on this registered model"""

    tags: Optional[List[ModelTag]] = None
    """Array of tags associated with the model."""

    user_id: Optional[str] = None
    """The username of the user that created the object."""

    def as_dict(self) -> dict:
        """Serializes the ModelDatabricks into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.latest_versions:
            body["latest_versions"] = [v.as_dict() for v in self.latest_versions]
        if self.name is not None:
            body["name"] = self.name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelDatabricks into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.latest_versions:
            body["latest_versions"] = self.latest_versions
        if self.name is not None:
            body["name"] = self.name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.tags:
            body["tags"] = self.tags
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelDatabricks:
        """Deserializes the ModelDatabricks from a dictionary."""
        return cls(
            creation_timestamp=d.get("creation_timestamp", None),
            description=d.get("description", None),
            id=d.get("id", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            latest_versions=_repeated_dict(d, "latest_versions", ModelVersion),
            name=d.get("name", None),
            permission_level=_enum(d, "permission_level", PermissionLevel),
            tags=_repeated_dict(d, "tags", ModelTag),
            user_id=d.get("user_id", None),
        )


@dataclass
class ModelInput:
    """Represents a LoggedModel or Registered Model Version input to a Run."""

    model_id: str
    """The unique identifier of the model."""

    def as_dict(self) -> dict:
        """Serializes the ModelInput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_id is not None:
            body["model_id"] = self.model_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelInput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_id is not None:
            body["model_id"] = self.model_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelInput:
        """Deserializes the ModelInput from a dictionary."""
        return cls(model_id=d.get("model_id", None))


@dataclass
class ModelOutput:
    """Represents a LoggedModel output of a Run."""

    model_id: str
    """The unique identifier of the model."""

    step: int
    """The step at which the model was produced."""

    def as_dict(self) -> dict:
        """Serializes the ModelOutput into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_id is not None:
            body["model_id"] = self.model_id
        if self.step is not None:
            body["step"] = self.step
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelOutput into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_id is not None:
            body["model_id"] = self.model_id
        if self.step is not None:
            body["step"] = self.step
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelOutput:
        """Deserializes the ModelOutput from a dictionary."""
        return cls(model_id=d.get("model_id", None), step=d.get("step", None))


@dataclass
class ModelTag:
    """Tag for a registered model"""

    key: Optional[str] = None
    """The tag key."""

    value: Optional[str] = None
    """The tag value."""

    def as_dict(self) -> dict:
        """Serializes the ModelTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelTag:
        """Deserializes the ModelTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class ModelVersion:
    creation_timestamp: Optional[int] = None
    """Timestamp recorded when this `model_version` was created."""

    current_stage: Optional[str] = None
    """Current stage for this `model_version`."""

    description: Optional[str] = None
    """Description of this `model_version`."""

    last_updated_timestamp: Optional[int] = None
    """Timestamp recorded when metadata for this `model_version` was last updated."""

    name: Optional[str] = None
    """Unique name of the model"""

    run_id: Optional[str] = None
    """MLflow run ID used when creating `model_version`, if `source` was generated by an experiment run
    stored in MLflow tracking server."""

    run_link: Optional[str] = None
    """Run Link: Direct link to the run that generated this version"""

    source: Optional[str] = None
    """URI indicating the location of the source model artifacts, used when creating `model_version`"""

    status: Optional[ModelVersionStatus] = None
    """Current status of `model_version`"""

    status_message: Optional[str] = None
    """Details on current `status`, if it is pending or failed."""

    tags: Optional[List[ModelVersionTag]] = None
    """Tags: Additional metadata key-value pairs for this `model_version`."""

    user_id: Optional[str] = None
    """User that created this `model_version`."""

    version: Optional[str] = None
    """Model's version number."""

    def as_dict(self) -> dict:
        """Serializes the ModelVersion into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.current_stage is not None:
            body["current_stage"] = self.current_stage
        if self.description is not None:
            body["description"] = self.description
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.name is not None:
            body["name"] = self.name
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_link is not None:
            body["run_link"] = self.run_link
        if self.source is not None:
            body["source"] = self.source
        if self.status is not None:
            body["status"] = self.status.value
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelVersion into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.current_stage is not None:
            body["current_stage"] = self.current_stage
        if self.description is not None:
            body["description"] = self.description
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.name is not None:
            body["name"] = self.name
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_link is not None:
            body["run_link"] = self.run_link
        if self.source is not None:
            body["source"] = self.source
        if self.status is not None:
            body["status"] = self.status
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.tags:
            body["tags"] = self.tags
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelVersion:
        """Deserializes the ModelVersion from a dictionary."""
        return cls(
            creation_timestamp=d.get("creation_timestamp", None),
            current_stage=d.get("current_stage", None),
            description=d.get("description", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            name=d.get("name", None),
            run_id=d.get("run_id", None),
            run_link=d.get("run_link", None),
            source=d.get("source", None),
            status=_enum(d, "status", ModelVersionStatus),
            status_message=d.get("status_message", None),
            tags=_repeated_dict(d, "tags", ModelVersionTag),
            user_id=d.get("user_id", None),
            version=d.get("version", None),
        )


@dataclass
class ModelVersionDatabricks:
    creation_timestamp: Optional[int] = None
    """Creation time of the object, as a Unix timestamp in milliseconds."""

    current_stage: Optional[str] = None

    description: Optional[str] = None
    """User-specified description for the object."""

    email_subscription_status: Optional[RegistryEmailSubscriptionType] = None
    """Email Subscription Status: This is the subscription status of the user to the model version
    Users get subscribed by interacting with the model version."""

    feature_list: Optional[FeatureList] = None
    """Feature lineage of `model_version`."""

    last_updated_timestamp: Optional[int] = None
    """Time of the object at last update, as a Unix timestamp in milliseconds."""

    name: Optional[str] = None
    """Name of the model."""

    open_requests: Optional[List[Activity]] = None
    """Open requests for this `model_versions`. Gap in sequence number is intentional and is done in
    order to match field sequence numbers of `ModelVersion` proto message"""

    permission_level: Optional[PermissionLevel] = None

    run_id: Optional[str] = None
    """Unique identifier for the MLflow tracking run associated with the source model artifacts."""

    run_link: Optional[str] = None
    """URL of the run associated with the model artifacts. This field is set at model version creation
    time only for model versions whose source run is from a tracking server that is different from
    the registry server."""

    source: Optional[str] = None
    """URI that indicates the location of the source model artifacts. This is used when creating the
    model version."""

    status: Optional[Status] = None

    status_message: Optional[str] = None
    """Details on the current status, for example why registration failed."""

    tags: Optional[List[ModelVersionTag]] = None
    """Array of tags that are associated with the model version."""

    user_id: Optional[str] = None
    """The username of the user that created the object."""

    version: Optional[str] = None
    """Version of the model."""

    def as_dict(self) -> dict:
        """Serializes the ModelVersionDatabricks into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.current_stage is not None:
            body["current_stage"] = self.current_stage
        if self.description is not None:
            body["description"] = self.description
        if self.email_subscription_status is not None:
            body["email_subscription_status"] = self.email_subscription_status.value
        if self.feature_list:
            body["feature_list"] = self.feature_list.as_dict()
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.name is not None:
            body["name"] = self.name
        if self.open_requests:
            body["open_requests"] = [v.as_dict() for v in self.open_requests]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_link is not None:
            body["run_link"] = self.run_link
        if self.source is not None:
            body["source"] = self.source
        if self.status is not None:
            body["status"] = self.status.value
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelVersionDatabricks into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.current_stage is not None:
            body["current_stage"] = self.current_stage
        if self.description is not None:
            body["description"] = self.description
        if self.email_subscription_status is not None:
            body["email_subscription_status"] = self.email_subscription_status
        if self.feature_list:
            body["feature_list"] = self.feature_list
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.name is not None:
            body["name"] = self.name
        if self.open_requests:
            body["open_requests"] = self.open_requests
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_link is not None:
            body["run_link"] = self.run_link
        if self.source is not None:
            body["source"] = self.source
        if self.status is not None:
            body["status"] = self.status
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.tags:
            body["tags"] = self.tags
        if self.user_id is not None:
            body["user_id"] = self.user_id
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelVersionDatabricks:
        """Deserializes the ModelVersionDatabricks from a dictionary."""
        return cls(
            creation_timestamp=d.get("creation_timestamp", None),
            current_stage=d.get("current_stage", None),
            description=d.get("description", None),
            email_subscription_status=_enum(d, "email_subscription_status", RegistryEmailSubscriptionType),
            feature_list=_from_dict(d, "feature_list", FeatureList),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            name=d.get("name", None),
            open_requests=_repeated_dict(d, "open_requests", Activity),
            permission_level=_enum(d, "permission_level", PermissionLevel),
            run_id=d.get("run_id", None),
            run_link=d.get("run_link", None),
            source=d.get("source", None),
            status=_enum(d, "status", Status),
            status_message=d.get("status_message", None),
            tags=_repeated_dict(d, "tags", ModelVersionTag),
            user_id=d.get("user_id", None),
            version=d.get("version", None),
        )


class ModelVersionStatus(Enum):
    """The status of the model version. Valid values are: * `PENDING_REGISTRATION`: Request to register
    a new model version is pending as server performs background tasks.

    * `FAILED_REGISTRATION`: Request to register a new model version has failed.

    * `READY`: Model version is ready for use."""

    FAILED_REGISTRATION = "FAILED_REGISTRATION"
    PENDING_REGISTRATION = "PENDING_REGISTRATION"
    READY = "READY"


@dataclass
class ModelVersionTag:
    key: Optional[str] = None
    """The tag key."""

    value: Optional[str] = None
    """The tag value."""

    def as_dict(self) -> dict:
        """Serializes the ModelVersionTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ModelVersionTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ModelVersionTag:
        """Deserializes the ModelVersionTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class OfflineStoreConfig:
    """Configuration for offline store destination."""

    catalog_name: str
    """The Unity Catalog catalog name."""

    schema_name: str
    """The Unity Catalog schema name."""

    table_name_prefix: str
    """Prefix for Unity Catalog table name. The materialized feature will be stored in a table with
    this prefix and a generated postfix."""

    def as_dict(self) -> dict:
        """Serializes the OfflineStoreConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.table_name_prefix is not None:
            body["table_name_prefix"] = self.table_name_prefix
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OfflineStoreConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.table_name_prefix is not None:
            body["table_name_prefix"] = self.table_name_prefix
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OfflineStoreConfig:
        """Deserializes the OfflineStoreConfig from a dictionary."""
        return cls(
            catalog_name=d.get("catalog_name", None),
            schema_name=d.get("schema_name", None),
            table_name_prefix=d.get("table_name_prefix", None),
        )


@dataclass
class OnlineStore:
    """An OnlineStore is a logical database instance that stores and serves features online."""

    name: str
    """The name of the online store. This is the unique identifier for the online store."""

    capacity: str
    """The capacity of the online store. Valid values are "CU_1", "CU_2", "CU_4", "CU_8"."""

    creation_time: Optional[str] = None
    """The timestamp when the online store was created."""

    creator: Optional[str] = None
    """The email of the creator of the online store."""

    read_replica_count: Optional[int] = None
    """The number of read replicas for the online store. Defaults to 0."""

    state: Optional[OnlineStoreState] = None
    """The current state of the online store."""

    usage_policy_id: Optional[str] = None
    """The usage policy applied to the online store to track billing."""

    def as_dict(self) -> dict:
        """Serializes the OnlineStore into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.capacity is not None:
            body["capacity"] = self.capacity
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.name is not None:
            body["name"] = self.name
        if self.read_replica_count is not None:
            body["read_replica_count"] = self.read_replica_count
        if self.state is not None:
            body["state"] = self.state.value
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineStore into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.capacity is not None:
            body["capacity"] = self.capacity
        if self.creation_time is not None:
            body["creation_time"] = self.creation_time
        if self.creator is not None:
            body["creator"] = self.creator
        if self.name is not None:
            body["name"] = self.name
        if self.read_replica_count is not None:
            body["read_replica_count"] = self.read_replica_count
        if self.state is not None:
            body["state"] = self.state
        if self.usage_policy_id is not None:
            body["usage_policy_id"] = self.usage_policy_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineStore:
        """Deserializes the OnlineStore from a dictionary."""
        return cls(
            capacity=d.get("capacity", None),
            creation_time=d.get("creation_time", None),
            creator=d.get("creator", None),
            name=d.get("name", None),
            read_replica_count=d.get("read_replica_count", None),
            state=_enum(d, "state", OnlineStoreState),
            usage_policy_id=d.get("usage_policy_id", None),
        )


@dataclass
class OnlineStoreConfig:
    """Configuration for online store destination."""

    catalog_name: str
    """The Unity Catalog catalog name. This name is also used as the Lakebase logical database name.
    Quoting is handled by the backend where needed, do not pre-quote it."""

    schema_name: str
    """The Unity Catalog schema name. This name is also used as the Lakebase schema name under the
    database. Quoting is handled by the backend where needed, do not pre-quote it."""

    table_name_prefix: str
    """Prefix for Unity Catalog table name. The materialized feature will be stored in a Lakebase table
    with this prefix and a generated postfix."""

    online_store_name: str
    """The name of the target online store."""

    def as_dict(self) -> dict:
        """Serializes the OnlineStoreConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.online_store_name is not None:
            body["online_store_name"] = self.online_store_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.table_name_prefix is not None:
            body["table_name_prefix"] = self.table_name_prefix
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the OnlineStoreConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.online_store_name is not None:
            body["online_store_name"] = self.online_store_name
        if self.schema_name is not None:
            body["schema_name"] = self.schema_name
        if self.table_name_prefix is not None:
            body["table_name_prefix"] = self.table_name_prefix
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> OnlineStoreConfig:
        """Deserializes the OnlineStoreConfig from a dictionary."""
        return cls(
            catalog_name=d.get("catalog_name", None),
            online_store_name=d.get("online_store_name", None),
            schema_name=d.get("schema_name", None),
            table_name_prefix=d.get("table_name_prefix", None),
        )


class OnlineStoreState(Enum):

    AVAILABLE = "AVAILABLE"
    DELETING = "DELETING"
    FAILING_OVER = "FAILING_OVER"
    STARTING = "STARTING"
    STOPPED = "STOPPED"
    UPDATING = "UPDATING"


@dataclass
class Param:
    """Param associated with a run."""

    key: Optional[str] = None
    """Key identifying this param."""

    value: Optional[str] = None
    """Value associated with this param."""

    def as_dict(self) -> dict:
        """Serializes the Param into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Param into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Param:
        """Deserializes the Param from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


class PermissionLevel(Enum):
    """Permission level of the requesting user on the object. For what is allowed at each level, see
    [MLflow Model permissions](..)."""

    CAN_CREATE_REGISTERED_MODEL = "CAN_CREATE_REGISTERED_MODEL"
    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_PRODUCTION_VERSIONS = "CAN_MANAGE_PRODUCTION_VERSIONS"
    CAN_MANAGE_STAGING_VERSIONS = "CAN_MANAGE_STAGING_VERSIONS"
    CAN_READ = "CAN_READ"


@dataclass
class PublishSpec:
    online_store: str
    """The name of the target online store."""

    online_table_name: str
    """The full three-part (catalog, schema, table) name of the online table."""

    publish_mode: PublishSpecPublishMode
    """The publish mode of the pipeline that syncs the online table with the source table."""

    def as_dict(self) -> dict:
        """Serializes the PublishSpec into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.online_store is not None:
            body["online_store"] = self.online_store
        if self.online_table_name is not None:
            body["online_table_name"] = self.online_table_name
        if self.publish_mode is not None:
            body["publish_mode"] = self.publish_mode.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PublishSpec into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.online_store is not None:
            body["online_store"] = self.online_store
        if self.online_table_name is not None:
            body["online_table_name"] = self.online_table_name
        if self.publish_mode is not None:
            body["publish_mode"] = self.publish_mode
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PublishSpec:
        """Deserializes the PublishSpec from a dictionary."""
        return cls(
            online_store=d.get("online_store", None),
            online_table_name=d.get("online_table_name", None),
            publish_mode=_enum(d, "publish_mode", PublishSpecPublishMode),
        )


class PublishSpecPublishMode(Enum):

    CONTINUOUS = "CONTINUOUS"
    SNAPSHOT = "SNAPSHOT"
    TRIGGERED = "TRIGGERED"


@dataclass
class PublishTableResponse:
    online_table_name: Optional[str] = None
    """The full three-part (catalog, schema, table) name of the online table."""

    pipeline_id: Optional[str] = None
    """The ID of the pipeline that syncs the online table with the source table."""

    def as_dict(self) -> dict:
        """Serializes the PublishTableResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.online_table_name is not None:
            body["online_table_name"] = self.online_table_name
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PublishTableResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.online_table_name is not None:
            body["online_table_name"] = self.online_table_name
        if self.pipeline_id is not None:
            body["pipeline_id"] = self.pipeline_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PublishTableResponse:
        """Deserializes the PublishTableResponse from a dictionary."""
        return cls(online_table_name=d.get("online_table_name", None), pipeline_id=d.get("pipeline_id", None))


@dataclass
class RegisteredModelAccessControlRequest:
    group_name: Optional[str] = None
    """name of the group"""

    permission_level: Optional[RegisteredModelPermissionLevel] = None

    service_principal_name: Optional[str] = None
    """application ID of a service principal"""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelAccessControlRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelAccessControlRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelAccessControlRequest:
        """Deserializes the RegisteredModelAccessControlRequest from a dictionary."""
        return cls(
            group_name=d.get("group_name", None),
            permission_level=_enum(d, "permission_level", RegisteredModelPermissionLevel),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class RegisteredModelAccessControlResponse:
    all_permissions: Optional[List[RegisteredModelPermission]] = None
    """All permissions."""

    display_name: Optional[str] = None
    """Display name of the user or service principal."""

    group_name: Optional[str] = None
    """name of the group"""

    service_principal_name: Optional[str] = None
    """Name of the service principal."""

    user_name: Optional[str] = None
    """name of the user"""

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelAccessControlResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = [v.as_dict() for v in self.all_permissions]
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelAccessControlResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.all_permissions:
            body["all_permissions"] = self.all_permissions
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.group_name is not None:
            body["group_name"] = self.group_name
        if self.service_principal_name is not None:
            body["service_principal_name"] = self.service_principal_name
        if self.user_name is not None:
            body["user_name"] = self.user_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelAccessControlResponse:
        """Deserializes the RegisteredModelAccessControlResponse from a dictionary."""
        return cls(
            all_permissions=_repeated_dict(d, "all_permissions", RegisteredModelPermission),
            display_name=d.get("display_name", None),
            group_name=d.get("group_name", None),
            service_principal_name=d.get("service_principal_name", None),
            user_name=d.get("user_name", None),
        )


@dataclass
class RegisteredModelPermission:
    inherited: Optional[bool] = None

    inherited_from_object: Optional[List[str]] = None

    permission_level: Optional[RegisteredModelPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelPermission into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = [v for v in self.inherited_from_object]
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelPermission into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.inherited is not None:
            body["inherited"] = self.inherited
        if self.inherited_from_object:
            body["inherited_from_object"] = self.inherited_from_object
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelPermission:
        """Deserializes the RegisteredModelPermission from a dictionary."""
        return cls(
            inherited=d.get("inherited", None),
            inherited_from_object=d.get("inherited_from_object", None),
            permission_level=_enum(d, "permission_level", RegisteredModelPermissionLevel),
        )


class RegisteredModelPermissionLevel(Enum):
    """Permission level"""

    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_PRODUCTION_VERSIONS = "CAN_MANAGE_PRODUCTION_VERSIONS"
    CAN_MANAGE_STAGING_VERSIONS = "CAN_MANAGE_STAGING_VERSIONS"
    CAN_READ = "CAN_READ"


@dataclass
class RegisteredModelPermissions:
    access_control_list: Optional[List[RegisteredModelAccessControlResponse]] = None

    object_id: Optional[str] = None

    object_type: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelPermissions into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [v.as_dict() for v in self.access_control_list]
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelPermissions into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.object_id is not None:
            body["object_id"] = self.object_id
        if self.object_type is not None:
            body["object_type"] = self.object_type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelPermissions:
        """Deserializes the RegisteredModelPermissions from a dictionary."""
        return cls(
            access_control_list=_repeated_dict(d, "access_control_list", RegisteredModelAccessControlResponse),
            object_id=d.get("object_id", None),
            object_type=d.get("object_type", None),
        )


@dataclass
class RegisteredModelPermissionsDescription:
    description: Optional[str] = None

    permission_level: Optional[RegisteredModelPermissionLevel] = None

    def as_dict(self) -> dict:
        """Serializes the RegisteredModelPermissionsDescription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegisteredModelPermissionsDescription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.permission_level is not None:
            body["permission_level"] = self.permission_level
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisteredModelPermissionsDescription:
        """Deserializes the RegisteredModelPermissionsDescription from a dictionary."""
        return cls(
            description=d.get("description", None),
            permission_level=_enum(d, "permission_level", RegisteredModelPermissionLevel),
        )


class RegistryEmailSubscriptionType(Enum):
    """.. note:: Experimental: This entity may change or be removed in a future release without
    warning. Email subscription types for registry notifications: - `ALL_EVENTS`: Subscribed to all
    events. - `DEFAULT`: Default subscription type. - `SUBSCRIBED`: Subscribed to notifications. -
    `UNSUBSCRIBED`: Not subscribed to notifications."""

    ALL_EVENTS = "ALL_EVENTS"
    DEFAULT = "DEFAULT"
    SUBSCRIBED = "SUBSCRIBED"
    UNSUBSCRIBED = "UNSUBSCRIBED"


@dataclass
class RegistryWebhook:
    creation_timestamp: Optional[int] = None
    """Creation time of the object, as a Unix timestamp in milliseconds."""

    description: Optional[str] = None
    """User-specified description for the webhook."""

    events: Optional[List[RegistryWebhookEvent]] = None
    """Events that can trigger a registry webhook: * `MODEL_VERSION_CREATED`: A new model version was
    created for the associated model.
    
    * `MODEL_VERSION_TRANSITIONED_STAGE`: A model version’s stage was changed.
    
    * `TRANSITION_REQUEST_CREATED`: A user requested a model version’s stage be transitioned.
    
    * `COMMENT_CREATED`: A user wrote a comment on a registered model.
    
    * `REGISTERED_MODEL_CREATED`: A new registered model was created. This event type can only be
    specified for a registry-wide webhook, which can be created by not specifying a model name in
    the create request.
    
    * `MODEL_VERSION_TAG_SET`: A user set a tag on the model version.
    
    * `MODEL_VERSION_TRANSITIONED_TO_STAGING`: A model version was transitioned to staging.
    
    * `MODEL_VERSION_TRANSITIONED_TO_PRODUCTION`: A model version was transitioned to production.
    
    * `MODEL_VERSION_TRANSITIONED_TO_ARCHIVED`: A model version was archived.
    
    * `TRANSITION_REQUEST_TO_STAGING_CREATED`: A user requested a model version be transitioned to
    staging.
    
    * `TRANSITION_REQUEST_TO_PRODUCTION_CREATED`: A user requested a model version be transitioned
    to production.
    
    * `TRANSITION_REQUEST_TO_ARCHIVED_CREATED`: A user requested a model version be archived."""

    http_url_spec: Optional[HttpUrlSpecWithoutSecret] = None

    id: Optional[str] = None
    """Webhook ID"""

    job_spec: Optional[JobSpecWithoutSecret] = None

    last_updated_timestamp: Optional[int] = None
    """Time of the object at last update, as a Unix timestamp in milliseconds."""

    model_name: Optional[str] = None
    """Name of the model whose events would trigger this webhook."""

    status: Optional[RegistryWebhookStatus] = None

    def as_dict(self) -> dict:
        """Serializes the RegistryWebhook into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.description is not None:
            body["description"] = self.description
        if self.events:
            body["events"] = [v.value for v in self.events]
        if self.http_url_spec:
            body["http_url_spec"] = self.http_url_spec.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.job_spec:
            body["job_spec"] = self.job_spec.as_dict()
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.status is not None:
            body["status"] = self.status.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegistryWebhook into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.description is not None:
            body["description"] = self.description
        if self.events:
            body["events"] = self.events
        if self.http_url_spec:
            body["http_url_spec"] = self.http_url_spec
        if self.id is not None:
            body["id"] = self.id
        if self.job_spec:
            body["job_spec"] = self.job_spec
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.model_name is not None:
            body["model_name"] = self.model_name
        if self.status is not None:
            body["status"] = self.status
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegistryWebhook:
        """Deserializes the RegistryWebhook from a dictionary."""
        return cls(
            creation_timestamp=d.get("creation_timestamp", None),
            description=d.get("description", None),
            events=_repeated_enum(d, "events", RegistryWebhookEvent),
            http_url_spec=_from_dict(d, "http_url_spec", HttpUrlSpecWithoutSecret),
            id=d.get("id", None),
            job_spec=_from_dict(d, "job_spec", JobSpecWithoutSecret),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            model_name=d.get("model_name", None),
            status=_enum(d, "status", RegistryWebhookStatus),
        )


class RegistryWebhookEvent(Enum):

    COMMENT_CREATED = "COMMENT_CREATED"
    MODEL_VERSION_CREATED = "MODEL_VERSION_CREATED"
    MODEL_VERSION_TAG_SET = "MODEL_VERSION_TAG_SET"
    MODEL_VERSION_TRANSITIONED_STAGE = "MODEL_VERSION_TRANSITIONED_STAGE"
    MODEL_VERSION_TRANSITIONED_TO_ARCHIVED = "MODEL_VERSION_TRANSITIONED_TO_ARCHIVED"
    MODEL_VERSION_TRANSITIONED_TO_PRODUCTION = "MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"
    MODEL_VERSION_TRANSITIONED_TO_STAGING = "MODEL_VERSION_TRANSITIONED_TO_STAGING"
    REGISTERED_MODEL_CREATED = "REGISTERED_MODEL_CREATED"
    TRANSITION_REQUEST_CREATED = "TRANSITION_REQUEST_CREATED"
    TRANSITION_REQUEST_TO_ARCHIVED_CREATED = "TRANSITION_REQUEST_TO_ARCHIVED_CREATED"
    TRANSITION_REQUEST_TO_PRODUCTION_CREATED = "TRANSITION_REQUEST_TO_PRODUCTION_CREATED"
    TRANSITION_REQUEST_TO_STAGING_CREATED = "TRANSITION_REQUEST_TO_STAGING_CREATED"


class RegistryWebhookStatus(Enum):
    """Enable or disable triggering the webhook, or put the webhook into test mode. The default is
    `ACTIVE`: * `ACTIVE`: Webhook is triggered when an associated event happens.

    * `DISABLED`: Webhook is not triggered.

    * `TEST_MODE`: Webhook can be triggered through the test endpoint, but is not triggered on a
    real event."""

    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"
    TEST_MODE = "TEST_MODE"


@dataclass
class RejectTransitionRequestResponse:
    activity: Optional[Activity] = None
    """New activity generated as a result of this operation."""

    def as_dict(self) -> dict:
        """Serializes the RejectTransitionRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activity:
            body["activity"] = self.activity.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RejectTransitionRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activity:
            body["activity"] = self.activity
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RejectTransitionRequestResponse:
        """Deserializes the RejectTransitionRequestResponse from a dictionary."""
        return cls(activity=_from_dict(d, "activity", Activity))


@dataclass
class RenameModelResponse:
    registered_model: Optional[Model] = None

    def as_dict(self) -> dict:
        """Serializes the RenameModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.registered_model:
            body["registered_model"] = self.registered_model.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RenameModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.registered_model:
            body["registered_model"] = self.registered_model
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RenameModelResponse:
        """Deserializes the RenameModelResponse from a dictionary."""
        return cls(registered_model=_from_dict(d, "registered_model", Model))


@dataclass
class RestoreExperimentResponse:
    def as_dict(self) -> dict:
        """Serializes the RestoreExperimentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestoreExperimentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestoreExperimentResponse:
        """Deserializes the RestoreExperimentResponse from a dictionary."""
        return cls()


@dataclass
class RestoreRunResponse:
    def as_dict(self) -> dict:
        """Serializes the RestoreRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestoreRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestoreRunResponse:
        """Deserializes the RestoreRunResponse from a dictionary."""
        return cls()


@dataclass
class RestoreRunsResponse:
    runs_restored: Optional[int] = None
    """The number of runs restored."""

    def as_dict(self) -> dict:
        """Serializes the RestoreRunsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.runs_restored is not None:
            body["runs_restored"] = self.runs_restored
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RestoreRunsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.runs_restored is not None:
            body["runs_restored"] = self.runs_restored
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RestoreRunsResponse:
        """Deserializes the RestoreRunsResponse from a dictionary."""
        return cls(runs_restored=d.get("runs_restored", None))


@dataclass
class Run:
    """A single run."""

    data: Optional[RunData] = None
    """Run data."""

    info: Optional[RunInfo] = None
    """Run metadata."""

    inputs: Optional[RunInputs] = None
    """Run inputs."""

    def as_dict(self) -> dict:
        """Serializes the Run into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.data:
            body["data"] = self.data.as_dict()
        if self.info:
            body["info"] = self.info.as_dict()
        if self.inputs:
            body["inputs"] = self.inputs.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Run into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.data:
            body["data"] = self.data
        if self.info:
            body["info"] = self.info
        if self.inputs:
            body["inputs"] = self.inputs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Run:
        """Deserializes the Run from a dictionary."""
        return cls(
            data=_from_dict(d, "data", RunData),
            info=_from_dict(d, "info", RunInfo),
            inputs=_from_dict(d, "inputs", RunInputs),
        )


@dataclass
class RunData:
    """Run data (metrics, params, and tags)."""

    metrics: Optional[List[Metric]] = None
    """Run metrics."""

    params: Optional[List[Param]] = None
    """Run parameters."""

    tags: Optional[List[RunTag]] = None
    """Additional metadata key-value pairs."""

    def as_dict(self) -> dict:
        """Serializes the RunData into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.metrics:
            body["metrics"] = [v.as_dict() for v in self.metrics]
        if self.params:
            body["params"] = [v.as_dict() for v in self.params]
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunData into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.metrics:
            body["metrics"] = self.metrics
        if self.params:
            body["params"] = self.params
        if self.tags:
            body["tags"] = self.tags
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunData:
        """Deserializes the RunData from a dictionary."""
        return cls(
            metrics=_repeated_dict(d, "metrics", Metric),
            params=_repeated_dict(d, "params", Param),
            tags=_repeated_dict(d, "tags", RunTag),
        )


@dataclass
class RunInfo:
    """Metadata of a single run."""

    artifact_uri: Optional[str] = None
    """URI of the directory where artifacts should be uploaded. This can be a local path (starting with
    "/"), or a distributed file system (DFS) path, like ``s3://bucket/directory`` or
    ``dbfs:/my/directory``. If not set, the local ``./mlruns`` directory is chosen."""

    end_time: Optional[int] = None
    """Unix timestamp of when the run ended in milliseconds."""

    experiment_id: Optional[str] = None
    """The experiment ID."""

    lifecycle_stage: Optional[str] = None
    """Current life cycle stage of the experiment : OneOf("active", "deleted")"""

    run_id: Optional[str] = None
    """Unique identifier for the run."""

    run_name: Optional[str] = None
    """The name of the run."""

    run_uuid: Optional[str] = None
    """[Deprecated, use run_id instead] Unique identifier for the run. This field will be removed in a
    future MLflow version."""

    start_time: Optional[int] = None
    """Unix timestamp of when the run started in milliseconds."""

    status: Optional[RunInfoStatus] = None
    """Current status of the run."""

    user_id: Optional[str] = None
    """User who initiated the run. This field is deprecated as of MLflow 1.0, and will be removed in a
    future MLflow release. Use 'mlflow.user' tag instead."""

    def as_dict(self) -> dict:
        """Serializes the RunInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.artifact_uri is not None:
            body["artifact_uri"] = self.artifact_uri
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.lifecycle_stage is not None:
            body["lifecycle_stage"] = self.lifecycle_stage
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_name is not None:
            body["run_name"] = self.run_name
        if self.run_uuid is not None:
            body["run_uuid"] = self.run_uuid
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.status is not None:
            body["status"] = self.status.value
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.artifact_uri is not None:
            body["artifact_uri"] = self.artifact_uri
        if self.end_time is not None:
            body["end_time"] = self.end_time
        if self.experiment_id is not None:
            body["experiment_id"] = self.experiment_id
        if self.lifecycle_stage is not None:
            body["lifecycle_stage"] = self.lifecycle_stage
        if self.run_id is not None:
            body["run_id"] = self.run_id
        if self.run_name is not None:
            body["run_name"] = self.run_name
        if self.run_uuid is not None:
            body["run_uuid"] = self.run_uuid
        if self.start_time is not None:
            body["start_time"] = self.start_time
        if self.status is not None:
            body["status"] = self.status
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunInfo:
        """Deserializes the RunInfo from a dictionary."""
        return cls(
            artifact_uri=d.get("artifact_uri", None),
            end_time=d.get("end_time", None),
            experiment_id=d.get("experiment_id", None),
            lifecycle_stage=d.get("lifecycle_stage", None),
            run_id=d.get("run_id", None),
            run_name=d.get("run_name", None),
            run_uuid=d.get("run_uuid", None),
            start_time=d.get("start_time", None),
            status=_enum(d, "status", RunInfoStatus),
            user_id=d.get("user_id", None),
        )


class RunInfoStatus(Enum):
    """Status of a run."""

    FAILED = "FAILED"
    FINISHED = "FINISHED"
    KILLED = "KILLED"
    RUNNING = "RUNNING"
    SCHEDULED = "SCHEDULED"


@dataclass
class RunInputs:
    """Run inputs."""

    dataset_inputs: Optional[List[DatasetInput]] = None
    """Run metrics."""

    model_inputs: Optional[List[ModelInput]] = None
    """Model inputs to the Run."""

    def as_dict(self) -> dict:
        """Serializes the RunInputs into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dataset_inputs:
            body["dataset_inputs"] = [v.as_dict() for v in self.dataset_inputs]
        if self.model_inputs:
            body["model_inputs"] = [v.as_dict() for v in self.model_inputs]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunInputs into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dataset_inputs:
            body["dataset_inputs"] = self.dataset_inputs
        if self.model_inputs:
            body["model_inputs"] = self.model_inputs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunInputs:
        """Deserializes the RunInputs from a dictionary."""
        return cls(
            dataset_inputs=_repeated_dict(d, "dataset_inputs", DatasetInput),
            model_inputs=_repeated_dict(d, "model_inputs", ModelInput),
        )


@dataclass
class RunTag:
    """Tag for a run."""

    key: Optional[str] = None
    """The tag key."""

    value: Optional[str] = None
    """The tag value."""

    def as_dict(self) -> dict:
        """Serializes the RunTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RunTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.key is not None:
            body["key"] = self.key
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RunTag:
        """Deserializes the RunTag from a dictionary."""
        return cls(key=d.get("key", None), value=d.get("value", None))


@dataclass
class SchemaConfig:
    json_schema: Optional[str] = None
    """Schema of the JSON object in standard IETF JSON schema format (https://json-schema.org/)"""

    def as_dict(self) -> dict:
        """Serializes the SchemaConfig into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.json_schema is not None:
            body["json_schema"] = self.json_schema
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SchemaConfig into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.json_schema is not None:
            body["json_schema"] = self.json_schema
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SchemaConfig:
        """Deserializes the SchemaConfig from a dictionary."""
        return cls(json_schema=d.get("json_schema", None))


@dataclass
class SearchExperimentsResponse:
    experiments: Optional[List[Experiment]] = None
    """Experiments that match the search criteria"""

    next_page_token: Optional[str] = None
    """Token that can be used to retrieve the next page of experiments. An empty token means that no
    more experiments are available for retrieval."""

    def as_dict(self) -> dict:
        """Serializes the SearchExperimentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.experiments:
            body["experiments"] = [v.as_dict() for v in self.experiments]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchExperimentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.experiments:
            body["experiments"] = self.experiments
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchExperimentsResponse:
        """Deserializes the SearchExperimentsResponse from a dictionary."""
        return cls(
            experiments=_repeated_dict(d, "experiments", Experiment), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class SearchLoggedModelsDataset:
    dataset_name: str
    """The name of the dataset."""

    dataset_digest: Optional[str] = None
    """The digest of the dataset."""

    def as_dict(self) -> dict:
        """Serializes the SearchLoggedModelsDataset into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dataset_digest is not None:
            body["dataset_digest"] = self.dataset_digest
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchLoggedModelsDataset into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dataset_digest is not None:
            body["dataset_digest"] = self.dataset_digest
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchLoggedModelsDataset:
        """Deserializes the SearchLoggedModelsDataset from a dictionary."""
        return cls(dataset_digest=d.get("dataset_digest", None), dataset_name=d.get("dataset_name", None))


@dataclass
class SearchLoggedModelsOrderBy:
    field_name: str
    """The name of the field to order by, e.g. "metrics.accuracy"."""

    ascending: Optional[bool] = None
    """Whether the search results order is ascending or not."""

    dataset_digest: Optional[str] = None
    """If ``field_name`` refers to a metric, this field specifies the digest of the dataset associated
    with the metric. Only metrics associated with the specified dataset name and digest will be
    considered for ordering. This field may only be set if ``dataset_name`` is also set."""

    dataset_name: Optional[str] = None
    """If ``field_name`` refers to a metric, this field specifies the name of the dataset associated
    with the metric. Only metrics associated with the specified dataset name will be considered for
    ordering. This field may only be set if ``field_name`` refers to a metric."""

    def as_dict(self) -> dict:
        """Serializes the SearchLoggedModelsOrderBy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.ascending is not None:
            body["ascending"] = self.ascending
        if self.dataset_digest is not None:
            body["dataset_digest"] = self.dataset_digest
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        if self.field_name is not None:
            body["field_name"] = self.field_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchLoggedModelsOrderBy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.ascending is not None:
            body["ascending"] = self.ascending
        if self.dataset_digest is not None:
            body["dataset_digest"] = self.dataset_digest
        if self.dataset_name is not None:
            body["dataset_name"] = self.dataset_name
        if self.field_name is not None:
            body["field_name"] = self.field_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchLoggedModelsOrderBy:
        """Deserializes the SearchLoggedModelsOrderBy from a dictionary."""
        return cls(
            ascending=d.get("ascending", None),
            dataset_digest=d.get("dataset_digest", None),
            dataset_name=d.get("dataset_name", None),
            field_name=d.get("field_name", None),
        )


@dataclass
class SearchLoggedModelsResponse:
    models: Optional[List[LoggedModel]] = None
    """Logged models that match the search criteria."""

    next_page_token: Optional[str] = None
    """The token that can be used to retrieve the next page of logged models."""

    def as_dict(self) -> dict:
        """Serializes the SearchLoggedModelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.models:
            body["models"] = [v.as_dict() for v in self.models]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchLoggedModelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.models:
            body["models"] = self.models
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchLoggedModelsResponse:
        """Deserializes the SearchLoggedModelsResponse from a dictionary."""
        return cls(models=_repeated_dict(d, "models", LoggedModel), next_page_token=d.get("next_page_token", None))


@dataclass
class SearchModelVersionsResponse:
    model_versions: Optional[List[ModelVersion]] = None
    """Models that match the search criteria"""

    next_page_token: Optional[str] = None
    """Pagination token to request next page of models for the same search query."""

    def as_dict(self) -> dict:
        """Serializes the SearchModelVersionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_versions:
            body["model_versions"] = [v.as_dict() for v in self.model_versions]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchModelVersionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_versions:
            body["model_versions"] = self.model_versions
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchModelVersionsResponse:
        """Deserializes the SearchModelVersionsResponse from a dictionary."""
        return cls(
            model_versions=_repeated_dict(d, "model_versions", ModelVersion),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class SearchModelsResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request the next page of models."""

    registered_models: Optional[List[Model]] = None
    """Registered Models that match the search criteria."""

    def as_dict(self) -> dict:
        """Serializes the SearchModelsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.registered_models:
            body["registered_models"] = [v.as_dict() for v in self.registered_models]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchModelsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.registered_models:
            body["registered_models"] = self.registered_models
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchModelsResponse:
        """Deserializes the SearchModelsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            registered_models=_repeated_dict(d, "registered_models", Model),
        )


@dataclass
class SearchRunsResponse:
    next_page_token: Optional[str] = None
    """Token for the next page of runs."""

    runs: Optional[List[Run]] = None
    """Runs that match the search criteria."""

    def as_dict(self) -> dict:
        """Serializes the SearchRunsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.runs:
            body["runs"] = [v.as_dict() for v in self.runs]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchRunsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.runs:
            body["runs"] = self.runs
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchRunsResponse:
        """Deserializes the SearchRunsResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), runs=_repeated_dict(d, "runs", Run))


@dataclass
class SetExperimentTagResponse:
    def as_dict(self) -> dict:
        """Serializes the SetExperimentTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetExperimentTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetExperimentTagResponse:
        """Deserializes the SetExperimentTagResponse from a dictionary."""
        return cls()


@dataclass
class SetLoggedModelTagsResponse:
    def as_dict(self) -> dict:
        """Serializes the SetLoggedModelTagsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetLoggedModelTagsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetLoggedModelTagsResponse:
        """Deserializes the SetLoggedModelTagsResponse from a dictionary."""
        return cls()


@dataclass
class SetModelTagResponse:
    def as_dict(self) -> dict:
        """Serializes the SetModelTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetModelTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetModelTagResponse:
        """Deserializes the SetModelTagResponse from a dictionary."""
        return cls()


@dataclass
class SetModelVersionTagResponse:
    def as_dict(self) -> dict:
        """Serializes the SetModelVersionTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetModelVersionTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetModelVersionTagResponse:
        """Deserializes the SetModelVersionTagResponse from a dictionary."""
        return cls()


@dataclass
class SetTagResponse:
    def as_dict(self) -> dict:
        """Serializes the SetTagResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SetTagResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SetTagResponse:
        """Deserializes the SetTagResponse from a dictionary."""
        return cls()


@dataclass
class SlidingWindow:
    window_duration: str
    """The duration of the sliding window."""

    slide_duration: str
    """The slide duration (interval by which windows advance, must be positive and less than duration)."""

    def as_dict(self) -> dict:
        """Serializes the SlidingWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.slide_duration is not None:
            body["slide_duration"] = self.slide_duration
        if self.window_duration is not None:
            body["window_duration"] = self.window_duration
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SlidingWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.slide_duration is not None:
            body["slide_duration"] = self.slide_duration
        if self.window_duration is not None:
            body["window_duration"] = self.window_duration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SlidingWindow:
        """Deserializes the SlidingWindow from a dictionary."""
        return cls(slide_duration=d.get("slide_duration", None), window_duration=d.get("window_duration", None))


class Status(Enum):
    """The status of the model version. Valid values are: * `PENDING_REGISTRATION`: Request to register
    a new model version is pending as server performs background tasks.

    * `FAILED_REGISTRATION`: Request to register a new model version has failed.

    * `READY`: Model version is ready for use."""

    FAILED_REGISTRATION = "FAILED_REGISTRATION"
    PENDING_REGISTRATION = "PENDING_REGISTRATION"
    READY = "READY"


@dataclass
class SubscriptionMode:
    assign: Optional[str] = None
    """A JSON string that contains the specific topic-partitions to consume from. For example, for
    '{"topicA":[0,1],"topicB":[2,4]}', topicA's 0'th and 1st partitions will be consumed from."""

    subscribe: Optional[str] = None
    """A comma-separated list of Kafka topics to read from. For example, 'topicA,topicB,topicC'."""

    subscribe_pattern: Optional[str] = None
    """A regular expression matching topics to subscribe to. For example, 'topic.*' will subscribe to
    all topics starting with 'topic'."""

    def as_dict(self) -> dict:
        """Serializes the SubscriptionMode into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.assign is not None:
            body["assign"] = self.assign
        if self.subscribe is not None:
            body["subscribe"] = self.subscribe
        if self.subscribe_pattern is not None:
            body["subscribe_pattern"] = self.subscribe_pattern
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SubscriptionMode into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.assign is not None:
            body["assign"] = self.assign
        if self.subscribe is not None:
            body["subscribe"] = self.subscribe
        if self.subscribe_pattern is not None:
            body["subscribe_pattern"] = self.subscribe_pattern
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SubscriptionMode:
        """Deserializes the SubscriptionMode from a dictionary."""
        return cls(
            assign=d.get("assign", None),
            subscribe=d.get("subscribe", None),
            subscribe_pattern=d.get("subscribe_pattern", None),
        )


@dataclass
class TestRegistryWebhookResponse:
    body: Optional[str] = None
    """Body of the response from the webhook URL"""

    status_code: Optional[int] = None
    """Status code returned by the webhook URL"""

    def as_dict(self) -> dict:
        """Serializes the TestRegistryWebhookResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.body is not None:
            body["body"] = self.body
        if self.status_code is not None:
            body["status_code"] = self.status_code
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TestRegistryWebhookResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.body is not None:
            body["body"] = self.body
        if self.status_code is not None:
            body["status_code"] = self.status_code
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TestRegistryWebhookResponse:
        """Deserializes the TestRegistryWebhookResponse from a dictionary."""
        return cls(body=d.get("body", None), status_code=d.get("status_code", None))


@dataclass
class TimeWindow:
    continuous: Optional[ContinuousWindow] = None

    sliding: Optional[SlidingWindow] = None

    tumbling: Optional[TumblingWindow] = None

    def as_dict(self) -> dict:
        """Serializes the TimeWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.continuous:
            body["continuous"] = self.continuous.as_dict()
        if self.sliding:
            body["sliding"] = self.sliding.as_dict()
        if self.tumbling:
            body["tumbling"] = self.tumbling.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TimeWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.continuous:
            body["continuous"] = self.continuous
        if self.sliding:
            body["sliding"] = self.sliding
        if self.tumbling:
            body["tumbling"] = self.tumbling
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TimeWindow:
        """Deserializes the TimeWindow from a dictionary."""
        return cls(
            continuous=_from_dict(d, "continuous", ContinuousWindow),
            sliding=_from_dict(d, "sliding", SlidingWindow),
            tumbling=_from_dict(d, "tumbling", TumblingWindow),
        )


@dataclass
class TransitionRequest:
    """For activities, this contains the activity recorded for the action. For comments, this contains
    the comment details. For transition requests, this contains the transition request details."""

    available_actions: Optional[List[ActivityAction]] = None
    """Array of actions on the activity allowed for the current viewer."""

    comment: Optional[str] = None
    """User-provided comment associated with the activity, comment, or transition request."""

    creation_timestamp: Optional[int] = None
    """Creation time of the object, as a Unix timestamp in milliseconds."""

    to_stage: Optional[str] = None
    """Target stage of the transition (if the activity is stage transition related). Valid values are:
    
    * `None`: The initial stage of a model version.
    
    * `Staging`: Staging or pre-production stage.
    
    * `Production`: Production stage.
    
    * `Archived`: Archived stage."""

    user_id: Optional[str] = None
    """The username of the user that created the object."""

    def as_dict(self) -> dict:
        """Serializes the TransitionRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.available_actions:
            body["available_actions"] = [v.value for v in self.available_actions]
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.to_stage is not None:
            body["to_stage"] = self.to_stage
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TransitionRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.available_actions:
            body["available_actions"] = self.available_actions
        if self.comment is not None:
            body["comment"] = self.comment
        if self.creation_timestamp is not None:
            body["creation_timestamp"] = self.creation_timestamp
        if self.to_stage is not None:
            body["to_stage"] = self.to_stage
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TransitionRequest:
        """Deserializes the TransitionRequest from a dictionary."""
        return cls(
            available_actions=_repeated_enum(d, "available_actions", ActivityAction),
            comment=d.get("comment", None),
            creation_timestamp=d.get("creation_timestamp", None),
            to_stage=d.get("to_stage", None),
            user_id=d.get("user_id", None),
        )


@dataclass
class TransitionStageResponse:
    model_version_databricks: Optional[ModelVersionDatabricks] = None
    """Updated model version"""

    def as_dict(self) -> dict:
        """Serializes the TransitionStageResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_version_databricks:
            body["model_version_databricks"] = self.model_version_databricks.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TransitionStageResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_version_databricks:
            body["model_version_databricks"] = self.model_version_databricks
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TransitionStageResponse:
        """Deserializes the TransitionStageResponse from a dictionary."""
        return cls(model_version_databricks=_from_dict(d, "model_version_databricks", ModelVersionDatabricks))


@dataclass
class TumblingWindow:
    window_duration: str
    """The duration of each tumbling window (non-overlapping, fixed-duration windows)."""

    def as_dict(self) -> dict:
        """Serializes the TumblingWindow into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.window_duration is not None:
            body["window_duration"] = self.window_duration
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TumblingWindow into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.window_duration is not None:
            body["window_duration"] = self.window_duration
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TumblingWindow:
        """Deserializes the TumblingWindow from a dictionary."""
        return cls(window_duration=d.get("window_duration", None))


@dataclass
class UpdateCommentResponse:
    comment: Optional[CommentObject] = None
    """Updated comment object"""

    def as_dict(self) -> dict:
        """Serializes the UpdateCommentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment:
            body["comment"] = self.comment.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateCommentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment:
            body["comment"] = self.comment
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateCommentResponse:
        """Deserializes the UpdateCommentResponse from a dictionary."""
        return cls(comment=_from_dict(d, "comment", CommentObject))


@dataclass
class UpdateExperimentResponse:
    def as_dict(self) -> dict:
        """Serializes the UpdateExperimentResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateExperimentResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateExperimentResponse:
        """Deserializes the UpdateExperimentResponse from a dictionary."""
        return cls()


@dataclass
class UpdateModelResponse:
    registered_model: Optional[Model] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateModelResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.registered_model:
            body["registered_model"] = self.registered_model.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateModelResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.registered_model:
            body["registered_model"] = self.registered_model
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateModelResponse:
        """Deserializes the UpdateModelResponse from a dictionary."""
        return cls(registered_model=_from_dict(d, "registered_model", Model))


@dataclass
class UpdateModelVersionResponse:
    model_version: Optional[ModelVersion] = None
    """Return new version number generated for this model in registry."""

    def as_dict(self) -> dict:
        """Serializes the UpdateModelVersionResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.model_version:
            body["model_version"] = self.model_version.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateModelVersionResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.model_version:
            body["model_version"] = self.model_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateModelVersionResponse:
        """Deserializes the UpdateModelVersionResponse from a dictionary."""
        return cls(model_version=_from_dict(d, "model_version", ModelVersion))


@dataclass
class UpdateRunResponse:
    run_info: Optional[RunInfo] = None
    """Updated metadata of the run."""

    def as_dict(self) -> dict:
        """Serializes the UpdateRunResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.run_info:
            body["run_info"] = self.run_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateRunResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.run_info:
            body["run_info"] = self.run_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateRunResponse:
        """Deserializes the UpdateRunResponse from a dictionary."""
        return cls(run_info=_from_dict(d, "run_info", RunInfo))


class UpdateRunStatus(Enum):
    """Status of a run."""

    FAILED = "FAILED"
    FINISHED = "FINISHED"
    KILLED = "KILLED"
    RUNNING = "RUNNING"
    SCHEDULED = "SCHEDULED"


@dataclass
class UpdateWebhookResponse:
    webhook: Optional[RegistryWebhook] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateWebhookResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.webhook:
            body["webhook"] = self.webhook.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateWebhookResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.webhook:
            body["webhook"] = self.webhook
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateWebhookResponse:
        """Deserializes the UpdateWebhookResponse from a dictionary."""
        return cls(webhook=_from_dict(d, "webhook", RegistryWebhook))


class ViewType(Enum):
    """Qualifier for the view type."""

    ACTIVE_ONLY = "ACTIVE_ONLY"
    ALL = "ALL"
    DELETED_ONLY = "DELETED_ONLY"


class ExperimentsAPI:
    """Experiments are the primary unit of organization in MLflow; all MLflow runs belong to an experiment. Each
    experiment lets you visualize, search, and compare runs, as well as download run artifacts or metadata for
    analysis in other tools. Experiments are maintained in a Databricks hosted MLflow tracking server.

    Experiments are located in the workspace file tree. You manage experiments using the same tools you use to
    manage other workspace objects such as folders, notebooks, and libraries."""

    def __init__(self, api_client):
        self._api = api_client

    def create_experiment(
        self, name: str, *, artifact_location: Optional[str] = None, tags: Optional[List[ExperimentTag]] = None
    ) -> CreateExperimentResponse:
        """Creates an experiment with a name. Returns the ID of the newly created experiment. Validates that
        another experiment with the same name does not already exist and fails if another experiment with the
        same name already exists.

        Throws `RESOURCE_ALREADY_EXISTS` if an experiment with the given name exists.

        :param name: str
          Experiment name.
        :param artifact_location: str (optional)
          Location where all artifacts for the experiment are stored. If not provided, the remote server will
          select an appropriate default.
        :param tags: List[:class:`ExperimentTag`] (optional)
          A collection of tags to set on the experiment. Maximum tag size and number of tags per request
          depends on the storage backend. All storage backends are guaranteed to support tag keys up to 250
          bytes in size and tag values up to 5000 bytes in size. All storage backends are also guaranteed to
          support up to 20 tags per request.

        :returns: :class:`CreateExperimentResponse`
        """

        body = {}
        if artifact_location is not None:
            body["artifact_location"] = artifact_location
        if name is not None:
            body["name"] = name
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/experiments/create", body=body, headers=headers)
        return CreateExperimentResponse.from_dict(res)

    def create_logged_model(
        self,
        experiment_id: str,
        *,
        model_type: Optional[str] = None,
        name: Optional[str] = None,
        params: Optional[List[LoggedModelParameter]] = None,
        source_run_id: Optional[str] = None,
        tags: Optional[List[LoggedModelTag]] = None,
    ) -> CreateLoggedModelResponse:
        """Create a logged model.

        :param experiment_id: str
          The ID of the experiment that owns the model.
        :param model_type: str (optional)
          The type of the model, such as ``"Agent"``, ``"Classifier"``, ``"LLM"``.
        :param name: str (optional)
          The name of the model (optional). If not specified one will be generated.
        :param params: List[:class:`LoggedModelParameter`] (optional)
          Parameters attached to the model.
        :param source_run_id: str (optional)
          The ID of the run that created the model.
        :param tags: List[:class:`LoggedModelTag`] (optional)
          Tags attached to the model.

        :returns: :class:`CreateLoggedModelResponse`
        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        if model_type is not None:
            body["model_type"] = model_type
        if name is not None:
            body["name"] = name
        if params is not None:
            body["params"] = [v.as_dict() for v in params]
        if source_run_id is not None:
            body["source_run_id"] = source_run_id
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/logged-models", body=body, headers=headers)
        return CreateLoggedModelResponse.from_dict(res)

    def create_run(
        self,
        *,
        experiment_id: Optional[str] = None,
        run_name: Optional[str] = None,
        start_time: Optional[int] = None,
        tags: Optional[List[RunTag]] = None,
        user_id: Optional[str] = None,
    ) -> CreateRunResponse:
        """Creates a new run within an experiment. A run is usually a single execution of a machine learning or
        data ETL pipeline. MLflow uses runs to track the `mlflowParam`, `mlflowMetric`, and `mlflowRunTag`
        associated with a single execution.

        :param experiment_id: str (optional)
          ID of the associated experiment.
        :param run_name: str (optional)
          The name of the run.
        :param start_time: int (optional)
          Unix timestamp in milliseconds of when the run started.
        :param tags: List[:class:`RunTag`] (optional)
          Additional metadata for run.
        :param user_id: str (optional)
          ID of the user executing the run. This field is deprecated as of MLflow 1.0, and will be removed in
          a future MLflow release. Use 'mlflow.user' tag instead.

        :returns: :class:`CreateRunResponse`
        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        if run_name is not None:
            body["run_name"] = run_name
        if start_time is not None:
            body["start_time"] = start_time
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        if user_id is not None:
            body["user_id"] = user_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/runs/create", body=body, headers=headers)
        return CreateRunResponse.from_dict(res)

    def delete_experiment(self, experiment_id: str):
        """Marks an experiment and associated metadata, runs, metrics, params, and tags for deletion. If the
        experiment uses FileStore, artifacts associated with the experiment are also deleted.

        :param experiment_id: str
          ID of the associated experiment.


        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/experiments/delete", body=body, headers=headers)

    def delete_logged_model(self, model_id: str):
        """Delete a logged model.

        :param model_id: str
          The ID of the logged model to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/mlflow/logged-models/{model_id}", headers=headers)

    def delete_logged_model_tag(self, model_id: str, tag_key: str):
        """Delete a tag on a logged model.

        :param model_id: str
          The ID of the logged model to delete the tag from.
        :param tag_key: str
          The tag key.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/mlflow/logged-models/{model_id}/tags/{tag_key}", headers=headers)

    def delete_run(self, run_id: str):
        """Marks a run for deletion.

        :param run_id: str
          ID of the run to delete.


        """

        body = {}
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/delete", body=body, headers=headers)

    def delete_runs(
        self, experiment_id: str, max_timestamp_millis: int, *, max_runs: Optional[int] = None
    ) -> DeleteRunsResponse:
        """Bulk delete runs in an experiment that were created prior to or at the specified timestamp. Deletes at
        most max_runs per request. To call this API from a Databricks Notebook in Python, you can use the
        client code snippet on

        :param experiment_id: str
          The ID of the experiment containing the runs to delete.
        :param max_timestamp_millis: int
          The maximum creation timestamp in milliseconds since the UNIX epoch for deleting runs. Only runs
          created prior to or at this timestamp are deleted.
        :param max_runs: int (optional)
          An optional positive integer indicating the maximum number of runs to delete. The maximum allowed
          value for max_runs is 10000.

        :returns: :class:`DeleteRunsResponse`
        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        if max_runs is not None:
            body["max_runs"] = max_runs
        if max_timestamp_millis is not None:
            body["max_timestamp_millis"] = max_timestamp_millis
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/databricks/runs/delete-runs", body=body, headers=headers)
        return DeleteRunsResponse.from_dict(res)

    def delete_tag(self, run_id: str, key: str):
        """Deletes a tag on a run. Tags are run metadata that can be updated during a run and after a run
        completes.

        :param run_id: str
          ID of the run that the tag was logged under. Must be provided.
        :param key: str
          Name of the tag. Maximum size is 255 bytes. Must be provided.


        """

        body = {}
        if key is not None:
            body["key"] = key
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/delete-tag", body=body, headers=headers)

    def finalize_logged_model(self, model_id: str, status: LoggedModelStatus) -> FinalizeLoggedModelResponse:
        """Finalize a logged model.

        :param model_id: str
          The ID of the logged model to finalize.
        :param status: :class:`LoggedModelStatus`
          Whether or not the model is ready for use. ``"LOGGED_MODEL_UPLOAD_FAILED"`` indicates that something
          went wrong when logging the model weights / agent code.

        :returns: :class:`FinalizeLoggedModelResponse`
        """

        body = {}
        if status is not None:
            body["status"] = status.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/mlflow/logged-models/{model_id}", body=body, headers=headers)
        return FinalizeLoggedModelResponse.from_dict(res)

    def get_by_name(self, experiment_name: str) -> GetExperimentByNameResponse:
        """Gets metadata for an experiment.

        This endpoint will return deleted experiments, but prefers the active experiment if an active and
        deleted experiment share the same name. If multiple deleted experiments share the same name, the API
        will return one of them.

        Throws `RESOURCE_DOES_NOT_EXIST` if no experiment with the specified name exists.

        :param experiment_name: str
          Name of the associated experiment.

        :returns: :class:`GetExperimentByNameResponse`
        """

        query = {}
        if experiment_name is not None:
            query["experiment_name"] = experiment_name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/mlflow/experiments/get-by-name", query=query, headers=headers)
        return GetExperimentByNameResponse.from_dict(res)

    def get_experiment(self, experiment_id: str) -> GetExperimentResponse:
        """Gets metadata for an experiment. This method works on deleted experiments.

        :param experiment_id: str
          ID of the associated experiment.

        :returns: :class:`GetExperimentResponse`
        """

        query = {}
        if experiment_id is not None:
            query["experiment_id"] = experiment_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/mlflow/experiments/get", query=query, headers=headers)
        return GetExperimentResponse.from_dict(res)

    def get_history(
        self,
        metric_key: str,
        *,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        run_id: Optional[str] = None,
        run_uuid: Optional[str] = None,
    ) -> Iterator[Metric]:
        """Gets a list of all values for the specified metric for a given run.

        :param metric_key: str
          Name of the metric.
        :param max_results: int (optional)
          Maximum number of Metric records to return per paginated request. Default is set to 25,000. If set
          higher than 25,000, a request Exception will be raised.
        :param page_token: str (optional)
          Token indicating the page of metric histories to fetch.
        :param run_id: str (optional)
          ID of the run from which to fetch metric values. Must be provided.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run from which to fetch metric values. This field will
          be removed in a future MLflow version.

        :returns: Iterator over :class:`Metric`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if metric_key is not None:
            query["metric_key"] = metric_key
        if page_token is not None:
            query["page_token"] = page_token
        if run_id is not None:
            query["run_id"] = run_id
        if run_uuid is not None:
            query["run_uuid"] = run_uuid
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/metrics/get-history", query=query, headers=headers)
            if "metrics" in json:
                for v in json["metrics"]:
                    yield Metric.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def get_logged_model(self, model_id: str) -> GetLoggedModelResponse:
        """Get a logged model.

        :param model_id: str
          The ID of the logged model to retrieve.

        :returns: :class:`GetLoggedModelResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/mlflow/logged-models/{model_id}", headers=headers)
        return GetLoggedModelResponse.from_dict(res)

    def get_permission_levels(self, experiment_id: str) -> GetExperimentPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param experiment_id: str
          The experiment for which to get or manage permissions.

        :returns: :class:`GetExperimentPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/experiments/{experiment_id}/permissionLevels", headers=headers)
        return GetExperimentPermissionLevelsResponse.from_dict(res)

    def get_permissions(self, experiment_id: str) -> ExperimentPermissions:
        """Gets the permissions of an experiment. Experiments can inherit permissions from their root object.

        :param experiment_id: str
          The experiment for which to get or manage permissions.

        :returns: :class:`ExperimentPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/experiments/{experiment_id}", headers=headers)
        return ExperimentPermissions.from_dict(res)

    def get_run(self, run_id: str, *, run_uuid: Optional[str] = None) -> GetRunResponse:
        """Gets the metadata, metrics, params, and tags for a run. In the case where multiple metrics with the
        same key are logged for a run, return only the value with the latest timestamp.

        If there are multiple values with the latest timestamp, return the maximum of these values.

        :param run_id: str
          ID of the run to fetch. Must be provided.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run to fetch. This field will be removed in a future
          MLflow version.

        :returns: :class:`GetRunResponse`
        """

        query = {}
        if run_id is not None:
            query["run_id"] = run_id
        if run_uuid is not None:
            query["run_uuid"] = run_uuid
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/mlflow/runs/get", query=query, headers=headers)
        return GetRunResponse.from_dict(res)

    def list_artifacts(
        self,
        *,
        page_token: Optional[str] = None,
        path: Optional[str] = None,
        run_id: Optional[str] = None,
        run_uuid: Optional[str] = None,
    ) -> Iterator[FileInfo]:
        """List artifacts for a run. Takes an optional `artifact_path` prefix which if specified, the response
        contains only artifacts with the specified prefix. A maximum of 1000 artifacts will be retrieved for
        UC Volumes. Please call `/api/2.0/fs/directories{directory_path}` for listing artifacts in UC Volumes,
        which supports pagination. See [List directory contents | Files
        API](/api/workspace/files/listdirectorycontents).

        :param page_token: str (optional)
          The token indicating the page of artifact results to fetch. `page_token` is not supported when
          listing artifacts in UC Volumes. A maximum of 1000 artifacts will be retrieved for UC Volumes.
          Please call `/api/2.0/fs/directories{directory_path}` for listing artifacts in UC Volumes, which
          supports pagination. See [List directory contents | Files
          API](/api/workspace/files/listdirectorycontents).
        :param path: str (optional)
          Filter artifacts matching this path (a relative path from the root artifact directory).
        :param run_id: str (optional)
          ID of the run whose artifacts to list. Must be provided.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run whose artifacts to list. This field will be removed
          in a future MLflow version.

        :returns: Iterator over :class:`FileInfo`
        """

        query = {}
        if page_token is not None:
            query["page_token"] = page_token
        if path is not None:
            query["path"] = path
        if run_id is not None:
            query["run_id"] = run_id
        if run_uuid is not None:
            query["run_uuid"] = run_uuid
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/artifacts/list", query=query, headers=headers)
            if "files" in json:
                for v in json["files"]:
                    yield FileInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_experiments(
        self,
        *,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        view_type: Optional[ViewType] = None,
    ) -> Iterator[Experiment]:
        """Gets a list of all experiments.

        :param max_results: int (optional)
          Maximum number of experiments desired. If `max_results` is unspecified, return all experiments. If
          `max_results` is too large, it'll be automatically capped at 1000. Callers of this endpoint are
          encouraged to pass max_results explicitly and leverage page_token to iterate through experiments.
        :param page_token: str (optional)
          Token indicating the page of experiments to fetch
        :param view_type: :class:`ViewType` (optional)
          Qualifier for type of experiments to be returned. If unspecified, return only active experiments.

        :returns: Iterator over :class:`Experiment`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if view_type is not None:
            query["view_type"] = view_type.value
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/experiments/list", query=query, headers=headers)
            if "experiments" in json:
                for v in json["experiments"]:
                    yield Experiment.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def log_batch(
        self,
        *,
        metrics: Optional[List[Metric]] = None,
        params: Optional[List[Param]] = None,
        run_id: Optional[str] = None,
        tags: Optional[List[RunTag]] = None,
    ):
        """Logs a batch of metrics, params, and tags for a run. If any data failed to be persisted, the server
        will respond with an error (non-200 status code).

        In case of error (due to internal server error or an invalid request), partial data may be written.

        You can write metrics, params, and tags in interleaving fashion, but within a given entity type are
        guaranteed to follow the order specified in the request body.

        The overwrite behavior for metrics, params, and tags is as follows:

        * Metrics: metric values are never overwritten. Logging a metric (key, value, timestamp) appends to
        the set of values for the metric with the provided key.

        * Tags: tag values can be overwritten by successive writes to the same tag key. That is, if multiple
        tag values with the same key are provided in the same API request, the last-provided tag value is
        written. Logging the same tag (key, value) is permitted. Specifically, logging a tag is idempotent.

        * Parameters: once written, param values cannot be changed (attempting to overwrite a param value will
        result in an error). However, logging the same param (key, value) is permitted. Specifically, logging
        a param is idempotent.

        Request Limits ------------------------------- A single JSON-serialized API request may be up to 1 MB
        in size and contain:

        * No more than 1000 metrics, params, and tags in total

        * Up to 1000 metrics

        * Up to 100 params

        * Up to 100 tags

        For example, a valid request might contain 900 metrics, 50 params, and 50 tags, but logging 900
        metrics, 50 params, and 51 tags is invalid.

        The following limits also apply to metric, param, and tag keys and values:

        * Metric keys, param keys, and tag keys can be up to 250 characters in length

        * Parameter and tag values can be up to 250 characters in length

        :param metrics: List[:class:`Metric`] (optional)
          Metrics to log. A single request can contain up to 1000 metrics, and up to 1000 metrics, params, and
          tags in total.
        :param params: List[:class:`Param`] (optional)
          Params to log. A single request can contain up to 100 params, and up to 1000 metrics, params, and
          tags in total.
        :param run_id: str (optional)
          ID of the run to log under
        :param tags: List[:class:`RunTag`] (optional)
          Tags to log. A single request can contain up to 100 tags, and up to 1000 metrics, params, and tags
          in total.


        """

        body = {}
        if metrics is not None:
            body["metrics"] = [v.as_dict() for v in metrics]
        if params is not None:
            body["params"] = [v.as_dict() for v in params]
        if run_id is not None:
            body["run_id"] = run_id
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/log-batch", body=body, headers=headers)

    def log_inputs(
        self, run_id: str, *, datasets: Optional[List[DatasetInput]] = None, models: Optional[List[ModelInput]] = None
    ):
        """Logs inputs, such as datasets and models, to an MLflow Run.

        :param run_id: str
          ID of the run to log under
        :param datasets: List[:class:`DatasetInput`] (optional)
          Dataset inputs
        :param models: List[:class:`ModelInput`] (optional)
          Model inputs


        """

        body = {}
        if datasets is not None:
            body["datasets"] = [v.as_dict() for v in datasets]
        if models is not None:
            body["models"] = [v.as_dict() for v in models]
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/log-inputs", body=body, headers=headers)

    def log_logged_model_params(self, model_id: str, *, params: Optional[List[LoggedModelParameter]] = None):
        """Logs params for a logged model. A param is a key-value pair (string key, string value). Examples
        include hyperparameters used for ML model training. A param can be logged only once for a logged
        model, and attempting to overwrite an existing param with a different value will result in an error

        :param model_id: str
          The ID of the logged model to log params for.
        :param params: List[:class:`LoggedModelParameter`] (optional)
          Parameters to attach to the model.


        """

        body = {}
        if params is not None:
            body["params"] = [v.as_dict() for v in params]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", f"/api/2.0/mlflow/logged-models/{model_id}/params", body=body, headers=headers)

    def log_metric(
        self,
        key: str,
        value: float,
        timestamp: int,
        *,
        dataset_digest: Optional[str] = None,
        dataset_name: Optional[str] = None,
        model_id: Optional[str] = None,
        run_id: Optional[str] = None,
        run_uuid: Optional[str] = None,
        step: Optional[int] = None,
    ):
        """Log a metric for a run. A metric is a key-value pair (string key, float value) with an associated
        timestamp. Examples include the various metrics that represent ML model accuracy. A metric can be
        logged multiple times.

        :param key: str
          Name of the metric.
        :param value: float
          Double value of the metric being logged.
        :param timestamp: int
          Unix timestamp in milliseconds at the time metric was logged.
        :param dataset_digest: str (optional)
          Dataset digest of the dataset associated with the metric, e.g. an md5 hash of the dataset that
          uniquely identifies it within datasets of the same name.
        :param dataset_name: str (optional)
          The name of the dataset associated with the metric. E.g. “my.uc.table@2” “nyc-taxi-dataset”,
          “fantastic-elk-3”
        :param model_id: str (optional)
          ID of the logged model associated with the metric, if applicable
        :param run_id: str (optional)
          ID of the run under which to log the metric. Must be provided.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run under which to log the metric. This field will be
          removed in a future MLflow version.
        :param step: int (optional)
          Step at which to log the metric


        """

        body = {}
        if dataset_digest is not None:
            body["dataset_digest"] = dataset_digest
        if dataset_name is not None:
            body["dataset_name"] = dataset_name
        if key is not None:
            body["key"] = key
        if model_id is not None:
            body["model_id"] = model_id
        if run_id is not None:
            body["run_id"] = run_id
        if run_uuid is not None:
            body["run_uuid"] = run_uuid
        if step is not None:
            body["step"] = step
        if timestamp is not None:
            body["timestamp"] = timestamp
        if value is not None:
            body["value"] = value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/log-metric", body=body, headers=headers)

    def log_model(self, *, model_json: Optional[str] = None, run_id: Optional[str] = None):
        """**Note:** the [Create a logged model](/api/workspace/experiments/createloggedmodel) API replaces this
        endpoint.

        Log a model to an MLflow Run.

        :param model_json: str (optional)
          MLmodel file in json format.
        :param run_id: str (optional)
          ID of the run to log under


        """

        body = {}
        if model_json is not None:
            body["model_json"] = model_json
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/log-model", body=body, headers=headers)

    def log_outputs(self, run_id: str, *, models: Optional[List[ModelOutput]] = None):
        """Logs outputs, such as models, from an MLflow Run.

        :param run_id: str
          The ID of the Run from which to log outputs.
        :param models: List[:class:`ModelOutput`] (optional)
          The model outputs from the Run.


        """

        body = {}
        if models is not None:
            body["models"] = [v.as_dict() for v in models]
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/outputs", body=body, headers=headers)

    def log_param(self, key: str, value: str, *, run_id: Optional[str] = None, run_uuid: Optional[str] = None):
        """Logs a param used for a run. A param is a key-value pair (string key, string value). Examples include
        hyperparameters used for ML model training and constant dates and values used in an ETL pipeline. A
        param can be logged only once for a run.

        :param key: str
          Name of the param. Maximum size is 255 bytes.
        :param value: str
          String value of the param being logged. Maximum size is 500 bytes.
        :param run_id: str (optional)
          ID of the run under which to log the param. Must be provided.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run under which to log the param. This field will be
          removed in a future MLflow version.


        """

        body = {}
        if key is not None:
            body["key"] = key
        if run_id is not None:
            body["run_id"] = run_id
        if run_uuid is not None:
            body["run_uuid"] = run_uuid
        if value is not None:
            body["value"] = value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/log-parameter", body=body, headers=headers)

    def restore_experiment(self, experiment_id: str):
        """Restore an experiment marked for deletion. This also restores associated metadata, runs, metrics,
        params, and tags. If experiment uses FileStore, underlying artifacts associated with experiment are
        also restored.

        Throws `RESOURCE_DOES_NOT_EXIST` if experiment was never created or was permanently deleted.

        :param experiment_id: str
          ID of the associated experiment.


        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/experiments/restore", body=body, headers=headers)

    def restore_run(self, run_id: str):
        """Restores a deleted run. This also restores associated metadata, runs, metrics, params, and tags.

        Throws `RESOURCE_DOES_NOT_EXIST` if the run was never created or was permanently deleted.

        :param run_id: str
          ID of the run to restore.


        """

        body = {}
        if run_id is not None:
            body["run_id"] = run_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/restore", body=body, headers=headers)

    def restore_runs(
        self, experiment_id: str, min_timestamp_millis: int, *, max_runs: Optional[int] = None
    ) -> RestoreRunsResponse:
        """Bulk restore runs in an experiment that were deleted no earlier than the specified timestamp. Restores
        at most max_runs per request. To call this API from a Databricks Notebook in Python, you can use the
        client code snippet on

        :param experiment_id: str
          The ID of the experiment containing the runs to restore.
        :param min_timestamp_millis: int
          The minimum deletion timestamp in milliseconds since the UNIX epoch for restoring runs. Only runs
          deleted no earlier than this timestamp are restored.
        :param max_runs: int (optional)
          An optional positive integer indicating the maximum number of runs to restore. The maximum allowed
          value for max_runs is 10000.

        :returns: :class:`RestoreRunsResponse`
        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        if max_runs is not None:
            body["max_runs"] = max_runs
        if min_timestamp_millis is not None:
            body["min_timestamp_millis"] = min_timestamp_millis
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/databricks/runs/restore-runs", body=body, headers=headers)
        return RestoreRunsResponse.from_dict(res)

    def search_experiments(
        self,
        *,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        view_type: Optional[ViewType] = None,
    ) -> Iterator[Experiment]:
        """Searches for experiments that satisfy specified search criteria.

        :param filter: str (optional)
          String representing a SQL filter condition (e.g. "name ILIKE 'my-experiment%'")
        :param max_results: int (optional)
          Maximum number of experiments desired. Max threshold is 3000.
        :param order_by: List[str] (optional)
          List of columns for ordering search results, which can include experiment name and last updated
          timestamp with an optional "DESC" or "ASC" annotation, where "ASC" is the default. Tiebreaks are
          done by experiment id DESC.
        :param page_token: str (optional)
          Token indicating the page of experiments to fetch
        :param view_type: :class:`ViewType` (optional)
          Qualifier for type of experiments to be returned. If unspecified, return only active experiments.

        :returns: Iterator over :class:`Experiment`
        """

        body = {}
        if filter is not None:
            body["filter"] = filter
        if max_results is not None:
            body["max_results"] = max_results
        if order_by is not None:
            body["order_by"] = [v for v in order_by]
        if page_token is not None:
            body["page_token"] = page_token
        if view_type is not None:
            body["view_type"] = view_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("POST", "/api/2.0/mlflow/experiments/search", body=body, headers=headers)
            if "experiments" in json:
                for v in json["experiments"]:
                    yield Experiment.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            body["page_token"] = json["next_page_token"]

    def search_logged_models(
        self,
        *,
        datasets: Optional[List[SearchLoggedModelsDataset]] = None,
        experiment_ids: Optional[List[str]] = None,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[SearchLoggedModelsOrderBy]] = None,
        page_token: Optional[str] = None,
    ) -> SearchLoggedModelsResponse:
        """Search for Logged Models that satisfy specified search criteria.

        :param datasets: List[:class:`SearchLoggedModelsDataset`] (optional)
          List of datasets on which to apply the metrics filter clauses. For example, a filter with
          `metrics.accuracy > 0.9` and dataset info with name "test_dataset" means we will return all logged
          models with accuracy > 0.9 on the test_dataset. Metric values from ANY dataset matching the criteria
          are considered. If no datasets are specified, then metrics across all datasets are considered in the
          filter.
        :param experiment_ids: List[str] (optional)
          The IDs of the experiments in which to search for logged models.
        :param filter: str (optional)
          A filter expression over logged model info and data that allows returning a subset of logged models.
          The syntax is a subset of SQL that supports AND'ing together binary operations.

          Example: ``params.alpha < 0.3 AND metrics.accuracy > 0.9``.
        :param max_results: int (optional)
          The maximum number of Logged Models to return. The maximum limit is 50.
        :param order_by: List[:class:`SearchLoggedModelsOrderBy`] (optional)
          The list of columns for ordering the results, with additional fields for sorting criteria.
        :param page_token: str (optional)
          The token indicating the page of logged models to fetch.

        :returns: :class:`SearchLoggedModelsResponse`
        """

        body = {}
        if datasets is not None:
            body["datasets"] = [v.as_dict() for v in datasets]
        if experiment_ids is not None:
            body["experiment_ids"] = [v for v in experiment_ids]
        if filter is not None:
            body["filter"] = filter
        if max_results is not None:
            body["max_results"] = max_results
        if order_by is not None:
            body["order_by"] = [v.as_dict() for v in order_by]
        if page_token is not None:
            body["page_token"] = page_token
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/logged-models/search", body=body, headers=headers)
        return SearchLoggedModelsResponse.from_dict(res)

    def search_runs(
        self,
        *,
        experiment_ids: Optional[List[str]] = None,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        run_view_type: Optional[ViewType] = None,
    ) -> Iterator[Run]:
        """Searches for runs that satisfy expressions.

        Search expressions can use `mlflowMetric` and `mlflowParam` keys.

        :param experiment_ids: List[str] (optional)
          List of experiment IDs to search over.
        :param filter: str (optional)
          A filter expression over params, metrics, and tags, that allows returning a subset of runs. The
          syntax is a subset of SQL that supports ANDing together binary operations between a param, metric,
          or tag and a constant.

          Example: `metrics.rmse < 1 and params.model_class = 'LogisticRegression'`

          You can select columns with special characters (hyphen, space, period, etc.) by using double quotes:
          `metrics."model class" = 'LinearRegression' and tags."user-name" = 'Tomas'`

          Supported operators are `=`, `!=`, `>`, `>=`, `<`, and `<=`.
        :param max_results: int (optional)
          Maximum number of runs desired. Max threshold is 50000
        :param order_by: List[str] (optional)
          List of columns to be ordered by, including attributes, params, metrics, and tags with an optional
          `"DESC"` or `"ASC"` annotation, where `"ASC"` is the default. Example: `["params.input DESC",
          "metrics.alpha ASC", "metrics.rmse"]`. Tiebreaks are done by start_time `DESC` followed by `run_id`
          for runs with the same start time (and this is the default ordering criterion if order_by is not
          provided).
        :param page_token: str (optional)
          Token for the current page of runs.
        :param run_view_type: :class:`ViewType` (optional)
          Whether to display only active, only deleted, or all runs. Defaults to only active runs.

        :returns: Iterator over :class:`Run`
        """

        body = {}
        if experiment_ids is not None:
            body["experiment_ids"] = [v for v in experiment_ids]
        if filter is not None:
            body["filter"] = filter
        if max_results is not None:
            body["max_results"] = max_results
        if order_by is not None:
            body["order_by"] = [v for v in order_by]
        if page_token is not None:
            body["page_token"] = page_token
        if run_view_type is not None:
            body["run_view_type"] = run_view_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("POST", "/api/2.0/mlflow/runs/search", body=body, headers=headers)
            if "runs" in json:
                for v in json["runs"]:
                    yield Run.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            body["page_token"] = json["next_page_token"]

    def set_experiment_tag(self, experiment_id: str, key: str, value: str):
        """Sets a tag on an experiment. Experiment tags are metadata that can be updated.

        :param experiment_id: str
          ID of the experiment under which to log the tag. Must be provided.
        :param key: str
          Name of the tag. Keys up to 250 bytes in size are supported.
        :param value: str
          String value of the tag being logged. Values up to 64KB in size are supported.


        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        if key is not None:
            body["key"] = key
        if value is not None:
            body["value"] = value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/experiments/set-experiment-tag", body=body, headers=headers)

    def set_logged_model_tags(self, model_id: str, *, tags: Optional[List[LoggedModelTag]] = None):
        """Set tags for a logged model.

        :param model_id: str
          The ID of the logged model to set the tags on.
        :param tags: List[:class:`LoggedModelTag`] (optional)
          The tags to set on the logged model.


        """

        body = {}
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("PATCH", f"/api/2.0/mlflow/logged-models/{model_id}/tags", body=body, headers=headers)

    def set_permissions(
        self, experiment_id: str, *, access_control_list: Optional[List[ExperimentAccessControlRequest]] = None
    ) -> ExperimentPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param experiment_id: str
          The experiment for which to get or manage permissions.
        :param access_control_list: List[:class:`ExperimentAccessControlRequest`] (optional)

        :returns: :class:`ExperimentPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/permissions/experiments/{experiment_id}", body=body, headers=headers)
        return ExperimentPermissions.from_dict(res)

    def set_tag(self, key: str, value: str, *, run_id: Optional[str] = None, run_uuid: Optional[str] = None):
        """Sets a tag on a run. Tags are run metadata that can be updated during a run and after a run completes.

        :param key: str
          Name of the tag. Keys up to 250 bytes in size are supported.
        :param value: str
          String value of the tag being logged. Values up to 64KB in size are supported.
        :param run_id: str (optional)
          ID of the run under which to log the tag. Must be provided.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run under which to log the tag. This field will be
          removed in a future MLflow version.


        """

        body = {}
        if key is not None:
            body["key"] = key
        if run_id is not None:
            body["run_id"] = run_id
        if run_uuid is not None:
            body["run_uuid"] = run_uuid
        if value is not None:
            body["value"] = value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/runs/set-tag", body=body, headers=headers)

    def update_experiment(self, experiment_id: str, *, new_name: Optional[str] = None):
        """Updates experiment metadata.

        :param experiment_id: str
          ID of the associated experiment.
        :param new_name: str (optional)
          If provided, the experiment's name is changed to the new name. The new name must be unique.


        """

        body = {}
        if experiment_id is not None:
            body["experiment_id"] = experiment_id
        if new_name is not None:
            body["new_name"] = new_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/experiments/update", body=body, headers=headers)

    def update_permissions(
        self, experiment_id: str, *, access_control_list: Optional[List[ExperimentAccessControlRequest]] = None
    ) -> ExperimentPermissions:
        """Updates the permissions on an experiment. Experiments can inherit permissions from their root object.

        :param experiment_id: str
          The experiment for which to get or manage permissions.
        :param access_control_list: List[:class:`ExperimentAccessControlRequest`] (optional)

        :returns: :class:`ExperimentPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/permissions/experiments/{experiment_id}", body=body, headers=headers)
        return ExperimentPermissions.from_dict(res)

    def update_run(
        self,
        *,
        end_time: Optional[int] = None,
        run_id: Optional[str] = None,
        run_name: Optional[str] = None,
        run_uuid: Optional[str] = None,
        status: Optional[UpdateRunStatus] = None,
    ) -> UpdateRunResponse:
        """Updates run metadata.

        :param end_time: int (optional)
          Unix timestamp in milliseconds of when the run ended.
        :param run_id: str (optional)
          ID of the run to update. Must be provided.
        :param run_name: str (optional)
          Updated name of the run.
        :param run_uuid: str (optional)
          [Deprecated, use `run_id` instead] ID of the run to update. This field will be removed in a future
          MLflow version.
        :param status: :class:`UpdateRunStatus` (optional)
          Updated status of the run.

        :returns: :class:`UpdateRunResponse`
        """

        body = {}
        if end_time is not None:
            body["end_time"] = end_time
        if run_id is not None:
            body["run_id"] = run_id
        if run_name is not None:
            body["run_name"] = run_name
        if run_uuid is not None:
            body["run_uuid"] = run_uuid
        if status is not None:
            body["status"] = status.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/runs/update", body=body, headers=headers)
        return UpdateRunResponse.from_dict(res)


class FeatureEngineeringAPI:
    """[description]"""

    def __init__(self, api_client):
        self._api = api_client

    def batch_create_materialized_features(
        self, requests: List[CreateMaterializedFeatureRequest]
    ) -> BatchCreateMaterializedFeaturesResponse:
        """Batch create materialized features.

        :param requests: List[:class:`CreateMaterializedFeatureRequest`]
          The requests to create materialized features.

        :returns: :class:`BatchCreateMaterializedFeaturesResponse`
        """

        body = {}
        if requests is not None:
            body["requests"] = [v.as_dict() for v in requests]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", "/api/2.0/feature-engineering/materialized-features:batchCreate", body=body, headers=headers
        )
        return BatchCreateMaterializedFeaturesResponse.from_dict(res)

    def create_feature(self, feature: Feature) -> Feature:
        """Create a Feature.

        :param feature: :class:`Feature`
          Feature to create.

        :returns: :class:`Feature`
        """

        body = feature.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/feature-engineering/features", body=body, headers=headers)
        return Feature.from_dict(res)

    def create_kafka_config(self, kafka_config: KafkaConfig) -> KafkaConfig:
        """Create a Kafka config. During PrPr, Kafka configs can be read and used when creating features under
        the entire metastore. Only the creator of the Kafka config can delete it.

        :param kafka_config: :class:`KafkaConfig`

        :returns: :class:`KafkaConfig`
        """

        body = kafka_config.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/feature-engineering/features/kafka-configs", body=body, headers=headers)
        return KafkaConfig.from_dict(res)

    def create_materialized_feature(self, materialized_feature: MaterializedFeature) -> MaterializedFeature:
        """Create a materialized feature.

        :param materialized_feature: :class:`MaterializedFeature`
          The materialized feature to create.

        :returns: :class:`MaterializedFeature`
        """

        body = materialized_feature.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/feature-engineering/materialized-features", body=body, headers=headers)
        return MaterializedFeature.from_dict(res)

    def delete_feature(self, full_name: str):
        """Delete a Feature.

        :param full_name: str
          Name of the feature to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/feature-engineering/features/{full_name}", headers=headers)

    def delete_kafka_config(self, name: str):
        """Delete a Kafka config. During PrPr, Kafka configs can be read and used when creating features under
        the entire metastore. Only the creator of the Kafka config can delete it.

        :param name: str
          Name of the Kafka config to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/feature-engineering/features/kafka-configs/{name}", headers=headers)

    def delete_materialized_feature(self, materialized_feature_id: str):
        """Delete a materialized feature.

        :param materialized_feature_id: str
          The ID of the materialized feature to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.0/feature-engineering/materialized-features/{materialized_feature_id}", headers=headers
        )

    def get_feature(self, full_name: str) -> Feature:
        """Get a Feature.

        :param full_name: str
          Name of the feature to get.

        :returns: :class:`Feature`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/feature-engineering/features/{full_name}", headers=headers)
        return Feature.from_dict(res)

    def get_kafka_config(self, name: str) -> KafkaConfig:
        """Get a Kafka config. During PrPr, Kafka configs can be read and used when creating features under the
        entire metastore. Only the creator of the Kafka config can delete it.

        :param name: str
          Name of the Kafka config to get.

        :returns: :class:`KafkaConfig`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/feature-engineering/features/kafka-configs/{name}", headers=headers)
        return KafkaConfig.from_dict(res)

    def get_materialized_feature(self, materialized_feature_id: str) -> MaterializedFeature:
        """Get a materialized feature.

        :param materialized_feature_id: str
          The ID of the materialized feature.

        :returns: :class:`MaterializedFeature`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/feature-engineering/materialized-features/{materialized_feature_id}", headers=headers
        )
        return MaterializedFeature.from_dict(res)

    def list_features(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Feature]:
        """List Features.

        :param page_size: int (optional)
          The maximum number of results to return.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous query.

        :returns: Iterator over :class:`Feature`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/feature-engineering/features", query=query, headers=headers)
            if "features" in json:
                for v in json["features"]:
                    yield Feature.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_kafka_configs(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[KafkaConfig]:
        """List Kafka configs. During PrPr, Kafka configs can be read and used when creating features under the
        entire metastore. Only the creator of the Kafka config can delete it.

        :param page_size: int (optional)
          The maximum number of results to return.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous query.

        :returns: Iterator over :class:`KafkaConfig`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.0/feature-engineering/features/kafka-configs", query=query, headers=headers
            )
            if "kafka_configs" in json:
                for v in json["kafka_configs"]:
                    yield KafkaConfig.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_materialized_features(
        self, *, feature_name: Optional[str] = None, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[MaterializedFeature]:
        """List materialized features.

        :param feature_name: str (optional)
          Filter by feature name. If specified, only materialized features materialized from this feature will
          be returned.
        :param page_size: int (optional)
          The maximum number of results to return. Defaults to 100 if not specified. Cannot be greater than
          1000.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous query.

        :returns: Iterator over :class:`MaterializedFeature`
        """

        query = {}
        if feature_name is not None:
            query["feature_name"] = feature_name
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.0/feature-engineering/materialized-features", query=query, headers=headers
            )
            if "materialized_features" in json:
                for v in json["materialized_features"]:
                    yield MaterializedFeature.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_feature(self, full_name: str, feature: Feature, update_mask: str) -> Feature:
        """Update a Feature.

        :param full_name: str
          The full three-part name (catalog, schema, name) of the feature.
        :param feature: :class:`Feature`
          Feature to update.
        :param update_mask: str
          The list of fields to update.

        :returns: :class:`Feature`
        """

        body = feature.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.0/feature-engineering/features/{full_name}", query=query, body=body, headers=headers
        )
        return Feature.from_dict(res)

    def update_kafka_config(self, name: str, kafka_config: KafkaConfig, update_mask: FieldMask) -> KafkaConfig:
        """Update a Kafka config. During PrPr, Kafka configs can be read and used when creating features under
        the entire metastore. Only the creator of the Kafka config can delete it.

        :param name: str
          Name that uniquely identifies this Kafka config within the metastore. This will be the identifier
          used from the Feature object to reference these configs for a feature. Can be distinct from topic
          name.
        :param kafka_config: :class:`KafkaConfig`
          The Kafka config to update.
        :param update_mask: FieldMask
          The list of fields to update.

        :returns: :class:`KafkaConfig`
        """

        body = kafka_config.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask.ToJsonString()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH",
            f"/api/2.0/feature-engineering/features/kafka-configs/{name}",
            query=query,
            body=body,
            headers=headers,
        )
        return KafkaConfig.from_dict(res)

    def update_materialized_feature(
        self, materialized_feature_id: str, materialized_feature: MaterializedFeature, update_mask: str
    ) -> MaterializedFeature:
        """Update a materialized feature (pause/resume).

        :param materialized_feature_id: str
          Unique identifier for the materialized feature.
        :param materialized_feature: :class:`MaterializedFeature`
          The materialized feature to update.
        :param update_mask: str
          Provide the materialization feature fields which should be updated. Currently, only the
          pipeline_state field can be updated.

        :returns: :class:`MaterializedFeature`
        """

        body = materialized_feature.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH",
            f"/api/2.0/feature-engineering/materialized-features/{materialized_feature_id}",
            query=query,
            body=body,
            headers=headers,
        )
        return MaterializedFeature.from_dict(res)


class FeatureStoreAPI:
    """A feature store is a centralized repository that enables data scientists to find and share features. Using
    a feature store also ensures that the code used to compute feature values is the same during model
    training and when the model is used for inference.

    An online store is a low-latency database used for feature lookup during real-time model inference or
    serve feature for real-time applications."""

    def __init__(self, api_client):
        self._api = api_client

    def create_online_store(self, online_store: OnlineStore) -> OnlineStore:
        """Create an Online Feature Store.

        :param online_store: :class:`OnlineStore`
          Online store to create.

        :returns: :class:`OnlineStore`
        """

        body = online_store.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/feature-store/online-stores", body=body, headers=headers)
        return OnlineStore.from_dict(res)

    def delete_online_store(self, name: str):
        """Delete an Online Feature Store.

        :param name: str
          Name of the online store to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/feature-store/online-stores/{name}", headers=headers)

    def delete_online_table(self, online_table_name: str):
        """Delete online table.

        :param online_table_name: str
          The full three-part (catalog, schema, table) name of the online table.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/feature-store/online-tables/{online_table_name}", headers=headers)

    def get_online_store(self, name: str) -> OnlineStore:
        """Get an Online Feature Store.

        :param name: str
          Name of the online store to get.

        :returns: :class:`OnlineStore`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/feature-store/online-stores/{name}", headers=headers)
        return OnlineStore.from_dict(res)

    def list_online_stores(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[OnlineStore]:
        """List Online Feature Stores.

        :param page_size: int (optional)
          The maximum number of results to return. Defaults to 100 if not specified.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous query.

        :returns: Iterator over :class:`OnlineStore`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/feature-store/online-stores", query=query, headers=headers)
            if "online_stores" in json:
                for v in json["online_stores"]:
                    yield OnlineStore.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def publish_table(self, source_table_name: str, publish_spec: PublishSpec) -> PublishTableResponse:
        """Publish features.

        :param source_table_name: str
          The full three-part (catalog, schema, table) name of the source table.
        :param publish_spec: :class:`PublishSpec`
          The specification for publishing the online table from the source table.

        :returns: :class:`PublishTableResponse`
        """

        body = {}
        if publish_spec is not None:
            body["publish_spec"] = publish_spec.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", f"/api/2.0/feature-store/tables/{source_table_name}/publish", body=body, headers=headers
        )
        return PublishTableResponse.from_dict(res)

    def update_online_store(self, name: str, online_store: OnlineStore, update_mask: str) -> OnlineStore:
        """Update an Online Feature Store.

        :param name: str
          The name of the online store. This is the unique identifier for the online store.
        :param online_store: :class:`OnlineStore`
          Online store to update.
        :param update_mask: str
          The list of fields to update.

        :returns: :class:`OnlineStore`
        """

        body = online_store.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.0/feature-store/online-stores/{name}", query=query, body=body, headers=headers
        )
        return OnlineStore.from_dict(res)


class ForecastingAPI:
    """The Forecasting API allows you to create and get serverless forecasting experiments"""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_experiment_forecasting_succeeded(
        self,
        experiment_id: str,
        timeout=timedelta(minutes=120),
        callback: Optional[Callable[[ForecastingExperiment], None]] = None,
    ) -> ForecastingExperiment:
        deadline = time.time() + timeout.total_seconds()
        target_states = (ForecastingExperimentState.SUCCEEDED,)
        failure_states = (
            ForecastingExperimentState.FAILED,
            ForecastingExperimentState.CANCELLED,
        )
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get_experiment(experiment_id=experiment_id)
            status = poll.state
            status_message = f"current status: {status}"
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach SUCCEEDED, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"experiment_id={experiment_id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def create_experiment(
        self,
        train_data_path: str,
        target_column: str,
        time_column: str,
        forecast_granularity: str,
        forecast_horizon: int,
        *,
        custom_weights_column: Optional[str] = None,
        experiment_path: Optional[str] = None,
        future_feature_data_path: Optional[str] = None,
        holiday_regions: Optional[List[str]] = None,
        include_features: Optional[List[str]] = None,
        max_runtime: Optional[int] = None,
        prediction_data_path: Optional[str] = None,
        primary_metric: Optional[str] = None,
        register_to: Optional[str] = None,
        split_column: Optional[str] = None,
        timeseries_identifier_columns: Optional[List[str]] = None,
        training_frameworks: Optional[List[str]] = None,
    ) -> Wait[ForecastingExperiment]:
        """Creates a serverless forecasting experiment. Returns the experiment ID.

        :param train_data_path: str
          The fully qualified path of a Unity Catalog table, formatted as catalog_name.schema_name.table_name,
          used as training data for the forecasting model.
        :param target_column: str
          The column in the input training table used as the prediction target for model training. The values
          in this column are used as the ground truth for model training.
        :param time_column: str
          The column in the input training table that represents each row's timestamp.
        :param forecast_granularity: str
          The time interval between consecutive rows in the time series data. Possible values include: '1
          second', '1 minute', '5 minutes', '10 minutes', '15 minutes', '30 minutes', 'Hourly', 'Daily',
          'Weekly', 'Monthly', 'Quarterly', 'Yearly'.
        :param forecast_horizon: int
          The number of time steps into the future to make predictions, calculated as a multiple of
          forecast_granularity. This value represents how far ahead the model should forecast.
        :param custom_weights_column: str (optional)
          The column in the training table used to customize weights for each time series.
        :param experiment_path: str (optional)
          The path in the workspace to store the created experiment.
        :param future_feature_data_path: str (optional)
          The fully qualified path of a Unity Catalog table, formatted as catalog_name.schema_name.table_name,
          used to store future feature data for predictions.
        :param holiday_regions: List[str] (optional)
          The region code(s) to automatically add holiday features. Currently supports only one region.
        :param include_features: List[str] (optional)
          Specifies the list of feature columns to include in model training. These columns must exist in the
          training data and be of type string, numerical, or boolean. If not specified, no additional features
          will be included. Note: Certain columns are automatically handled: - Automatically excluded:
          split_column, target_column, custom_weights_column. - Automatically included: time_column.
        :param max_runtime: int (optional)
          The maximum duration for the experiment in minutes. The experiment stops automatically if it exceeds
          this limit.
        :param prediction_data_path: str (optional)
          The fully qualified path of a Unity Catalog table, formatted as catalog_name.schema_name.table_name,
          used to store predictions.
        :param primary_metric: str (optional)
          The evaluation metric used to optimize the forecasting model.
        :param register_to: str (optional)
          The fully qualified path of a Unity Catalog model, formatted as catalog_name.schema_name.model_name,
          used to store the best model.
        :param split_column: str (optional)
          // The column in the training table used for custom data splits. Values must be 'train', 'validate',
          or 'test'.
        :param timeseries_identifier_columns: List[str] (optional)
          The column in the training table used to group the dataset for predicting individual time series.
        :param training_frameworks: List[str] (optional)
          List of frameworks to include for model tuning. Possible values are 'Prophet', 'ARIMA', 'DeepAR'. An
          empty list includes all supported frameworks.

        :returns:
          Long-running operation waiter for :class:`ForecastingExperiment`.
          See :method:wait_get_experiment_forecasting_succeeded for more details.
        """

        body = {}
        if custom_weights_column is not None:
            body["custom_weights_column"] = custom_weights_column
        if experiment_path is not None:
            body["experiment_path"] = experiment_path
        if forecast_granularity is not None:
            body["forecast_granularity"] = forecast_granularity
        if forecast_horizon is not None:
            body["forecast_horizon"] = forecast_horizon
        if future_feature_data_path is not None:
            body["future_feature_data_path"] = future_feature_data_path
        if holiday_regions is not None:
            body["holiday_regions"] = [v for v in holiday_regions]
        if include_features is not None:
            body["include_features"] = [v for v in include_features]
        if max_runtime is not None:
            body["max_runtime"] = max_runtime
        if prediction_data_path is not None:
            body["prediction_data_path"] = prediction_data_path
        if primary_metric is not None:
            body["primary_metric"] = primary_metric
        if register_to is not None:
            body["register_to"] = register_to
        if split_column is not None:
            body["split_column"] = split_column
        if target_column is not None:
            body["target_column"] = target_column
        if time_column is not None:
            body["time_column"] = time_column
        if timeseries_identifier_columns is not None:
            body["timeseries_identifier_columns"] = [v for v in timeseries_identifier_columns]
        if train_data_path is not None:
            body["train_data_path"] = train_data_path
        if training_frameworks is not None:
            body["training_frameworks"] = [v for v in training_frameworks]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do("POST", "/api/2.0/automl/create-forecasting-experiment", body=body, headers=headers)
        return Wait(
            self.wait_get_experiment_forecasting_succeeded,
            response=CreateForecastingExperimentResponse.from_dict(op_response),
            experiment_id=op_response["experiment_id"],
        )

    def create_experiment_and_wait(
        self,
        train_data_path: str,
        target_column: str,
        time_column: str,
        forecast_granularity: str,
        forecast_horizon: int,
        *,
        custom_weights_column: Optional[str] = None,
        experiment_path: Optional[str] = None,
        future_feature_data_path: Optional[str] = None,
        holiday_regions: Optional[List[str]] = None,
        include_features: Optional[List[str]] = None,
        max_runtime: Optional[int] = None,
        prediction_data_path: Optional[str] = None,
        primary_metric: Optional[str] = None,
        register_to: Optional[str] = None,
        split_column: Optional[str] = None,
        timeseries_identifier_columns: Optional[List[str]] = None,
        training_frameworks: Optional[List[str]] = None,
        timeout=timedelta(minutes=120),
    ) -> ForecastingExperiment:
        return self.create_experiment(
            custom_weights_column=custom_weights_column,
            experiment_path=experiment_path,
            forecast_granularity=forecast_granularity,
            forecast_horizon=forecast_horizon,
            future_feature_data_path=future_feature_data_path,
            holiday_regions=holiday_regions,
            include_features=include_features,
            max_runtime=max_runtime,
            prediction_data_path=prediction_data_path,
            primary_metric=primary_metric,
            register_to=register_to,
            split_column=split_column,
            target_column=target_column,
            time_column=time_column,
            timeseries_identifier_columns=timeseries_identifier_columns,
            train_data_path=train_data_path,
            training_frameworks=training_frameworks,
        ).result(timeout=timeout)

    def get_experiment(self, experiment_id: str) -> ForecastingExperiment:
        """Public RPC to get forecasting experiment

        :param experiment_id: str
          The unique ID of a forecasting experiment

        :returns: :class:`ForecastingExperiment`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/automl/get-forecasting-experiment/{experiment_id}", headers=headers)
        return ForecastingExperiment.from_dict(res)


class MaterializedFeaturesAPI:
    """Materialized Features are columns in tables and views that can be directly used as features to train and
    serve ML models."""

    def __init__(self, api_client):
        self._api = api_client

    def create_feature_tag(self, table_name: str, feature_name: str, feature_tag: FeatureTag) -> FeatureTag:
        """Creates a FeatureTag.

        :param table_name: str
        :param feature_name: str
        :param feature_tag: :class:`FeatureTag`

        :returns: :class:`FeatureTag`
        """

        body = feature_tag.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.0/feature-store/feature-tables/{table_name}/features/{feature_name}/tags",
            body=body,
            headers=headers,
        )
        return FeatureTag.from_dict(res)

    def delete_feature_tag(self, table_name: str, feature_name: str, key: str):
        """Deletes a FeatureTag.

        :param table_name: str
          The name of the feature table.
        :param feature_name: str
          The name of the feature within the feature table.
        :param key: str
          The key of the tag to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE",
            f"/api/2.0/feature-store/feature-tables/{table_name}/features/{feature_name}/tags/{key}",
            headers=headers,
        )

    def get_feature_lineage(self, table_name: str, feature_name: str) -> FeatureLineage:
        """Get Feature Lineage.

        :param table_name: str
          The full name of the feature table in Unity Catalog.
        :param feature_name: str
          The name of the feature.

        :returns: :class:`FeatureLineage`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/feature-store/feature-tables/{table_name}/features/{feature_name}/lineage",
            headers=headers,
        )
        return FeatureLineage.from_dict(res)

    def get_feature_tag(self, table_name: str, feature_name: str, key: str) -> FeatureTag:
        """Gets a FeatureTag.

        :param table_name: str
        :param feature_name: str
        :param key: str

        :returns: :class:`FeatureTag`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/feature-store/feature-tables/{table_name}/features/{feature_name}/tags/{key}",
            headers=headers,
        )
        return FeatureTag.from_dict(res)

    def list_feature_tags(
        self, table_name: str, feature_name: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[FeatureTag]:
        """Lists FeatureTags.

        :param table_name: str
        :param feature_name: str
        :param page_size: int (optional)
          The maximum number of results to return.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous query.

        :returns: Iterator over :class:`FeatureTag`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.0/feature-store/feature-tables/{table_name}/features/{feature_name}/tags",
                query=query,
                headers=headers,
            )
            if "feature_tags" in json:
                for v in json["feature_tags"]:
                    yield FeatureTag.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_feature_tag(
        self,
        table_name: str,
        feature_name: str,
        key: str,
        feature_tag: FeatureTag,
        *,
        update_mask: Optional[str] = None,
    ) -> FeatureTag:
        """Updates a FeatureTag.

        :param table_name: str
        :param feature_name: str
        :param key: str
        :param feature_tag: :class:`FeatureTag`
        :param update_mask: str (optional)
          The list of fields to update.

        :returns: :class:`FeatureTag`
        """

        body = feature_tag.as_dict()
        query = {}
        if update_mask is not None:
            query["update_mask"] = update_mask
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH",
            f"/api/2.0/feature-store/feature-tables/{table_name}/features/{feature_name}/tags/{key}",
            query=query,
            body=body,
            headers=headers,
        )
        return FeatureTag.from_dict(res)


class ModelRegistryAPI:
    """Note: This API reference documents APIs for the Workspace Model Registry. Databricks recommends using
    [Models in Unity Catalog](/api/workspace/registeredmodels) instead. Models in Unity Catalog provides
    centralized model governance, cross-workspace access, lineage, and deployment. Workspace Model Registry
    will be deprecated in the future.

    The Workspace Model Registry is a centralized model repository and a UI and set of APIs that enable you to
    manage the full lifecycle of MLflow Models."""

    def __init__(self, api_client):
        self._api = api_client

    def approve_transition_request(
        self, name: str, version: str, stage: str, archive_existing_versions: bool, *, comment: Optional[str] = None
    ) -> ApproveTransitionRequestResponse:
        """Approves a model version stage transition request.

        :param name: str
          Name of the model.
        :param version: str
          Version of the model.
        :param stage: str
          Target stage of the transition. Valid values are:

          * `None`: The initial stage of a model version.

          * `Staging`: Staging or pre-production stage.

          * `Production`: Production stage.

          * `Archived`: Archived stage.
        :param archive_existing_versions: bool
          Specifies whether to archive all current model versions in the target stage.
        :param comment: str (optional)
          User-provided comment on the action.

        :returns: :class:`ApproveTransitionRequestResponse`
        """

        body = {}
        if archive_existing_versions is not None:
            body["archive_existing_versions"] = archive_existing_versions
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if stage is not None:
            body["stage"] = stage
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/transition-requests/approve", body=body, headers=headers)
        return ApproveTransitionRequestResponse.from_dict(res)

    def create_comment(self, name: str, version: str, comment: str) -> CreateCommentResponse:
        """Posts a comment on a model version. A comment can be submitted either by a user or programmatically to
        display relevant information about the model. For example, test results or deployment errors.

        :param name: str
          Name of the model.
        :param version: str
          Version of the model.
        :param comment: str
          User-provided comment on the action.

        :returns: :class:`CreateCommentResponse`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/comments/create", body=body, headers=headers)
        return CreateCommentResponse.from_dict(res)

    def create_model(
        self, name: str, *, description: Optional[str] = None, tags: Optional[List[ModelTag]] = None
    ) -> CreateModelResponse:
        """Creates a new registered model with the name specified in the request body. Throws
        `RESOURCE_ALREADY_EXISTS` if a registered model with the given name exists.

        :param name: str
          Register models under this name
        :param description: str (optional)
          Optional description for registered model.
        :param tags: List[:class:`ModelTag`] (optional)
          Additional metadata for registered model.

        :returns: :class:`CreateModelResponse`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/registered-models/create", body=body, headers=headers)
        return CreateModelResponse.from_dict(res)

    def create_model_version(
        self,
        name: str,
        source: str,
        *,
        description: Optional[str] = None,
        run_id: Optional[str] = None,
        run_link: Optional[str] = None,
        tags: Optional[List[ModelVersionTag]] = None,
    ) -> CreateModelVersionResponse:
        """Creates a model version.

        :param name: str
          Register model under this name
        :param source: str
          URI indicating the location of the model artifacts.
        :param description: str (optional)
          Optional description for model version.
        :param run_id: str (optional)
          MLflow run ID for correlation, if `source` was generated by an experiment run in MLflow tracking
          server
        :param run_link: str (optional)
          MLflow run link - this is the exact link of the run that generated this model version, potentially
          hosted at another instance of MLflow.
        :param tags: List[:class:`ModelVersionTag`] (optional)
          Additional metadata for model version.

        :returns: :class:`CreateModelVersionResponse`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        if run_id is not None:
            body["run_id"] = run_id
        if run_link is not None:
            body["run_link"] = run_link
        if source is not None:
            body["source"] = source
        if tags is not None:
            body["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/model-versions/create", body=body, headers=headers)
        return CreateModelVersionResponse.from_dict(res)

    def create_transition_request(
        self, name: str, version: str, stage: str, *, comment: Optional[str] = None
    ) -> CreateTransitionRequestResponse:
        """Creates a model version stage transition request.

        :param name: str
          Name of the model.
        :param version: str
          Version of the model.
        :param stage: str
          Target stage of the transition. Valid values are:

          * `None`: The initial stage of a model version.

          * `Staging`: Staging or pre-production stage.

          * `Production`: Production stage.

          * `Archived`: Archived stage.
        :param comment: str (optional)
          User-provided comment on the action.

        :returns: :class:`CreateTransitionRequestResponse`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if stage is not None:
            body["stage"] = stage
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/transition-requests/create", body=body, headers=headers)
        return CreateTransitionRequestResponse.from_dict(res)

    def create_webhook(
        self,
        events: List[RegistryWebhookEvent],
        *,
        description: Optional[str] = None,
        http_url_spec: Optional[HttpUrlSpec] = None,
        job_spec: Optional[JobSpec] = None,
        model_name: Optional[str] = None,
        status: Optional[RegistryWebhookStatus] = None,
    ) -> CreateWebhookResponse:
        """**NOTE:** This endpoint is in Public Preview. Creates a registry webhook.

        :param events: List[:class:`RegistryWebhookEvent`]
          Events that can trigger a registry webhook: * `MODEL_VERSION_CREATED`: A new model version was
          created for the associated model.

          * `MODEL_VERSION_TRANSITIONED_STAGE`: A model version’s stage was changed.

          * `TRANSITION_REQUEST_CREATED`: A user requested a model version’s stage be transitioned.

          * `COMMENT_CREATED`: A user wrote a comment on a registered model.

          * `REGISTERED_MODEL_CREATED`: A new registered model was created. This event type can only be
          specified for a registry-wide webhook, which can be created by not specifying a model name in the
          create request.

          * `MODEL_VERSION_TAG_SET`: A user set a tag on the model version.

          * `MODEL_VERSION_TRANSITIONED_TO_STAGING`: A model version was transitioned to staging.

          * `MODEL_VERSION_TRANSITIONED_TO_PRODUCTION`: A model version was transitioned to production.

          * `MODEL_VERSION_TRANSITIONED_TO_ARCHIVED`: A model version was archived.

          * `TRANSITION_REQUEST_TO_STAGING_CREATED`: A user requested a model version be transitioned to
          staging.

          * `TRANSITION_REQUEST_TO_PRODUCTION_CREATED`: A user requested a model version be transitioned to
          production.

          * `TRANSITION_REQUEST_TO_ARCHIVED_CREATED`: A user requested a model version be archived.
        :param description: str (optional)
          User-specified description for the webhook.
        :param http_url_spec: :class:`HttpUrlSpec` (optional)
          External HTTPS URL called on event trigger (by using a POST request).
        :param job_spec: :class:`JobSpec` (optional)
          ID of the job that the webhook runs.
        :param model_name: str (optional)
          If model name is not specified, a registry-wide webhook is created that listens for the specified
          events across all versions of all registered models.
        :param status: :class:`RegistryWebhookStatus` (optional)
          Enable or disable triggering the webhook, or put the webhook into test mode. The default is
          `ACTIVE`: * `ACTIVE`: Webhook is triggered when an associated event happens.

          * `DISABLED`: Webhook is not triggered.

          * `TEST_MODE`: Webhook can be triggered through the test endpoint, but is not triggered on a real
          event.

        :returns: :class:`CreateWebhookResponse`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if events is not None:
            body["events"] = [v.value for v in events]
        if http_url_spec is not None:
            body["http_url_spec"] = http_url_spec.as_dict()
        if job_spec is not None:
            body["job_spec"] = job_spec.as_dict()
        if model_name is not None:
            body["model_name"] = model_name
        if status is not None:
            body["status"] = status.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/registry-webhooks/create", body=body, headers=headers)
        return CreateWebhookResponse.from_dict(res)

    def delete_comment(self, id: str):
        """Deletes a comment on a model version.

        :param id: str
          Unique identifier of an activity


        """

        query = {}
        if id is not None:
            query["id"] = id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/mlflow/comments/delete", query=query, headers=headers)

    def delete_model(self, name: str):
        """Deletes a registered model.

        :param name: str
          Registered model unique name identifier.


        """

        query = {}
        if name is not None:
            query["name"] = name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/mlflow/registered-models/delete", query=query, headers=headers)

    def delete_model_tag(self, name: str, key: str):
        """Deletes the tag for a registered model.

        :param name: str
          Name of the registered model that the tag was logged under.
        :param key: str
          Name of the tag. The name must be an exact match; wild-card deletion is not supported. Maximum size
          is 250 bytes.


        """

        query = {}
        if key is not None:
            query["key"] = key
        if name is not None:
            query["name"] = name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/mlflow/registered-models/delete-tag", query=query, headers=headers)

    def delete_model_version(self, name: str, version: str):
        """Deletes a model version.

        :param name: str
          Name of the registered model
        :param version: str
          Model version number


        """

        query = {}
        if name is not None:
            query["name"] = name
        if version is not None:
            query["version"] = version
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/mlflow/model-versions/delete", query=query, headers=headers)

    def delete_model_version_tag(self, name: str, version: str, key: str):
        """Deletes a model version tag.

        :param name: str
          Name of the registered model that the tag was logged under.
        :param version: str
          Model version number that the tag was logged under.
        :param key: str
          Name of the tag. The name must be an exact match; wild-card deletion is not supported. Maximum size
          is 250 bytes.


        """

        query = {}
        if key is not None:
            query["key"] = key
        if name is not None:
            query["name"] = name
        if version is not None:
            query["version"] = version
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/mlflow/model-versions/delete-tag", query=query, headers=headers)

    def delete_transition_request(
        self, name: str, version: str, stage: str, creator: str, *, comment: Optional[str] = None
    ) -> DeleteTransitionRequestResponse:
        """Cancels a model version stage transition request.

        :param name: str
          Name of the model.
        :param version: str
          Version of the model.
        :param stage: str
          Target stage of the transition request. Valid values are:

          * `None`: The initial stage of a model version.

          * `Staging`: Staging or pre-production stage.

          * `Production`: Production stage.

          * `Archived`: Archived stage.
        :param creator: str
          Username of the user who created this request. Of the transition requests matching the specified
          details, only the one transition created by this user will be deleted.
        :param comment: str (optional)
          User-provided comment on the action.

        :returns: :class:`DeleteTransitionRequestResponse`
        """

        query = {}
        if comment is not None:
            query["comment"] = comment
        if creator is not None:
            query["creator"] = creator
        if name is not None:
            query["name"] = name
        if stage is not None:
            query["stage"] = stage
        if version is not None:
            query["version"] = version
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("DELETE", "/api/2.0/mlflow/transition-requests/delete", query=query, headers=headers)
        return DeleteTransitionRequestResponse.from_dict(res)

    def delete_webhook(self, id: str):
        """**NOTE:** This endpoint is in Public Preview. Deletes a registry webhook.

        :param id: str
          Webhook ID required to delete a registry webhook.


        """

        query = {}
        if id is not None:
            query["id"] = id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", "/api/2.0/mlflow/registry-webhooks/delete", query=query, headers=headers)

    def get_latest_versions(self, name: str, *, stages: Optional[List[str]] = None) -> Iterator[ModelVersion]:
        """Gets the latest version of a registered model.

        :param name: str
          Registered model unique name identifier.
        :param stages: List[str] (optional)
          List of stages.

        :returns: Iterator over :class:`ModelVersion`
        """

        body = {}
        if name is not None:
            body["name"] = name
        if stages is not None:
            body["stages"] = [v for v in stages]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("POST", "/api/2.0/mlflow/registered-models/get-latest-versions", body=body, headers=headers)
        parsed = GetLatestVersionsResponse.from_dict(json).model_versions
        return parsed if parsed is not None else []

    def get_model(self, name: str) -> GetModelResponse:
        """Get the details of a model. This is a Databricks workspace version of the [MLflow endpoint] that also
        returns the model's Databricks workspace ID and the permission level of the requesting user on the
        model.

        [MLflow endpoint]: https://www.mlflow.org/docs/latest/rest-api.html#get-registeredmodel

        :param name: str
          Registered model unique name identifier.

        :returns: :class:`GetModelResponse`
        """

        query = {}
        if name is not None:
            query["name"] = name
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/mlflow/databricks/registered-models/get", query=query, headers=headers)
        return GetModelResponse.from_dict(res)

    def get_model_version(self, name: str, version: str) -> GetModelVersionResponse:
        """Get a model version.

        :param name: str
          Name of the registered model
        :param version: str
          Model version number

        :returns: :class:`GetModelVersionResponse`
        """

        query = {}
        if name is not None:
            query["name"] = name
        if version is not None:
            query["version"] = version
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/mlflow/model-versions/get", query=query, headers=headers)
        return GetModelVersionResponse.from_dict(res)

    def get_model_version_download_uri(self, name: str, version: str) -> GetModelVersionDownloadUriResponse:
        """Gets a URI to download the model version.

        :param name: str
          Name of the registered model
        :param version: str
          Model version number

        :returns: :class:`GetModelVersionDownloadUriResponse`
        """

        query = {}
        if name is not None:
            query["name"] = name
        if version is not None:
            query["version"] = version
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/mlflow/model-versions/get-download-uri", query=query, headers=headers)
        return GetModelVersionDownloadUriResponse.from_dict(res)

    def get_permission_levels(self, registered_model_id: str) -> GetRegisteredModelPermissionLevelsResponse:
        """Gets the permission levels that a user can have on an object.

        :param registered_model_id: str
          The registered model for which to get or manage permissions.

        :returns: :class:`GetRegisteredModelPermissionLevelsResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/permissions/registered-models/{registered_model_id}/permissionLevels", headers=headers
        )
        return GetRegisteredModelPermissionLevelsResponse.from_dict(res)

    def get_permissions(self, registered_model_id: str) -> RegisteredModelPermissions:
        """Gets the permissions of a registered model. Registered models can inherit permissions from their root
        object.

        :param registered_model_id: str
          The registered model for which to get or manage permissions.

        :returns: :class:`RegisteredModelPermissions`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/permissions/registered-models/{registered_model_id}", headers=headers)
        return RegisteredModelPermissions.from_dict(res)

    def list_models(self, *, max_results: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Model]:
        """Lists all available registered models, up to the limit specified in __max_results__.

        :param max_results: int (optional)
          Maximum number of registered models desired. Max threshold is 1000.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous query.

        :returns: Iterator over :class:`Model`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/registered-models/list", query=query, headers=headers)
            if "registered_models" in json:
                for v in json["registered_models"]:
                    yield Model.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_transition_requests(self, name: str, version: str) -> Iterator[Activity]:
        """Gets a list of all open stage transition requests for the model version.

        :param name: str
          Name of the registered model.
        :param version: str
          Version of the model.

        :returns: Iterator over :class:`Activity`
        """

        query = {}
        if name is not None:
            query["name"] = name
        if version is not None:
            query["version"] = version
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/mlflow/transition-requests/list", query=query, headers=headers)
        parsed = ListTransitionRequestsResponse.from_dict(json).requests
        return parsed if parsed is not None else []

    def list_webhooks(
        self,
        *,
        events: Optional[List[RegistryWebhookEvent]] = None,
        max_results: Optional[int] = None,
        model_name: Optional[str] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[RegistryWebhook]:
        """**NOTE:** This endpoint is in Public Preview. Lists all registry webhooks.

        :param events: List[:class:`RegistryWebhookEvent`] (optional)
          Events that trigger the webhook. * `MODEL_VERSION_CREATED`: A new model version was created for the
          associated model.

          * `MODEL_VERSION_TRANSITIONED_STAGE`: A model version’s stage was changed.

          * `TRANSITION_REQUEST_CREATED`: A user requested a model version’s stage be transitioned.

          * `COMMENT_CREATED`: A user wrote a comment on a registered model.

          * `REGISTERED_MODEL_CREATED`: A new registered model was created. This event type can only be
          specified for a registry-wide webhook, which can be created by not specifying a model name in the
          create request.

          * `MODEL_VERSION_TAG_SET`: A user set a tag on the model version.

          * `MODEL_VERSION_TRANSITIONED_TO_STAGING`: A model version was transitioned to staging.

          * `MODEL_VERSION_TRANSITIONED_TO_PRODUCTION`: A model version was transitioned to production.

          * `MODEL_VERSION_TRANSITIONED_TO_ARCHIVED`: A model version was archived.

          * `TRANSITION_REQUEST_TO_STAGING_CREATED`: A user requested a model version be transitioned to
          staging.

          * `TRANSITION_REQUEST_TO_PRODUCTION_CREATED`: A user requested a model version be transitioned to
          production.

          * `TRANSITION_REQUEST_TO_ARCHIVED_CREATED`: A user requested a model version be archived.

          If `events` is specified, any webhook with one or more of the specified trigger events is included
          in the output. If `events` is not specified, webhooks of all event types are included in the output.
        :param max_results: int (optional)
        :param model_name: str (optional)
          Registered model name If not specified, all webhooks associated with the specified events are
          listed, regardless of their associated model.
        :param page_token: str (optional)
          Token indicating the page of artifact results to fetch

        :returns: Iterator over :class:`RegistryWebhook`
        """

        query = {}
        if events is not None:
            query["events"] = [v.value for v in events]
        if max_results is not None:
            query["max_results"] = max_results
        if model_name is not None:
            query["model_name"] = model_name
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/registry-webhooks/list", query=query, headers=headers)
            if "webhooks" in json:
                for v in json["webhooks"]:
                    yield RegistryWebhook.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def reject_transition_request(
        self, name: str, version: str, stage: str, *, comment: Optional[str] = None
    ) -> RejectTransitionRequestResponse:
        """Rejects a model version stage transition request.

        :param name: str
          Name of the model.
        :param version: str
          Version of the model.
        :param stage: str
          Target stage of the transition. Valid values are:

          * `None`: The initial stage of a model version.

          * `Staging`: Staging or pre-production stage.

          * `Production`: Production stage.

          * `Archived`: Archived stage.
        :param comment: str (optional)
          User-provided comment on the action.

        :returns: :class:`RejectTransitionRequestResponse`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if stage is not None:
            body["stage"] = stage
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/transition-requests/reject", body=body, headers=headers)
        return RejectTransitionRequestResponse.from_dict(res)

    def rename_model(self, name: str, *, new_name: Optional[str] = None) -> RenameModelResponse:
        """Renames a registered model.

        :param name: str
          Registered model unique name identifier.
        :param new_name: str (optional)
          If provided, updates the name for this `registered_model`.

        :returns: :class:`RenameModelResponse`
        """

        body = {}
        if name is not None:
            body["name"] = name
        if new_name is not None:
            body["new_name"] = new_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/registered-models/rename", body=body, headers=headers)
        return RenameModelResponse.from_dict(res)

    def search_model_versions(
        self,
        *,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[ModelVersion]:
        """Searches for specific model versions based on the supplied __filter__.

        :param filter: str (optional)
          String filter condition, like "name='my-model-name'". Must be a single boolean condition, with
          string values wrapped in single quotes.
        :param max_results: int (optional)
          Maximum number of models desired. Max threshold is 10K.
        :param order_by: List[str] (optional)
          List of columns to be ordered by including model name, version, stage with an optional "DESC" or
          "ASC" annotation, where "ASC" is the default. Tiebreaks are done by latest stage transition
          timestamp, followed by name ASC, followed by version DESC.
        :param page_token: str (optional)
          Pagination token to go to next page based on previous search query.

        :returns: Iterator over :class:`ModelVersion`
        """

        query = {}
        if filter is not None:
            query["filter"] = filter
        if max_results is not None:
            query["max_results"] = max_results
        if order_by is not None:
            query["order_by"] = [v for v in order_by]
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/model-versions/search", query=query, headers=headers)
            if "model_versions" in json:
                for v in json["model_versions"]:
                    yield ModelVersion.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def search_models(
        self,
        *,
        filter: Optional[str] = None,
        max_results: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        page_token: Optional[str] = None,
    ) -> Iterator[Model]:
        """Search for registered models based on the specified __filter__.

        :param filter: str (optional)
          String filter condition, like "name LIKE 'my-model-name'". Interpreted in the backend automatically
          as "name LIKE '%my-model-name%'". Single boolean condition, with string values wrapped in single
          quotes.
        :param max_results: int (optional)
          Maximum number of models desired. Default is 100. Max threshold is 1000.
        :param order_by: List[str] (optional)
          List of columns for ordering search results, which can include model name and last updated timestamp
          with an optional "DESC" or "ASC" annotation, where "ASC" is the default. Tiebreaks are done by model
          name ASC.
        :param page_token: str (optional)
          Pagination token to go to the next page based on a previous search query.

        :returns: Iterator over :class:`Model`
        """

        query = {}
        if filter is not None:
            query["filter"] = filter
        if max_results is not None:
            query["max_results"] = max_results
        if order_by is not None:
            query["order_by"] = [v for v in order_by]
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/mlflow/registered-models/search", query=query, headers=headers)
            if "registered_models" in json:
                for v in json["registered_models"]:
                    yield Model.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def set_model_tag(self, name: str, key: str, value: str):
        """Sets a tag on a registered model.

        :param name: str
          Unique name of the model.
        :param key: str
          Name of the tag. Maximum size depends on storage backend. If a tag with this name already exists,
          its preexisting value will be replaced by the specified `value`. All storage backends are guaranteed
          to support key values up to 250 bytes in size.
        :param value: str
          String value of the tag being logged. Maximum size depends on storage backend. All storage backends
          are guaranteed to support key values up to 5000 bytes in size.


        """

        body = {}
        if key is not None:
            body["key"] = key
        if name is not None:
            body["name"] = name
        if value is not None:
            body["value"] = value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/registered-models/set-tag", body=body, headers=headers)

    def set_model_version_tag(self, name: str, version: str, key: str, value: str):
        """Sets a model version tag.

        :param name: str
          Unique name of the model.
        :param version: str
          Model version number.
        :param key: str
          Name of the tag. Maximum size depends on storage backend. If a tag with this name already exists,
          its preexisting value will be replaced by the specified `value`. All storage backends are guaranteed
          to support key values up to 250 bytes in size.
        :param value: str
          String value of the tag being logged. Maximum size depends on storage backend. All storage backends
          are guaranteed to support key values up to 5000 bytes in size.


        """

        body = {}
        if key is not None:
            body["key"] = key
        if name is not None:
            body["name"] = name
        if value is not None:
            body["value"] = value
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/mlflow/model-versions/set-tag", body=body, headers=headers)

    def set_permissions(
        self,
        registered_model_id: str,
        *,
        access_control_list: Optional[List[RegisteredModelAccessControlRequest]] = None,
    ) -> RegisteredModelPermissions:
        """Sets permissions on an object, replacing existing permissions if they exist. Deletes all direct
        permissions if none are specified. Objects can inherit permissions from their root object.

        :param registered_model_id: str
          The registered model for which to get or manage permissions.
        :param access_control_list: List[:class:`RegisteredModelAccessControlRequest`] (optional)

        :returns: :class:`RegisteredModelPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT", f"/api/2.0/permissions/registered-models/{registered_model_id}", body=body, headers=headers
        )
        return RegisteredModelPermissions.from_dict(res)

    def test_registry_webhook(
        self, id: str, *, event: Optional[RegistryWebhookEvent] = None
    ) -> TestRegistryWebhookResponse:
        """**NOTE:** This endpoint is in Public Preview. Tests a registry webhook.

        :param id: str
          Webhook ID
        :param event: :class:`RegistryWebhookEvent` (optional)
          If `event` is specified, the test trigger uses the specified event. If `event` is not specified, the
          test trigger uses a randomly chosen event associated with the webhook.

        :returns: :class:`TestRegistryWebhookResponse`
        """

        body = {}
        if event is not None:
            body["event"] = event.value
        if id is not None:
            body["id"] = id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/mlflow/registry-webhooks/test", body=body, headers=headers)
        return TestRegistryWebhookResponse.from_dict(res)

    def transition_stage(
        self, name: str, version: str, stage: str, archive_existing_versions: bool, *, comment: Optional[str] = None
    ) -> TransitionStageResponse:
        """Transition a model version's stage. This is a Databricks workspace version of the [MLflow endpoint]
        that also accepts a comment associated with the transition to be recorded.

        [MLflow endpoint]: https://www.mlflow.org/docs/latest/rest-api.html#transition-modelversion-stage

        :param name: str
          Name of the model.
        :param version: str
          Version of the model.
        :param stage: str
          Target stage of the transition. Valid values are:

          * `None`: The initial stage of a model version.

          * `Staging`: Staging or pre-production stage.

          * `Production`: Production stage.

          * `Archived`: Archived stage.
        :param archive_existing_versions: bool
          Specifies whether to archive all current model versions in the target stage.
        :param comment: str (optional)
          User-provided comment on the action.

        :returns: :class:`TransitionStageResponse`
        """

        body = {}
        if archive_existing_versions is not None:
            body["archive_existing_versions"] = archive_existing_versions
        if comment is not None:
            body["comment"] = comment
        if name is not None:
            body["name"] = name
        if stage is not None:
            body["stage"] = stage
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", "/api/2.0/mlflow/databricks/model-versions/transition-stage", body=body, headers=headers
        )
        return TransitionStageResponse.from_dict(res)

    def update_comment(self, id: str, comment: str) -> UpdateCommentResponse:
        """Post an edit to a comment on a model version.

        :param id: str
          Unique identifier of an activity
        :param comment: str
          User-provided comment on the action.

        :returns: :class:`UpdateCommentResponse`
        """

        body = {}
        if comment is not None:
            body["comment"] = comment
        if id is not None:
            body["id"] = id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", "/api/2.0/mlflow/comments/update", body=body, headers=headers)
        return UpdateCommentResponse.from_dict(res)

    def update_model(self, name: str, *, description: Optional[str] = None) -> UpdateModelResponse:
        """Updates a registered model.

        :param name: str
          Registered model unique name identifier.
        :param description: str (optional)
          If provided, updates the description for this `registered_model`.

        :returns: :class:`UpdateModelResponse`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", "/api/2.0/mlflow/registered-models/update", body=body, headers=headers)
        return UpdateModelResponse.from_dict(res)

    def update_model_version(
        self, name: str, version: str, *, description: Optional[str] = None
    ) -> UpdateModelVersionResponse:
        """Updates the model version.

        :param name: str
          Name of the registered model
        :param version: str
          Model version number
        :param description: str (optional)
          If provided, updates the description for this `registered_model`.

        :returns: :class:`UpdateModelVersionResponse`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if name is not None:
            body["name"] = name
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", "/api/2.0/mlflow/model-versions/update", body=body, headers=headers)
        return UpdateModelVersionResponse.from_dict(res)

    def update_permissions(
        self,
        registered_model_id: str,
        *,
        access_control_list: Optional[List[RegisteredModelAccessControlRequest]] = None,
    ) -> RegisteredModelPermissions:
        """Updates the permissions on a registered model. Registered models can inherit permissions from their
        root object.

        :param registered_model_id: str
          The registered model for which to get or manage permissions.
        :param access_control_list: List[:class:`RegisteredModelAccessControlRequest`] (optional)

        :returns: :class:`RegisteredModelPermissions`
        """

        body = {}
        if access_control_list is not None:
            body["access_control_list"] = [v.as_dict() for v in access_control_list]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.0/permissions/registered-models/{registered_model_id}", body=body, headers=headers
        )
        return RegisteredModelPermissions.from_dict(res)

    def update_webhook(
        self,
        id: str,
        *,
        description: Optional[str] = None,
        events: Optional[List[RegistryWebhookEvent]] = None,
        http_url_spec: Optional[HttpUrlSpec] = None,
        job_spec: Optional[JobSpec] = None,
        status: Optional[RegistryWebhookStatus] = None,
    ) -> UpdateWebhookResponse:
        """**NOTE:** This endpoint is in Public Preview. Updates a registry webhook.

        :param id: str
          Webhook ID
        :param description: str (optional)
          User-specified description for the webhook.
        :param events: List[:class:`RegistryWebhookEvent`] (optional)
          Events that can trigger a registry webhook: * `MODEL_VERSION_CREATED`: A new model version was
          created for the associated model.

          * `MODEL_VERSION_TRANSITIONED_STAGE`: A model version’s stage was changed.

          * `TRANSITION_REQUEST_CREATED`: A user requested a model version’s stage be transitioned.

          * `COMMENT_CREATED`: A user wrote a comment on a registered model.

          * `REGISTERED_MODEL_CREATED`: A new registered model was created. This event type can only be
          specified for a registry-wide webhook, which can be created by not specifying a model name in the
          create request.

          * `MODEL_VERSION_TAG_SET`: A user set a tag on the model version.

          * `MODEL_VERSION_TRANSITIONED_TO_STAGING`: A model version was transitioned to staging.

          * `MODEL_VERSION_TRANSITIONED_TO_PRODUCTION`: A model version was transitioned to production.

          * `MODEL_VERSION_TRANSITIONED_TO_ARCHIVED`: A model version was archived.

          * `TRANSITION_REQUEST_TO_STAGING_CREATED`: A user requested a model version be transitioned to
          staging.

          * `TRANSITION_REQUEST_TO_PRODUCTION_CREATED`: A user requested a model version be transitioned to
          production.

          * `TRANSITION_REQUEST_TO_ARCHIVED_CREATED`: A user requested a model version be archived.
        :param http_url_spec: :class:`HttpUrlSpec` (optional)
        :param job_spec: :class:`JobSpec` (optional)
        :param status: :class:`RegistryWebhookStatus` (optional)

        :returns: :class:`UpdateWebhookResponse`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if events is not None:
            body["events"] = [v.value for v in events]
        if http_url_spec is not None:
            body["http_url_spec"] = http_url_spec.as_dict()
        if id is not None:
            body["id"] = id
        if job_spec is not None:
            body["job_spec"] = job_spec.as_dict()
        if status is not None:
            body["status"] = status.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", "/api/2.0/mlflow/registry-webhooks/update", body=body, headers=headers)
        return UpdateWebhookResponse.from_dict(res)
