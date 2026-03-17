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
from databricks.sdk.service import sql
from databricks.sdk.service._internal import (Wait, _enum, _from_dict,
                                              _repeated_dict)

from ..errors import OperationFailed

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AuthorizationDetails:
    grant_rules: Optional[List[AuthorizationDetailsGrantRule]] = None
    """Represents downscoped permission rules with specific access rights. This field is specific to
    `workspace_rule_set` constraint."""

    resource_legacy_acl_path: Optional[str] = None
    """The acl path of the tree store resource resource."""

    resource_name: Optional[str] = None
    """The resource name to which the authorization rule applies. This field is specific to
    `workspace_rule_set` constraint. Format: `workspaces/{workspace_id}/dashboards/{dashboard_id}`"""

    type: Optional[str] = None
    """The type of authorization downscoping policy. Ex: `workspace_rule_set` defines access rules for
    a specific workspace resource"""

    def as_dict(self) -> dict:
        """Serializes the AuthorizationDetails into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.grant_rules:
            body["grant_rules"] = [v.as_dict() for v in self.grant_rules]
        if self.resource_legacy_acl_path is not None:
            body["resource_legacy_acl_path"] = self.resource_legacy_acl_path
        if self.resource_name is not None:
            body["resource_name"] = self.resource_name
        if self.type is not None:
            body["type"] = self.type
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AuthorizationDetails into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.grant_rules:
            body["grant_rules"] = self.grant_rules
        if self.resource_legacy_acl_path is not None:
            body["resource_legacy_acl_path"] = self.resource_legacy_acl_path
        if self.resource_name is not None:
            body["resource_name"] = self.resource_name
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AuthorizationDetails:
        """Deserializes the AuthorizationDetails from a dictionary."""
        return cls(
            grant_rules=_repeated_dict(d, "grant_rules", AuthorizationDetailsGrantRule),
            resource_legacy_acl_path=d.get("resource_legacy_acl_path", None),
            resource_name=d.get("resource_name", None),
            type=d.get("type", None),
        )


@dataclass
class AuthorizationDetailsGrantRule:
    permission_set: Optional[str] = None
    """Permission sets for dashboard are defined in
    iam-common/rbac-common/permission-sets/definitions/TreeStoreBasePermissionSets Ex:
    `permissionSets/dashboard.runner`"""

    def as_dict(self) -> dict:
        """Serializes the AuthorizationDetailsGrantRule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.permission_set is not None:
            body["permission_set"] = self.permission_set
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AuthorizationDetailsGrantRule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.permission_set is not None:
            body["permission_set"] = self.permission_set
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AuthorizationDetailsGrantRule:
        """Deserializes the AuthorizationDetailsGrantRule from a dictionary."""
        return cls(permission_set=d.get("permission_set", None))


@dataclass
class CronSchedule:
    quartz_cron_expression: str
    """A cron expression using quartz syntax. EX: `0 0 8 * * ?` represents everyday at 8am. See [Cron
    Trigger] for details.
    
    [Cron Trigger]: http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html"""

    timezone_id: str
    """A Java timezone id. The schedule will be resolved with respect to this timezone. See [Java
    TimeZone] for details.
    
    [Java TimeZone]: https://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html"""

    def as_dict(self) -> dict:
        """Serializes the CronSchedule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.quartz_cron_expression is not None:
            body["quartz_cron_expression"] = self.quartz_cron_expression
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CronSchedule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.quartz_cron_expression is not None:
            body["quartz_cron_expression"] = self.quartz_cron_expression
        if self.timezone_id is not None:
            body["timezone_id"] = self.timezone_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CronSchedule:
        """Deserializes the CronSchedule from a dictionary."""
        return cls(quartz_cron_expression=d.get("quartz_cron_expression", None), timezone_id=d.get("timezone_id", None))


@dataclass
class Dashboard:
    create_time: Optional[str] = None
    """The timestamp of when the dashboard was created."""

    dashboard_id: Optional[str] = None
    """UUID identifying the dashboard."""

    display_name: Optional[str] = None
    """The display name of the dashboard."""

    etag: Optional[str] = None
    """The etag for the dashboard. Can be optionally provided on updates to ensure that the dashboard
    has not been modified since the last read. This field is excluded in List Dashboards responses."""

    lifecycle_state: Optional[LifecycleState] = None
    """The state of the dashboard resource. Used for tracking trashed status."""

    parent_path: Optional[str] = None
    """The workspace path of the folder containing the dashboard. Includes leading slash and no
    trailing slash. This field is excluded in List Dashboards responses."""

    path: Optional[str] = None
    """The workspace path of the dashboard asset, including the file name. Exported dashboards always
    have the file extension `.lvdash.json`. This field is excluded in List Dashboards responses."""

    serialized_dashboard: Optional[str] = None
    """The contents of the dashboard in serialized string form. This field is excluded in List
    Dashboards responses. Use the [get dashboard API] to retrieve an example response, which
    includes the `serialized_dashboard` field. This field provides the structure of the JSON string
    that represents the dashboard's layout and components.
    
    [get dashboard API]: https://docs.databricks.com/api/workspace/lakeview/get"""

    update_time: Optional[str] = None
    """The timestamp of when the dashboard was last updated by the user. This field is excluded in List
    Dashboards responses."""

    warehouse_id: Optional[str] = None
    """The warehouse ID used to run the dashboard."""

    def as_dict(self) -> dict:
        """Serializes the Dashboard into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.etag is not None:
            body["etag"] = self.etag
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state.value
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.path is not None:
            body["path"] = self.path
        if self.serialized_dashboard is not None:
            body["serialized_dashboard"] = self.serialized_dashboard
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Dashboard into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.etag is not None:
            body["etag"] = self.etag
        if self.lifecycle_state is not None:
            body["lifecycle_state"] = self.lifecycle_state
        if self.parent_path is not None:
            body["parent_path"] = self.parent_path
        if self.path is not None:
            body["path"] = self.path
        if self.serialized_dashboard is not None:
            body["serialized_dashboard"] = self.serialized_dashboard
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Dashboard:
        """Deserializes the Dashboard from a dictionary."""
        return cls(
            create_time=d.get("create_time", None),
            dashboard_id=d.get("dashboard_id", None),
            display_name=d.get("display_name", None),
            etag=d.get("etag", None),
            lifecycle_state=_enum(d, "lifecycle_state", LifecycleState),
            parent_path=d.get("parent_path", None),
            path=d.get("path", None),
            serialized_dashboard=d.get("serialized_dashboard", None),
            update_time=d.get("update_time", None),
            warehouse_id=d.get("warehouse_id", None),
        )


class DashboardView(Enum):

    DASHBOARD_VIEW_BASIC = "DASHBOARD_VIEW_BASIC"


@dataclass
class GenieAttachment:
    """Genie AI Response"""

    attachment_id: Optional[str] = None
    """Attachment ID"""

    query: Optional[GenieQueryAttachment] = None
    """Query Attachment if Genie responds with a SQL query"""

    suggested_questions: Optional[GenieSuggestedQuestionsAttachment] = None
    """Follow-up questions suggested by Genie"""

    text: Optional[TextAttachment] = None
    """Text Attachment if Genie responds with text This also contains the final summary when available."""

    def as_dict(self) -> dict:
        """Serializes the GenieAttachment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attachment_id is not None:
            body["attachment_id"] = self.attachment_id
        if self.query:
            body["query"] = self.query.as_dict()
        if self.suggested_questions:
            body["suggested_questions"] = self.suggested_questions.as_dict()
        if self.text:
            body["text"] = self.text.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieAttachment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attachment_id is not None:
            body["attachment_id"] = self.attachment_id
        if self.query:
            body["query"] = self.query
        if self.suggested_questions:
            body["suggested_questions"] = self.suggested_questions
        if self.text:
            body["text"] = self.text
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieAttachment:
        """Deserializes the GenieAttachment from a dictionary."""
        return cls(
            attachment_id=d.get("attachment_id", None),
            query=_from_dict(d, "query", GenieQueryAttachment),
            suggested_questions=_from_dict(d, "suggested_questions", GenieSuggestedQuestionsAttachment),
            text=_from_dict(d, "text", TextAttachment),
        )


@dataclass
class GenieConversation:
    space_id: str
    """Genie space ID"""

    title: str
    """Conversation title"""

    conversation_id: str
    """Conversation ID"""

    created_timestamp: Optional[int] = None
    """Timestamp when the message was created"""

    id: Optional[str] = None
    """Conversation ID. Legacy identifier, use conversation_id instead"""

    last_updated_timestamp: Optional[int] = None
    """Timestamp when the message was last updated"""

    user_id: Optional[int] = None
    """ID of the user who created the conversation"""

    def as_dict(self) -> dict:
        """Serializes the GenieConversation into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.created_timestamp is not None:
            body["created_timestamp"] = self.created_timestamp
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.space_id is not None:
            body["space_id"] = self.space_id
        if self.title is not None:
            body["title"] = self.title
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieConversation into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.created_timestamp is not None:
            body["created_timestamp"] = self.created_timestamp
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.space_id is not None:
            body["space_id"] = self.space_id
        if self.title is not None:
            body["title"] = self.title
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieConversation:
        """Deserializes the GenieConversation from a dictionary."""
        return cls(
            conversation_id=d.get("conversation_id", None),
            created_timestamp=d.get("created_timestamp", None),
            id=d.get("id", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            space_id=d.get("space_id", None),
            title=d.get("title", None),
            user_id=d.get("user_id", None),
        )


@dataclass
class GenieConversationSummary:
    conversation_id: str

    created_timestamp: Optional[int] = None

    title: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the GenieConversationSummary into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.created_timestamp is not None:
            body["created_timestamp"] = self.created_timestamp
        if self.title is not None:
            body["title"] = self.title
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieConversationSummary into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.created_timestamp is not None:
            body["created_timestamp"] = self.created_timestamp
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieConversationSummary:
        """Deserializes the GenieConversationSummary from a dictionary."""
        return cls(
            conversation_id=d.get("conversation_id", None),
            created_timestamp=d.get("created_timestamp", None),
            title=d.get("title", None),
        )


@dataclass
class GenieFeedback:
    """Feedback containing rating and optional comment"""

    rating: Optional[GenieFeedbackRating] = None
    """The feedback rating"""

    def as_dict(self) -> dict:
        """Serializes the GenieFeedback into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.rating is not None:
            body["rating"] = self.rating.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieFeedback into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.rating is not None:
            body["rating"] = self.rating
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieFeedback:
        """Deserializes the GenieFeedback from a dictionary."""
        return cls(rating=_enum(d, "rating", GenieFeedbackRating))


class GenieFeedbackRating(Enum):
    """Feedback rating for Genie messages"""

    NEGATIVE = "NEGATIVE"
    NONE = "NONE"
    POSITIVE = "POSITIVE"


@dataclass
class GenieGenerateDownloadFullQueryResultResponse:
    download_id: Optional[str] = None
    """Download ID. Use this ID to track the download request in subsequent polling calls"""

    download_id_signature: Optional[str] = None
    """JWT signature for the download_id to ensure secure access to query results"""

    def as_dict(self) -> dict:
        """Serializes the GenieGenerateDownloadFullQueryResultResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.download_id is not None:
            body["download_id"] = self.download_id
        if self.download_id_signature is not None:
            body["download_id_signature"] = self.download_id_signature
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieGenerateDownloadFullQueryResultResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.download_id is not None:
            body["download_id"] = self.download_id
        if self.download_id_signature is not None:
            body["download_id_signature"] = self.download_id_signature
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieGenerateDownloadFullQueryResultResponse:
        """Deserializes the GenieGenerateDownloadFullQueryResultResponse from a dictionary."""
        return cls(download_id=d.get("download_id", None), download_id_signature=d.get("download_id_signature", None))


@dataclass
class GenieGetDownloadFullQueryResultResponse:
    statement_response: Optional[sql.StatementResponse] = None
    """SQL Statement Execution response. See [Get status, manifest, and result first
    chunk](:method:statementexecution/getstatement) for more details."""

    def as_dict(self) -> dict:
        """Serializes the GenieGetDownloadFullQueryResultResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.statement_response:
            body["statement_response"] = self.statement_response.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieGetDownloadFullQueryResultResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.statement_response:
            body["statement_response"] = self.statement_response
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieGetDownloadFullQueryResultResponse:
        """Deserializes the GenieGetDownloadFullQueryResultResponse from a dictionary."""
        return cls(statement_response=_from_dict(d, "statement_response", sql.StatementResponse))


@dataclass
class GenieGetMessageQueryResultResponse:
    statement_response: Optional[sql.StatementResponse] = None
    """SQL Statement Execution response. See [Get status, manifest, and result first
    chunk](:method:statementexecution/getstatement) for more details."""

    def as_dict(self) -> dict:
        """Serializes the GenieGetMessageQueryResultResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.statement_response:
            body["statement_response"] = self.statement_response.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieGetMessageQueryResultResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.statement_response:
            body["statement_response"] = self.statement_response
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieGetMessageQueryResultResponse:
        """Deserializes the GenieGetMessageQueryResultResponse from a dictionary."""
        return cls(statement_response=_from_dict(d, "statement_response", sql.StatementResponse))


@dataclass
class GenieListConversationMessagesResponse:
    messages: Optional[List[GenieMessage]] = None
    """List of messages in the conversation."""

    next_page_token: Optional[str] = None
    """The token to use for retrieving the next page of results."""

    def as_dict(self) -> dict:
        """Serializes the GenieListConversationMessagesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.messages:
            body["messages"] = [v.as_dict() for v in self.messages]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieListConversationMessagesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.messages:
            body["messages"] = self.messages
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieListConversationMessagesResponse:
        """Deserializes the GenieListConversationMessagesResponse from a dictionary."""
        return cls(messages=_repeated_dict(d, "messages", GenieMessage), next_page_token=d.get("next_page_token", None))


@dataclass
class GenieListConversationsResponse:
    conversations: Optional[List[GenieConversationSummary]] = None
    """List of conversations in the Genie space"""

    next_page_token: Optional[str] = None
    """Token to get the next page of results"""

    def as_dict(self) -> dict:
        """Serializes the GenieListConversationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.conversations:
            body["conversations"] = [v.as_dict() for v in self.conversations]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieListConversationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.conversations:
            body["conversations"] = self.conversations
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieListConversationsResponse:
        """Deserializes the GenieListConversationsResponse from a dictionary."""
        return cls(
            conversations=_repeated_dict(d, "conversations", GenieConversationSummary),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class GenieListSpacesResponse:
    next_page_token: Optional[str] = None
    """Token to get the next page of results"""

    spaces: Optional[List[GenieSpace]] = None
    """List of Genie spaces"""

    def as_dict(self) -> dict:
        """Serializes the GenieListSpacesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.spaces:
            body["spaces"] = [v.as_dict() for v in self.spaces]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieListSpacesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.spaces:
            body["spaces"] = self.spaces
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieListSpacesResponse:
        """Deserializes the GenieListSpacesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), spaces=_repeated_dict(d, "spaces", GenieSpace))


@dataclass
class GenieMessage:
    space_id: str
    """Genie space ID"""

    conversation_id: str
    """Conversation ID"""

    content: str
    """User message content"""

    message_id: str
    """Message ID"""

    attachments: Optional[List[GenieAttachment]] = None
    """AI-generated response to the message"""

    created_timestamp: Optional[int] = None
    """Timestamp when the message was created"""

    error: Optional[MessageError] = None
    """Error message if Genie failed to respond to the message"""

    feedback: Optional[GenieFeedback] = None
    """User feedback for the message if provided"""

    id: Optional[str] = None
    """Message ID. Legacy identifier, use message_id instead"""

    last_updated_timestamp: Optional[int] = None
    """Timestamp when the message was last updated"""

    query_result: Optional[Result] = None
    """The result of SQL query if the message includes a query attachment. Deprecated. Use
    `query_result_metadata` in `GenieQueryAttachment` instead."""

    status: Optional[MessageStatus] = None

    user_id: Optional[int] = None
    """ID of the user who created the message"""

    def as_dict(self) -> dict:
        """Serializes the GenieMessage into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.attachments:
            body["attachments"] = [v.as_dict() for v in self.attachments]
        if self.content is not None:
            body["content"] = self.content
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.created_timestamp is not None:
            body["created_timestamp"] = self.created_timestamp
        if self.error:
            body["error"] = self.error.as_dict()
        if self.feedback:
            body["feedback"] = self.feedback.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.message_id is not None:
            body["message_id"] = self.message_id
        if self.query_result:
            body["query_result"] = self.query_result.as_dict()
        if self.space_id is not None:
            body["space_id"] = self.space_id
        if self.status is not None:
            body["status"] = self.status.value
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieMessage into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.attachments:
            body["attachments"] = self.attachments
        if self.content is not None:
            body["content"] = self.content
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.created_timestamp is not None:
            body["created_timestamp"] = self.created_timestamp
        if self.error:
            body["error"] = self.error
        if self.feedback:
            body["feedback"] = self.feedback
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.message_id is not None:
            body["message_id"] = self.message_id
        if self.query_result:
            body["query_result"] = self.query_result
        if self.space_id is not None:
            body["space_id"] = self.space_id
        if self.status is not None:
            body["status"] = self.status
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieMessage:
        """Deserializes the GenieMessage from a dictionary."""
        return cls(
            attachments=_repeated_dict(d, "attachments", GenieAttachment),
            content=d.get("content", None),
            conversation_id=d.get("conversation_id", None),
            created_timestamp=d.get("created_timestamp", None),
            error=_from_dict(d, "error", MessageError),
            feedback=_from_dict(d, "feedback", GenieFeedback),
            id=d.get("id", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            message_id=d.get("message_id", None),
            query_result=_from_dict(d, "query_result", Result),
            space_id=d.get("space_id", None),
            status=_enum(d, "status", MessageStatus),
            user_id=d.get("user_id", None),
        )


@dataclass
class GenieQueryAttachment:
    description: Optional[str] = None
    """Description of the query"""

    id: Optional[str] = None

    last_updated_timestamp: Optional[int] = None
    """Time when the user updated the query last"""

    parameters: Optional[List[QueryAttachmentParameter]] = None

    query: Optional[str] = None
    """AI generated SQL query"""

    query_result_metadata: Optional[GenieResultMetadata] = None
    """Metadata associated with the query result."""

    statement_id: Optional[str] = None
    """Statement Execution API statement id. Use [Get status, manifest, and result first
    chunk](:method:statementexecution/getstatement) to get the full result data."""

    title: Optional[str] = None
    """Name of the query"""

    def as_dict(self) -> dict:
        """Serializes the GenieQueryAttachment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.query is not None:
            body["query"] = self.query
        if self.query_result_metadata:
            body["query_result_metadata"] = self.query_result_metadata.as_dict()
        if self.statement_id is not None:
            body["statement_id"] = self.statement_id
        if self.title is not None:
            body["title"] = self.title
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieQueryAttachment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.last_updated_timestamp is not None:
            body["last_updated_timestamp"] = self.last_updated_timestamp
        if self.parameters:
            body["parameters"] = self.parameters
        if self.query is not None:
            body["query"] = self.query
        if self.query_result_metadata:
            body["query_result_metadata"] = self.query_result_metadata
        if self.statement_id is not None:
            body["statement_id"] = self.statement_id
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieQueryAttachment:
        """Deserializes the GenieQueryAttachment from a dictionary."""
        return cls(
            description=d.get("description", None),
            id=d.get("id", None),
            last_updated_timestamp=d.get("last_updated_timestamp", None),
            parameters=_repeated_dict(d, "parameters", QueryAttachmentParameter),
            query=d.get("query", None),
            query_result_metadata=_from_dict(d, "query_result_metadata", GenieResultMetadata),
            statement_id=d.get("statement_id", None),
            title=d.get("title", None),
        )


@dataclass
class GenieResultMetadata:
    is_truncated: Optional[bool] = None
    """Indicates whether the result set is truncated."""

    row_count: Optional[int] = None
    """The number of rows in the result set."""

    def as_dict(self) -> dict:
        """Serializes the GenieResultMetadata into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_truncated is not None:
            body["is_truncated"] = self.is_truncated
        if self.row_count is not None:
            body["row_count"] = self.row_count
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieResultMetadata into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_truncated is not None:
            body["is_truncated"] = self.is_truncated
        if self.row_count is not None:
            body["row_count"] = self.row_count
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieResultMetadata:
        """Deserializes the GenieResultMetadata from a dictionary."""
        return cls(is_truncated=d.get("is_truncated", None), row_count=d.get("row_count", None))


@dataclass
class GenieSpace:
    space_id: str
    """Genie space ID"""

    title: str
    """Title of the Genie Space"""

    description: Optional[str] = None
    """Description of the Genie Space"""

    serialized_space: Optional[str] = None
    """The contents of the Genie Space in serialized string form. This field is excluded in List Genie
    spaces responses. Use the [Get Genie Space](:method:genie/getspace) API to retrieve an example
    response, which includes the `serialized_space` field. This field provides the structure of the
    JSON string that represents the space's layout and components."""

    warehouse_id: Optional[str] = None
    """Warehouse associated with the Genie Space"""

    def as_dict(self) -> dict:
        """Serializes the GenieSpace into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.serialized_space is not None:
            body["serialized_space"] = self.serialized_space
        if self.space_id is not None:
            body["space_id"] = self.space_id
        if self.title is not None:
            body["title"] = self.title
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieSpace into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.description is not None:
            body["description"] = self.description
        if self.serialized_space is not None:
            body["serialized_space"] = self.serialized_space
        if self.space_id is not None:
            body["space_id"] = self.space_id
        if self.title is not None:
            body["title"] = self.title
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieSpace:
        """Deserializes the GenieSpace from a dictionary."""
        return cls(
            description=d.get("description", None),
            serialized_space=d.get("serialized_space", None),
            space_id=d.get("space_id", None),
            title=d.get("title", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class GenieStartConversationResponse:
    message_id: str
    """Message ID"""

    conversation_id: str
    """Conversation ID"""

    conversation: Optional[GenieConversation] = None

    message: Optional[GenieMessage] = None

    def as_dict(self) -> dict:
        """Serializes the GenieStartConversationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.conversation:
            body["conversation"] = self.conversation.as_dict()
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.message:
            body["message"] = self.message.as_dict()
        if self.message_id is not None:
            body["message_id"] = self.message_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieStartConversationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.conversation:
            body["conversation"] = self.conversation
        if self.conversation_id is not None:
            body["conversation_id"] = self.conversation_id
        if self.message:
            body["message"] = self.message
        if self.message_id is not None:
            body["message_id"] = self.message_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieStartConversationResponse:
        """Deserializes the GenieStartConversationResponse from a dictionary."""
        return cls(
            conversation=_from_dict(d, "conversation", GenieConversation),
            conversation_id=d.get("conversation_id", None),
            message=_from_dict(d, "message", GenieMessage),
            message_id=d.get("message_id", None),
        )


@dataclass
class GenieSuggestedQuestionsAttachment:
    """Follow-up questions suggested by Genie"""

    questions: Optional[List[str]] = None
    """The suggested follow-up questions"""

    def as_dict(self) -> dict:
        """Serializes the GenieSuggestedQuestionsAttachment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.questions:
            body["questions"] = [v for v in self.questions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GenieSuggestedQuestionsAttachment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.questions:
            body["questions"] = self.questions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GenieSuggestedQuestionsAttachment:
        """Deserializes the GenieSuggestedQuestionsAttachment from a dictionary."""
        return cls(questions=d.get("questions", None))


@dataclass
class GetPublishedDashboardTokenInfoResponse:
    authorization_details: Optional[List[AuthorizationDetails]] = None
    """Authorization constraints for accessing the published dashboard. Currently includes
    `workspace_rule_set` and could be enriched with `unity_catalog_privileges` before oAuth token
    generation."""

    custom_claim: Optional[str] = None
    """Custom claim generated from external_value and external_viewer_id. Format:
    `urn:aibi:external_data:<external_value>:<external_viewer_id>:<dashboard_id>`"""

    scope: Optional[str] = None
    """Scope defining access permissions."""

    def as_dict(self) -> dict:
        """Serializes the GetPublishedDashboardTokenInfoResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.authorization_details:
            body["authorization_details"] = [v.as_dict() for v in self.authorization_details]
        if self.custom_claim is not None:
            body["custom_claim"] = self.custom_claim
        if self.scope is not None:
            body["scope"] = self.scope
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetPublishedDashboardTokenInfoResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.authorization_details:
            body["authorization_details"] = self.authorization_details
        if self.custom_claim is not None:
            body["custom_claim"] = self.custom_claim
        if self.scope is not None:
            body["scope"] = self.scope
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetPublishedDashboardTokenInfoResponse:
        """Deserializes the GetPublishedDashboardTokenInfoResponse from a dictionary."""
        return cls(
            authorization_details=_repeated_dict(d, "authorization_details", AuthorizationDetails),
            custom_claim=d.get("custom_claim", None),
            scope=d.get("scope", None),
        )


class LifecycleState(Enum):

    ACTIVE = "ACTIVE"
    TRASHED = "TRASHED"


@dataclass
class ListDashboardsResponse:
    dashboards: Optional[List[Dashboard]] = None

    next_page_token: Optional[str] = None
    """A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
    there are no subsequent dashboards."""

    def as_dict(self) -> dict:
        """Serializes the ListDashboardsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboards:
            body["dashboards"] = [v.as_dict() for v in self.dashboards]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListDashboardsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboards:
            body["dashboards"] = self.dashboards
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListDashboardsResponse:
        """Deserializes the ListDashboardsResponse from a dictionary."""
        return cls(
            dashboards=_repeated_dict(d, "dashboards", Dashboard), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListSchedulesResponse:
    next_page_token: Optional[str] = None
    """A token that can be used as a `page_token` in subsequent requests to retrieve the next page of
    results. If this field is omitted, there are no subsequent schedules."""

    schedules: Optional[List[Schedule]] = None

    def as_dict(self) -> dict:
        """Serializes the ListSchedulesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.schedules:
            body["schedules"] = [v.as_dict() for v in self.schedules]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSchedulesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.schedules:
            body["schedules"] = self.schedules
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSchedulesResponse:
        """Deserializes the ListSchedulesResponse from a dictionary."""
        return cls(next_page_token=d.get("next_page_token", None), schedules=_repeated_dict(d, "schedules", Schedule))


@dataclass
class ListSubscriptionsResponse:
    next_page_token: Optional[str] = None
    """A token that can be used as a `page_token` in subsequent requests to retrieve the next page of
    results. If this field is omitted, there are no subsequent subscriptions."""

    subscriptions: Optional[List[Subscription]] = None

    def as_dict(self) -> dict:
        """Serializes the ListSubscriptionsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.subscriptions:
            body["subscriptions"] = [v.as_dict() for v in self.subscriptions]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListSubscriptionsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.subscriptions:
            body["subscriptions"] = self.subscriptions
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListSubscriptionsResponse:
        """Deserializes the ListSubscriptionsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            subscriptions=_repeated_dict(d, "subscriptions", Subscription),
        )


@dataclass
class MessageError:
    error: Optional[str] = None

    type: Optional[MessageErrorType] = None

    def as_dict(self) -> dict:
        """Serializes the MessageError into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.error is not None:
            body["error"] = self.error
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MessageError into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.error is not None:
            body["error"] = self.error
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MessageError:
        """Deserializes the MessageError from a dictionary."""
        return cls(error=d.get("error", None), type=_enum(d, "type", MessageErrorType))


class MessageErrorType(Enum):

    BLOCK_MULTIPLE_EXECUTIONS_EXCEPTION = "BLOCK_MULTIPLE_EXECUTIONS_EXCEPTION"
    CHAT_COMPLETION_CLIENT_EXCEPTION = "CHAT_COMPLETION_CLIENT_EXCEPTION"
    CHAT_COMPLETION_CLIENT_TIMEOUT_EXCEPTION = "CHAT_COMPLETION_CLIENT_TIMEOUT_EXCEPTION"
    CHAT_COMPLETION_NETWORK_EXCEPTION = "CHAT_COMPLETION_NETWORK_EXCEPTION"
    CONTENT_FILTER_EXCEPTION = "CONTENT_FILTER_EXCEPTION"
    CONTEXT_EXCEEDED_EXCEPTION = "CONTEXT_EXCEEDED_EXCEPTION"
    COULD_NOT_GET_DASHBOARD_SCHEMA_EXCEPTION = "COULD_NOT_GET_DASHBOARD_SCHEMA_EXCEPTION"
    COULD_NOT_GET_MODEL_DEPLOYMENTS_EXCEPTION = "COULD_NOT_GET_MODEL_DEPLOYMENTS_EXCEPTION"
    COULD_NOT_GET_UC_SCHEMA_EXCEPTION = "COULD_NOT_GET_UC_SCHEMA_EXCEPTION"
    DEPLOYMENT_NOT_FOUND_EXCEPTION = "DEPLOYMENT_NOT_FOUND_EXCEPTION"
    DESCRIBE_QUERY_INVALID_SQL_ERROR = "DESCRIBE_QUERY_INVALID_SQL_ERROR"
    DESCRIBE_QUERY_TIMEOUT = "DESCRIBE_QUERY_TIMEOUT"
    DESCRIBE_QUERY_UNEXPECTED_FAILURE = "DESCRIBE_QUERY_UNEXPECTED_FAILURE"
    EXCEEDED_MAX_TOKEN_LENGTH_EXCEPTION = "EXCEEDED_MAX_TOKEN_LENGTH_EXCEPTION"
    FUNCTIONS_NOT_AVAILABLE_EXCEPTION = "FUNCTIONS_NOT_AVAILABLE_EXCEPTION"
    FUNCTION_ARGUMENTS_INVALID_EXCEPTION = "FUNCTION_ARGUMENTS_INVALID_EXCEPTION"
    FUNCTION_ARGUMENTS_INVALID_JSON_EXCEPTION = "FUNCTION_ARGUMENTS_INVALID_JSON_EXCEPTION"
    FUNCTION_ARGUMENTS_INVALID_TYPE_EXCEPTION = "FUNCTION_ARGUMENTS_INVALID_TYPE_EXCEPTION"
    FUNCTION_CALL_MISSING_PARAMETER_EXCEPTION = "FUNCTION_CALL_MISSING_PARAMETER_EXCEPTION"
    GENERATED_SQL_QUERY_TOO_LONG_EXCEPTION = "GENERATED_SQL_QUERY_TOO_LONG_EXCEPTION"
    GENERIC_CHAT_COMPLETION_EXCEPTION = "GENERIC_CHAT_COMPLETION_EXCEPTION"
    GENERIC_CHAT_COMPLETION_SERVICE_EXCEPTION = "GENERIC_CHAT_COMPLETION_SERVICE_EXCEPTION"
    GENERIC_SQL_EXEC_API_CALL_EXCEPTION = "GENERIC_SQL_EXEC_API_CALL_EXCEPTION"
    ILLEGAL_PARAMETER_DEFINITION_EXCEPTION = "ILLEGAL_PARAMETER_DEFINITION_EXCEPTION"
    INTERNAL_CATALOG_ASSET_CREATION_FAILED_EXCEPTION = "INTERNAL_CATALOG_ASSET_CREATION_FAILED_EXCEPTION"
    INTERNAL_CATALOG_ASSET_CREATION_ONGOING_EXCEPTION = "INTERNAL_CATALOG_ASSET_CREATION_ONGOING_EXCEPTION"
    INTERNAL_CATALOG_ASSET_CREATION_UNSUPPORTED_EXCEPTION = "INTERNAL_CATALOG_ASSET_CREATION_UNSUPPORTED_EXCEPTION"
    INTERNAL_CATALOG_MISSING_UC_PATH_EXCEPTION = "INTERNAL_CATALOG_MISSING_UC_PATH_EXCEPTION"
    INTERNAL_CATALOG_PATH_OVERLAP_EXCEPTION = "INTERNAL_CATALOG_PATH_OVERLAP_EXCEPTION"
    INVALID_CERTIFIED_ANSWER_FUNCTION_EXCEPTION = "INVALID_CERTIFIED_ANSWER_FUNCTION_EXCEPTION"
    INVALID_CERTIFIED_ANSWER_IDENTIFIER_EXCEPTION = "INVALID_CERTIFIED_ANSWER_IDENTIFIER_EXCEPTION"
    INVALID_CHAT_COMPLETION_JSON_EXCEPTION = "INVALID_CHAT_COMPLETION_JSON_EXCEPTION"
    INVALID_COMPLETION_REQUEST_EXCEPTION = "INVALID_COMPLETION_REQUEST_EXCEPTION"
    INVALID_FUNCTION_CALL_EXCEPTION = "INVALID_FUNCTION_CALL_EXCEPTION"
    INVALID_SQL_MULTIPLE_DATASET_REFERENCES_EXCEPTION = "INVALID_SQL_MULTIPLE_DATASET_REFERENCES_EXCEPTION"
    INVALID_SQL_MULTIPLE_STATEMENTS_EXCEPTION = "INVALID_SQL_MULTIPLE_STATEMENTS_EXCEPTION"
    INVALID_SQL_UNKNOWN_TABLE_EXCEPTION = "INVALID_SQL_UNKNOWN_TABLE_EXCEPTION"
    INVALID_TABLE_IDENTIFIER_EXCEPTION = "INVALID_TABLE_IDENTIFIER_EXCEPTION"
    LOCAL_CONTEXT_EXCEEDED_EXCEPTION = "LOCAL_CONTEXT_EXCEEDED_EXCEPTION"
    MESSAGE_ATTACHMENT_TOO_LONG_ERROR = "MESSAGE_ATTACHMENT_TOO_LONG_ERROR"
    MESSAGE_CANCELLED_WHILE_EXECUTING_EXCEPTION = "MESSAGE_CANCELLED_WHILE_EXECUTING_EXCEPTION"
    MESSAGE_DELETED_WHILE_EXECUTING_EXCEPTION = "MESSAGE_DELETED_WHILE_EXECUTING_EXCEPTION"
    MESSAGE_UPDATED_WHILE_EXECUTING_EXCEPTION = "MESSAGE_UPDATED_WHILE_EXECUTING_EXCEPTION"
    MISSING_SQL_QUERY_EXCEPTION = "MISSING_SQL_QUERY_EXCEPTION"
    NO_DEPLOYMENTS_AVAILABLE_TO_WORKSPACE = "NO_DEPLOYMENTS_AVAILABLE_TO_WORKSPACE"
    NO_QUERY_TO_VISUALIZE_EXCEPTION = "NO_QUERY_TO_VISUALIZE_EXCEPTION"
    NO_TABLES_TO_QUERY_EXCEPTION = "NO_TABLES_TO_QUERY_EXCEPTION"
    RATE_LIMIT_EXCEEDED_GENERIC_EXCEPTION = "RATE_LIMIT_EXCEEDED_GENERIC_EXCEPTION"
    RATE_LIMIT_EXCEEDED_SPECIFIED_WAIT_EXCEPTION = "RATE_LIMIT_EXCEEDED_SPECIFIED_WAIT_EXCEPTION"
    REPLY_PROCESS_TIMEOUT_EXCEPTION = "REPLY_PROCESS_TIMEOUT_EXCEPTION"
    RETRYABLE_PROCESSING_EXCEPTION = "RETRYABLE_PROCESSING_EXCEPTION"
    SQL_EXECUTION_EXCEPTION = "SQL_EXECUTION_EXCEPTION"
    STOP_PROCESS_DUE_TO_AUTO_REGENERATE = "STOP_PROCESS_DUE_TO_AUTO_REGENERATE"
    TABLES_MISSING_EXCEPTION = "TABLES_MISSING_EXCEPTION"
    TOO_MANY_CERTIFIED_ANSWERS_EXCEPTION = "TOO_MANY_CERTIFIED_ANSWERS_EXCEPTION"
    TOO_MANY_TABLES_EXCEPTION = "TOO_MANY_TABLES_EXCEPTION"
    UNEXPECTED_REPLY_PROCESS_EXCEPTION = "UNEXPECTED_REPLY_PROCESS_EXCEPTION"
    UNKNOWN_AI_MODEL = "UNKNOWN_AI_MODEL"
    UNSUPPORTED_CONVERSATION_TYPE_EXCEPTION = "UNSUPPORTED_CONVERSATION_TYPE_EXCEPTION"
    WAREHOUSE_ACCESS_MISSING_EXCEPTION = "WAREHOUSE_ACCESS_MISSING_EXCEPTION"
    WAREHOUSE_NOT_FOUND_EXCEPTION = "WAREHOUSE_NOT_FOUND_EXCEPTION"


class MessageStatus(Enum):
    """MessageStatus. The possible values are: * `FETCHING_METADATA`: Fetching metadata from the data
    sources. * `FILTERING_CONTEXT`: Running smart context step to determine relevant context. *
    `ASKING_AI`: Waiting for the LLM to respond to the user's question. * `PENDING_WAREHOUSE`:
    Waiting for warehouse before the SQL query can start executing. * `EXECUTING_QUERY`: Executing a
    generated SQL query. Get the SQL query result by calling
    [getMessageAttachmentQueryResult](:method:genie/getMessageAttachmentQueryResult) API. *
    `FAILED`: The response generation or query execution failed. See `error` field. * `COMPLETED`:
    Message processing is completed. Results are in the `attachments` field. Get the SQL query
    result by calling
    [getMessageAttachmentQueryResult](:method:genie/getMessageAttachmentQueryResult) API. *
    `SUBMITTED`: Message has been submitted. * `QUERY_RESULT_EXPIRED`: SQL result is not available
    anymore. The user needs to rerun the query. Rerun the SQL query result by calling
    [executeMessageAttachmentQuery](:method:genie/executeMessageAttachmentQuery) API. * `CANCELLED`:
    Message has been cancelled."""

    ASKING_AI = "ASKING_AI"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    EXECUTING_QUERY = "EXECUTING_QUERY"
    FAILED = "FAILED"
    FETCHING_METADATA = "FETCHING_METADATA"
    FILTERING_CONTEXT = "FILTERING_CONTEXT"
    PENDING_WAREHOUSE = "PENDING_WAREHOUSE"
    QUERY_RESULT_EXPIRED = "QUERY_RESULT_EXPIRED"
    SUBMITTED = "SUBMITTED"


@dataclass
class PublishedDashboard:
    display_name: Optional[str] = None
    """The display name of the published dashboard."""

    embed_credentials: Optional[bool] = None
    """Indicates whether credentials are embedded in the published dashboard."""

    revision_create_time: Optional[str] = None
    """The timestamp of when the published dashboard was last revised."""

    warehouse_id: Optional[str] = None
    """The warehouse ID used to run the published dashboard."""

    def as_dict(self) -> dict:
        """Serializes the PublishedDashboard into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.embed_credentials is not None:
            body["embed_credentials"] = self.embed_credentials
        if self.revision_create_time is not None:
            body["revision_create_time"] = self.revision_create_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PublishedDashboard into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.embed_credentials is not None:
            body["embed_credentials"] = self.embed_credentials
        if self.revision_create_time is not None:
            body["revision_create_time"] = self.revision_create_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PublishedDashboard:
        """Deserializes the PublishedDashboard from a dictionary."""
        return cls(
            display_name=d.get("display_name", None),
            embed_credentials=d.get("embed_credentials", None),
            revision_create_time=d.get("revision_create_time", None),
            warehouse_id=d.get("warehouse_id", None),
        )


@dataclass
class QueryAttachmentParameter:
    keyword: Optional[str] = None

    sql_type: Optional[str] = None

    value: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the QueryAttachmentParameter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.keyword is not None:
            body["keyword"] = self.keyword
        if self.sql_type is not None:
            body["sql_type"] = self.sql_type
        if self.value is not None:
            body["value"] = self.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the QueryAttachmentParameter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.keyword is not None:
            body["keyword"] = self.keyword
        if self.sql_type is not None:
            body["sql_type"] = self.sql_type
        if self.value is not None:
            body["value"] = self.value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> QueryAttachmentParameter:
        """Deserializes the QueryAttachmentParameter from a dictionary."""
        return cls(keyword=d.get("keyword", None), sql_type=d.get("sql_type", None), value=d.get("value", None))


@dataclass
class Result:
    is_truncated: Optional[bool] = None
    """If result is truncated"""

    row_count: Optional[int] = None
    """Row count of the result"""

    statement_id: Optional[str] = None
    """Statement Execution API statement id. Use [Get status, manifest, and result first
    chunk](:method:statementexecution/getstatement) to get the full result data."""

    statement_id_signature: Optional[str] = None
    """JWT corresponding to the statement contained in this result"""

    def as_dict(self) -> dict:
        """Serializes the Result into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.is_truncated is not None:
            body["is_truncated"] = self.is_truncated
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.statement_id is not None:
            body["statement_id"] = self.statement_id
        if self.statement_id_signature is not None:
            body["statement_id_signature"] = self.statement_id_signature
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Result into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.is_truncated is not None:
            body["is_truncated"] = self.is_truncated
        if self.row_count is not None:
            body["row_count"] = self.row_count
        if self.statement_id is not None:
            body["statement_id"] = self.statement_id
        if self.statement_id_signature is not None:
            body["statement_id_signature"] = self.statement_id_signature
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Result:
        """Deserializes the Result from a dictionary."""
        return cls(
            is_truncated=d.get("is_truncated", None),
            row_count=d.get("row_count", None),
            statement_id=d.get("statement_id", None),
            statement_id_signature=d.get("statement_id_signature", None),
        )


@dataclass
class Schedule:
    cron_schedule: CronSchedule
    """The cron expression describing the frequency of the periodic refresh for this schedule."""

    create_time: Optional[str] = None
    """A timestamp indicating when the schedule was created."""

    dashboard_id: Optional[str] = None
    """UUID identifying the dashboard to which the schedule belongs."""

    display_name: Optional[str] = None
    """The display name for schedule."""

    etag: Optional[str] = None
    """The etag for the schedule. Must be left empty on create, must be provided on updates to ensure
    that the schedule has not been modified since the last read, and can be optionally provided on
    delete."""

    pause_status: Optional[SchedulePauseStatus] = None
    """The status indicates whether this schedule is paused or not."""

    schedule_id: Optional[str] = None
    """UUID identifying the schedule."""

    update_time: Optional[str] = None
    """A timestamp indicating when the schedule was last updated."""

    warehouse_id: Optional[str] = None
    """The warehouse id to run the dashboard with for the schedule."""

    def as_dict(self) -> dict:
        """Serializes the Schedule into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.cron_schedule:
            body["cron_schedule"] = self.cron_schedule.as_dict()
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.etag is not None:
            body["etag"] = self.etag
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status.value
        if self.schedule_id is not None:
            body["schedule_id"] = self.schedule_id
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Schedule into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.cron_schedule:
            body["cron_schedule"] = self.cron_schedule
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.etag is not None:
            body["etag"] = self.etag
        if self.pause_status is not None:
            body["pause_status"] = self.pause_status
        if self.schedule_id is not None:
            body["schedule_id"] = self.schedule_id
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.warehouse_id is not None:
            body["warehouse_id"] = self.warehouse_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Schedule:
        """Deserializes the Schedule from a dictionary."""
        return cls(
            create_time=d.get("create_time", None),
            cron_schedule=_from_dict(d, "cron_schedule", CronSchedule),
            dashboard_id=d.get("dashboard_id", None),
            display_name=d.get("display_name", None),
            etag=d.get("etag", None),
            pause_status=_enum(d, "pause_status", SchedulePauseStatus),
            schedule_id=d.get("schedule_id", None),
            update_time=d.get("update_time", None),
            warehouse_id=d.get("warehouse_id", None),
        )


class SchedulePauseStatus(Enum):

    PAUSED = "PAUSED"
    UNPAUSED = "UNPAUSED"


@dataclass
class Subscriber:
    destination_subscriber: Optional[SubscriptionSubscriberDestination] = None
    """The destination to receive the subscription email. This parameter is mutually exclusive with
    `user_subscriber`."""

    user_subscriber: Optional[SubscriptionSubscriberUser] = None
    """The user to receive the subscription email. This parameter is mutually exclusive with
    `destination_subscriber`."""

    def as_dict(self) -> dict:
        """Serializes the Subscriber into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_subscriber:
            body["destination_subscriber"] = self.destination_subscriber.as_dict()
        if self.user_subscriber:
            body["user_subscriber"] = self.user_subscriber.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Subscriber into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_subscriber:
            body["destination_subscriber"] = self.destination_subscriber
        if self.user_subscriber:
            body["user_subscriber"] = self.user_subscriber
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Subscriber:
        """Deserializes the Subscriber from a dictionary."""
        return cls(
            destination_subscriber=_from_dict(d, "destination_subscriber", SubscriptionSubscriberDestination),
            user_subscriber=_from_dict(d, "user_subscriber", SubscriptionSubscriberUser),
        )


@dataclass
class Subscription:
    subscriber: Subscriber
    """Subscriber details for users and destinations to be added as subscribers to the schedule."""

    create_time: Optional[str] = None
    """A timestamp indicating when the subscription was created."""

    created_by_user_id: Optional[int] = None
    """UserId of the user who adds subscribers (users or notification destinations) to the dashboard's
    schedule."""

    dashboard_id: Optional[str] = None
    """UUID identifying the dashboard to which the subscription belongs."""

    etag: Optional[str] = None
    """The etag for the subscription. Must be left empty on create, can be optionally provided on
    delete to ensure that the subscription has not been deleted since the last read."""

    schedule_id: Optional[str] = None
    """UUID identifying the schedule to which the subscription belongs."""

    skip_notify: Optional[bool] = None
    """Controls whether notifications are sent to the subscriber for scheduled dashboard refreshes. If
    not defined, defaults to false in the backend to match the current behavior (refresh and notify)"""

    subscription_id: Optional[str] = None
    """UUID identifying the subscription."""

    update_time: Optional[str] = None
    """A timestamp indicating when the subscription was last updated."""

    def as_dict(self) -> dict:
        """Serializes the Subscription into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.created_by_user_id is not None:
            body["created_by_user_id"] = self.created_by_user_id
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.etag is not None:
            body["etag"] = self.etag
        if self.schedule_id is not None:
            body["schedule_id"] = self.schedule_id
        if self.skip_notify is not None:
            body["skip_notify"] = self.skip_notify
        if self.subscriber:
            body["subscriber"] = self.subscriber.as_dict()
        if self.subscription_id is not None:
            body["subscription_id"] = self.subscription_id
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Subscription into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.created_by_user_id is not None:
            body["created_by_user_id"] = self.created_by_user_id
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.etag is not None:
            body["etag"] = self.etag
        if self.schedule_id is not None:
            body["schedule_id"] = self.schedule_id
        if self.skip_notify is not None:
            body["skip_notify"] = self.skip_notify
        if self.subscriber:
            body["subscriber"] = self.subscriber
        if self.subscription_id is not None:
            body["subscription_id"] = self.subscription_id
        if self.update_time is not None:
            body["update_time"] = self.update_time
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Subscription:
        """Deserializes the Subscription from a dictionary."""
        return cls(
            create_time=d.get("create_time", None),
            created_by_user_id=d.get("created_by_user_id", None),
            dashboard_id=d.get("dashboard_id", None),
            etag=d.get("etag", None),
            schedule_id=d.get("schedule_id", None),
            skip_notify=d.get("skip_notify", None),
            subscriber=_from_dict(d, "subscriber", Subscriber),
            subscription_id=d.get("subscription_id", None),
            update_time=d.get("update_time", None),
        )


@dataclass
class SubscriptionSubscriberDestination:
    destination_id: str
    """The canonical identifier of the destination to receive email notification."""

    def as_dict(self) -> dict:
        """Serializes the SubscriptionSubscriberDestination into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SubscriptionSubscriberDestination into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.destination_id is not None:
            body["destination_id"] = self.destination_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SubscriptionSubscriberDestination:
        """Deserializes the SubscriptionSubscriberDestination from a dictionary."""
        return cls(destination_id=d.get("destination_id", None))


@dataclass
class SubscriptionSubscriberUser:
    user_id: int
    """UserId of the subscriber."""

    def as_dict(self) -> dict:
        """Serializes the SubscriptionSubscriberUser into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SubscriptionSubscriberUser into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.user_id is not None:
            body["user_id"] = self.user_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SubscriptionSubscriberUser:
        """Deserializes the SubscriptionSubscriberUser from a dictionary."""
        return cls(user_id=d.get("user_id", None))


@dataclass
class TextAttachment:
    content: Optional[str] = None
    """AI generated message"""

    id: Optional[str] = None

    purpose: Optional[TextAttachmentPurpose] = None
    """Purpose/intent of this text attachment"""

    def as_dict(self) -> dict:
        """Serializes the TextAttachment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.content is not None:
            body["content"] = self.content
        if self.id is not None:
            body["id"] = self.id
        if self.purpose is not None:
            body["purpose"] = self.purpose.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TextAttachment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.content is not None:
            body["content"] = self.content
        if self.id is not None:
            body["id"] = self.id
        if self.purpose is not None:
            body["purpose"] = self.purpose
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TextAttachment:
        """Deserializes the TextAttachment from a dictionary."""
        return cls(
            content=d.get("content", None), id=d.get("id", None), purpose=_enum(d, "purpose", TextAttachmentPurpose)
        )


class TextAttachmentPurpose(Enum):
    """Purpose/intent of a text attachment"""

    FOLLOW_UP_QUESTION = "FOLLOW_UP_QUESTION"


@dataclass
class TrashDashboardResponse:
    def as_dict(self) -> dict:
        """Serializes the TrashDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TrashDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TrashDashboardResponse:
        """Deserializes the TrashDashboardResponse from a dictionary."""
        return cls()


@dataclass
class UnpublishDashboardResponse:
    def as_dict(self) -> dict:
        """Serializes the UnpublishDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UnpublishDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UnpublishDashboardResponse:
        """Deserializes the UnpublishDashboardResponse from a dictionary."""
        return cls()


class GenieAPI:
    """Genie provides a no-code experience for business users, powered by AI/BI. Analysts set up spaces that
    business users can use to ask questions using natural language. Genie uses data registered to Unity
    Catalog and requires at least CAN USE permission on a Pro or Serverless SQL warehouse. Also, Databricks
    Assistant must be enabled."""

    def __init__(self, api_client):
        self._api = api_client

    def wait_get_message_genie_completed(
        self,
        conversation_id: str,
        message_id: str,
        space_id: str,
        timeout=timedelta(minutes=20),
        callback: Optional[Callable[[GenieMessage], None]] = None,
    ) -> GenieMessage:
        deadline = time.time() + timeout.total_seconds()
        target_states = (MessageStatus.COMPLETED,)
        failure_states = (MessageStatus.FAILED,)
        status_message = "polling..."
        attempt = 1
        while time.time() < deadline:
            poll = self.get_message(conversation_id=conversation_id, message_id=message_id, space_id=space_id)
            status = poll.status
            status_message = f"current status: {status}"
            if status in target_states:
                return poll
            if callback:
                callback(poll)
            if status in failure_states:
                msg = f"failed to reach COMPLETED, got {status}: {status_message}"
                raise OperationFailed(msg)
            prefix = f"conversation_id={conversation_id}, message_id={message_id}, space_id={space_id}"
            sleep = attempt
            if sleep > 10:
                # sleep 10s max per attempt
                sleep = 10
            _LOG.debug(f"{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        raise TimeoutError(f"timed out after {timeout}: {status_message}")

    def create_message(self, space_id: str, conversation_id: str, content: str) -> Wait[GenieMessage]:
        """Create new message in a [conversation](:method:genie/startconversation). The AI response uses all
        previously created messages in the conversation to respond.

        :param space_id: str
          The ID associated with the Genie space where the conversation is started.
        :param conversation_id: str
          The ID associated with the conversation.
        :param content: str
          User message content.

        :returns:
          Long-running operation waiter for :class:`GenieMessage`.
          See :method:wait_get_message_genie_completed for more details.
        """

        body = {}
        if content is not None:
            body["content"] = content
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
            body=body,
            headers=headers,
        )
        return Wait(
            self.wait_get_message_genie_completed,
            response=GenieMessage.from_dict(op_response),
            conversation_id=conversation_id,
            message_id=op_response["message_id"],
            space_id=space_id,
        )

    def create_message_and_wait(
        self, space_id: str, conversation_id: str, content: str, timeout=timedelta(minutes=20)
    ) -> GenieMessage:
        return self.create_message(content=content, conversation_id=conversation_id, space_id=space_id).result(
            timeout=timeout
        )

    def create_space(
        self,
        warehouse_id: str,
        serialized_space: str,
        *,
        description: Optional[str] = None,
        parent_path: Optional[str] = None,
        title: Optional[str] = None,
    ) -> GenieSpace:
        """Creates a Genie space from a serialized payload.

        :param warehouse_id: str
          Warehouse to associate with the new space
        :param serialized_space: str
          The contents of the Genie Space in serialized string form. Use the [Get Genie
          Space](:method:genie/getspace) API to retrieve an example response, which includes the
          `serialized_space` field. This field provides the structure of the JSON string that represents the
          space's layout and components.
        :param description: str (optional)
          Optional description
        :param parent_path: str (optional)
          Parent folder path where the space will be registered
        :param title: str (optional)
          Optional title override

        :returns: :class:`GenieSpace`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if parent_path is not None:
            body["parent_path"] = parent_path
        if serialized_space is not None:
            body["serialized_space"] = serialized_space
        if title is not None:
            body["title"] = title
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/genie/spaces", body=body, headers=headers)
        return GenieSpace.from_dict(res)

    def delete_conversation(self, space_id: str, conversation_id: str):
        """Delete a conversation.

        :param space_id: str
          The ID associated with the Genie space where the conversation is located.
        :param conversation_id: str
          The ID of the conversation to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}", headers=headers)

    def delete_conversation_message(self, space_id: str, conversation_id: str, message_id: str):
        """Delete a conversation message.

        :param space_id: str
          The ID associated with the Genie space where the message is located.
        :param conversation_id: str
          The ID associated with the conversation.
        :param message_id: str
          The ID associated with the message to delete.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            headers=headers,
        )

    def execute_message_attachment_query(
        self, space_id: str, conversation_id: str, message_id: str, attachment_id: str
    ) -> GenieGetMessageQueryResultResponse:
        """Execute the SQL for a message query attachment. Use this API when the query attachment has expired and
        needs to be re-executed.

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID
        :param attachment_id: str
          Attachment ID

        :returns: :class:`GenieGetMessageQueryResultResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query",
            headers=headers,
        )
        return GenieGetMessageQueryResultResponse.from_dict(res)

    def execute_message_query(
        self, space_id: str, conversation_id: str, message_id: str
    ) -> GenieGetMessageQueryResultResponse:
        """DEPRECATED: Use [Execute Message Attachment Query](:method:genie/executemessageattachmentquery)
        instead.

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID

        :returns: :class:`GenieGetMessageQueryResultResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/execute-query",
            headers=headers,
        )
        return GenieGetMessageQueryResultResponse.from_dict(res)

    def generate_download_full_query_result(
        self, space_id: str, conversation_id: str, message_id: str, attachment_id: str
    ) -> GenieGenerateDownloadFullQueryResultResponse:
        """Initiates a new SQL execution and returns a `download_id` and `download_id_signature` that you can use
        to track the progress of the download. The query result is stored in an external link and can be
        retrieved using the [Get Download Full Query Result](:method:genie/getdownloadfullqueryresult) API.
        Both `download_id` and `download_id_signature` must be provided when calling the Get endpoint.

        ----

        ### **Warning: Databricks strongly recommends that you protect the URLs that are returned by the
        `EXTERNAL_LINKS` disposition.**

        When you use the `EXTERNAL_LINKS` disposition, a short-lived, URL is generated, which can be used to
        download the results directly from . As a short-lived is embedded in this URL, you should protect the
        URL.

        Because URLs are already generated with embedded temporary s, you must not set an `Authorization`
        header in the download requests.

        See [Execute Statement](:method:statementexecution/executestatement) for more details.

        ----

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID
        :param attachment_id: str
          Attachment ID

        :returns: :class:`GenieGenerateDownloadFullQueryResultResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/downloads",
            headers=headers,
        )
        return GenieGenerateDownloadFullQueryResultResponse.from_dict(res)

    def get_download_full_query_result(
        self,
        space_id: str,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
        download_id: str,
        download_id_signature: str,
    ) -> GenieGetDownloadFullQueryResultResponse:
        """After [Generating a Full Query Result Download](:method:genie/generatedownloadfullqueryresult) and
        successfully receiving a `download_id` and `download_id_signature`, use this API to poll the download
        progress. Both `download_id` and `download_id_signature` are required to call this endpoint. When the
        download is complete, the API returns the result in the `EXTERNAL_LINKS` disposition, containing one
        or more external links to the query result files.

        ----

        ### **Warning: Databricks strongly recommends that you protect the URLs that are returned by the
        `EXTERNAL_LINKS` disposition.**

        When you use the `EXTERNAL_LINKS` disposition, a short-lived, URL is generated, which can be used to
        download the results directly from . As a short-lived is embedded in this URL, you should protect the
        URL.

        Because URLs are already generated with embedded temporary s, you must not set an `Authorization`
        header in the download requests.

        See [Execute Statement](:method:statementexecution/executestatement) for more details.

        ----

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID
        :param attachment_id: str
          Attachment ID
        :param download_id: str
          Download ID. This ID is provided by the [Generate Download
          endpoint](:method:genie/generateDownloadFullQueryResult)
        :param download_id_signature: str
          JWT signature for the download_id to ensure secure access to query results

        :returns: :class:`GenieGetDownloadFullQueryResultResponse`
        """

        query = {}
        if download_id_signature is not None:
            query["download_id_signature"] = download_id_signature
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/downloads/{download_id}",
            query=query,
            headers=headers,
        )
        return GenieGetDownloadFullQueryResultResponse.from_dict(res)

    def get_message(self, space_id: str, conversation_id: str, message_id: str) -> GenieMessage:
        """Get message from conversation.

        :param space_id: str
          The ID associated with the Genie space where the target conversation is located.
        :param conversation_id: str
          The ID associated with the target conversation.
        :param message_id: str
          The ID associated with the target message from the identified conversation.

        :returns: :class:`GenieMessage`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            headers=headers,
        )
        return GenieMessage.from_dict(res)

    def get_message_attachment_query_result(
        self, space_id: str, conversation_id: str, message_id: str, attachment_id: str
    ) -> GenieGetMessageQueryResultResponse:
        """Get the result of SQL query if the message has a query attachment. This is only available if a message
        has a query attachment and the message status is `EXECUTING_QUERY` OR `COMPLETED`.

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID
        :param attachment_id: str
          Attachment ID

        :returns: :class:`GenieGetMessageQueryResultResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result",
            headers=headers,
        )
        return GenieGetMessageQueryResultResponse.from_dict(res)

    def get_message_query_result(
        self, space_id: str, conversation_id: str, message_id: str
    ) -> GenieGetMessageQueryResultResponse:
        """DEPRECATED: Use [Get Message Attachment Query Result](:method:genie/getmessageattachmentqueryresult)
        instead.

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID

        :returns: :class:`GenieGetMessageQueryResultResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result",
            headers=headers,
        )
        return GenieGetMessageQueryResultResponse.from_dict(res)

    def get_message_query_result_by_attachment(
        self, space_id: str, conversation_id: str, message_id: str, attachment_id: str
    ) -> GenieGetMessageQueryResultResponse:
        """DEPRECATED: Use [Get Message Attachment Query Result](:method:genie/getmessageattachmentqueryresult)
        instead.

        :param space_id: str
          Genie space ID
        :param conversation_id: str
          Conversation ID
        :param message_id: str
          Message ID
        :param attachment_id: str
          Attachment ID

        :returns: :class:`GenieGetMessageQueryResultResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result/{attachment_id}",
            headers=headers,
        )
        return GenieGetMessageQueryResultResponse.from_dict(res)

    def get_space(self, space_id: str, *, include_serialized_space: Optional[bool] = None) -> GenieSpace:
        """Get details of a Genie Space.

        :param space_id: str
          The ID associated with the Genie space
        :param include_serialized_space: bool (optional)
          Whether to include the serialized space export in the response. Requires at least CAN EDIT
          permission on the space.

        :returns: :class:`GenieSpace`
        """

        query = {}
        if include_serialized_space is not None:
            query["include_serialized_space"] = include_serialized_space
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/genie/spaces/{space_id}", query=query, headers=headers)
        return GenieSpace.from_dict(res)

    def list_conversation_messages(
        self, space_id: str, conversation_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> GenieListConversationMessagesResponse:
        """List messages in a conversation

        :param space_id: str
          The ID associated with the Genie space where the conversation is located
        :param conversation_id: str
          The ID of the conversation to list messages from
        :param page_size: int (optional)
          Maximum number of messages to return per page
        :param page_token: str (optional)
          Token to get the next page of results

        :returns: :class:`GenieListConversationMessagesResponse`
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

        res = self._api.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
            query=query,
            headers=headers,
        )
        return GenieListConversationMessagesResponse.from_dict(res)

    def list_conversations(
        self,
        space_id: str,
        *,
        include_all: Optional[bool] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> GenieListConversationsResponse:
        """Get a list of conversations in a Genie Space.

        :param space_id: str
          The ID of the Genie space to retrieve conversations from.
        :param include_all: bool (optional)
          Include all conversations in the space across all users. Requires at least CAN MANAGE permission on
          the space.
        :param page_size: int (optional)
          Maximum number of conversations to return per page
        :param page_token: str (optional)
          Token to get the next page of results

        :returns: :class:`GenieListConversationsResponse`
        """

        query = {}
        if include_all is not None:
            query["include_all"] = include_all
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

        res = self._api.do("GET", f"/api/2.0/genie/spaces/{space_id}/conversations", query=query, headers=headers)
        return GenieListConversationsResponse.from_dict(res)

    def list_spaces(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> GenieListSpacesResponse:
        """Get list of Genie Spaces.

        :param page_size: int (optional)
          Maximum number of spaces to return per page
        :param page_token: str (optional)
          Pagination token for getting the next page of results

        :returns: :class:`GenieListSpacesResponse`
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

        res = self._api.do("GET", "/api/2.0/genie/spaces", query=query, headers=headers)
        return GenieListSpacesResponse.from_dict(res)

    def send_message_feedback(self, space_id: str, conversation_id: str, message_id: str, rating: GenieFeedbackRating):
        """Send feedback for a message.

        :param space_id: str
          The ID associated with the Genie space where the message is located.
        :param conversation_id: str
          The ID associated with the conversation.
        :param message_id: str
          The ID associated with the message to provide feedback for.
        :param rating: :class:`GenieFeedbackRating`
          The rating (POSITIVE, NEGATIVE, or NONE).


        """

        body = {}
        if rating is not None:
            body["rating"] = rating.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/feedback",
            body=body,
            headers=headers,
        )

    def start_conversation(self, space_id: str, content: str) -> Wait[GenieMessage]:
        """Start a new conversation.

        :param space_id: str
          The ID associated with the Genie space where you want to start a conversation.
        :param content: str
          The text of the message that starts the conversation.

        :returns:
          Long-running operation waiter for :class:`GenieMessage`.
          See :method:wait_get_message_genie_completed for more details.
        """

        body = {}
        if content is not None:
            body["content"] = content
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        op_response = self._api.do(
            "POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation", body=body, headers=headers
        )
        return Wait(
            self.wait_get_message_genie_completed,
            response=GenieStartConversationResponse.from_dict(op_response),
            conversation_id=op_response["conversation_id"],
            message_id=op_response["message_id"],
            space_id=space_id,
        )

    def start_conversation_and_wait(self, space_id: str, content: str, timeout=timedelta(minutes=20)) -> GenieMessage:
        return self.start_conversation(content=content, space_id=space_id).result(timeout=timeout)

    def trash_space(self, space_id: str):
        """Move a Genie Space to the trash.

        :param space_id: str
          The ID associated with the Genie space to be sent to the trash.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/genie/spaces/{space_id}", headers=headers)

    def update_space(
        self,
        space_id: str,
        *,
        description: Optional[str] = None,
        serialized_space: Optional[str] = None,
        title: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ) -> GenieSpace:
        """Updates a Genie space with a serialized payload.

        :param space_id: str
          Genie space ID
        :param description: str (optional)
          Optional description
        :param serialized_space: str (optional)
          The contents of the Genie Space in serialized string form (full replacement). Use the [Get Genie
          Space](:method:genie/getspace) API to retrieve an example response, which includes the
          `serialized_space` field. This field provides the structure of the JSON string that represents the
          space's layout and components.
        :param title: str (optional)
          Optional title override
        :param warehouse_id: str (optional)
          Optional warehouse override

        :returns: :class:`GenieSpace`
        """

        body = {}
        if description is not None:
            body["description"] = description
        if serialized_space is not None:
            body["serialized_space"] = serialized_space
        if title is not None:
            body["title"] = title
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body, headers=headers)
        return GenieSpace.from_dict(res)


class LakeviewAPI:
    """These APIs provide specific management operations for Lakeview dashboards. Generic resource management can
    be done with Workspace API (import, export, get-status, list, delete)."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self, dashboard: Dashboard, *, dataset_catalog: Optional[str] = None, dataset_schema: Optional[str] = None
    ) -> Dashboard:
        """Create a draft dashboard.

        :param dashboard: :class:`Dashboard`
        :param dataset_catalog: str (optional)
          Sets the default catalog for all datasets in this dashboard. Does not impact table references that
          use fully qualified catalog names (ex: samples.nyctaxi.trips). Leave blank to keep each dataset’s
          existing configuration.
        :param dataset_schema: str (optional)
          Sets the default schema for all datasets in this dashboard. Does not impact table references that
          use fully qualified schema names (ex: nyctaxi.trips). Leave blank to keep each dataset’s existing
          configuration.

        :returns: :class:`Dashboard`
        """

        body = dashboard.as_dict()
        query = {}
        if dataset_catalog is not None:
            query["dataset_catalog"] = dataset_catalog
        if dataset_schema is not None:
            query["dataset_schema"] = dataset_schema
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/lakeview/dashboards", query=query, body=body, headers=headers)
        return Dashboard.from_dict(res)

    def create_schedule(self, dashboard_id: str, schedule: Schedule) -> Schedule:
        """Create dashboard schedule.

        :param dashboard_id: str
          UUID identifying the dashboard to which the schedule belongs.
        :param schedule: :class:`Schedule`
          The schedule to create. A dashboard is limited to 10 schedules.

        :returns: :class:`Schedule`
        """

        body = schedule.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules", body=body, headers=headers)
        return Schedule.from_dict(res)

    def create_subscription(self, dashboard_id: str, schedule_id: str, subscription: Subscription) -> Subscription:
        """Create schedule subscription.

        :param dashboard_id: str
          UUID identifying the dashboard to which the subscription belongs.
        :param schedule_id: str
          UUID identifying the schedule to which the subscription belongs.
        :param subscription: :class:`Subscription`
          The subscription to create. A schedule is limited to 100 subscriptions.

        :returns: :class:`Subscription`
        """

        body = subscription.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}/subscriptions",
            body=body,
            headers=headers,
        )
        return Subscription.from_dict(res)

    def delete_schedule(self, dashboard_id: str, schedule_id: str, *, etag: Optional[str] = None):
        """Delete dashboard schedule.

        :param dashboard_id: str
          UUID identifying the dashboard to which the schedule belongs.
        :param schedule_id: str
          UUID identifying the schedule.
        :param etag: str (optional)
          The etag for the schedule. Optionally, it can be provided to verify that the schedule has not been
          modified from its last retrieval.


        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}",
            query=query,
            headers=headers,
        )

    def delete_subscription(
        self, dashboard_id: str, schedule_id: str, subscription_id: str, *, etag: Optional[str] = None
    ):
        """Delete schedule subscription.

        :param dashboard_id: str
          UUID identifying the dashboard which the subscription belongs.
        :param schedule_id: str
          UUID identifying the schedule which the subscription belongs.
        :param subscription_id: str
          UUID identifying the subscription.
        :param etag: str (optional)
          The etag for the subscription. Can be optionally provided to ensure that the subscription has not
          been modified since the last read.


        """

        query = {}
        if etag is not None:
            query["etag"] = etag
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}/subscriptions/{subscription_id}",
            query=query,
            headers=headers,
        )

    def get(self, dashboard_id: str) -> Dashboard:
        """Get a draft dashboard.

        :param dashboard_id: str
          UUID identifying the dashboard.

        :returns: :class:`Dashboard`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/lakeview/dashboards/{dashboard_id}", headers=headers)
        return Dashboard.from_dict(res)

    def get_published(self, dashboard_id: str) -> PublishedDashboard:
        """Get the current published dashboard.

        :param dashboard_id: str
          UUID identifying the published dashboard.

        :returns: :class:`PublishedDashboard`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", headers=headers)
        return PublishedDashboard.from_dict(res)

    def get_schedule(self, dashboard_id: str, schedule_id: str) -> Schedule:
        """Get dashboard schedule.

        :param dashboard_id: str
          UUID identifying the dashboard to which the schedule belongs.
        :param schedule_id: str
          UUID identifying the schedule.

        :returns: :class:`Schedule`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}", headers=headers
        )
        return Schedule.from_dict(res)

    def get_subscription(self, dashboard_id: str, schedule_id: str, subscription_id: str) -> Subscription:
        """Get schedule subscription.

        :param dashboard_id: str
          UUID identifying the dashboard which the subscription belongs.
        :param schedule_id: str
          UUID identifying the schedule which the subscription belongs.
        :param subscription_id: str
          UUID identifying the subscription.

        :returns: :class:`Subscription`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}/subscriptions/{subscription_id}",
            headers=headers,
        )
        return Subscription.from_dict(res)

    def list(
        self,
        *,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        show_trashed: Optional[bool] = None,
        view: Optional[DashboardView] = None,
    ) -> Iterator[Dashboard]:
        """List dashboards.

        :param page_size: int (optional)
          The number of dashboards to return per page.
        :param page_token: str (optional)
          A page token, received from a previous `ListDashboards` call. This token can be used to retrieve the
          subsequent page.
        :param show_trashed: bool (optional)
          The flag to include dashboards located in the trash. If unspecified, only active dashboards will be
          returned.
        :param view: :class:`DashboardView` (optional)
          `DASHBOARD_VIEW_BASIC`only includes summary metadata from the dashboard.

        :returns: Iterator over :class:`Dashboard`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if show_trashed is not None:
            query["show_trashed"] = show_trashed
        if view is not None:
            query["view"] = view.value
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/lakeview/dashboards", query=query, headers=headers)
            if "dashboards" in json:
                for v in json["dashboards"]:
                    yield Dashboard.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_schedules(
        self, dashboard_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Schedule]:
        """List dashboard schedules.

        :param dashboard_id: str
          UUID identifying the dashboard to which the schedules belongs.
        :param page_size: int (optional)
          The number of schedules to return per page.
        :param page_token: str (optional)
          A page token, received from a previous `ListSchedules` call. Use this to retrieve the subsequent
          page.

        :returns: Iterator over :class:`Schedule`
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
                "GET", f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules", query=query, headers=headers
            )
            if "schedules" in json:
                for v in json["schedules"]:
                    yield Schedule.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_subscriptions(
        self, dashboard_id: str, schedule_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[Subscription]:
        """List schedule subscriptions.

        :param dashboard_id: str
          UUID identifying the dashboard which the subscriptions belongs.
        :param schedule_id: str
          UUID identifying the schedule which the subscriptions belongs.
        :param page_size: int (optional)
          The number of subscriptions to return per page.
        :param page_token: str (optional)
          A page token, received from a previous `ListSubscriptions` call. Use this to retrieve the subsequent
          page.

        :returns: Iterator over :class:`Subscription`
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
                f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}/subscriptions",
                query=query,
                headers=headers,
            )
            if "subscriptions" in json:
                for v in json["subscriptions"]:
                    yield Subscription.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def migrate(
        self,
        source_dashboard_id: str,
        *,
        display_name: Optional[str] = None,
        parent_path: Optional[str] = None,
        update_parameter_syntax: Optional[bool] = None,
    ) -> Dashboard:
        """Migrates a classic SQL dashboard to Lakeview.

        :param source_dashboard_id: str
          UUID of the dashboard to be migrated.
        :param display_name: str (optional)
          Display name for the new Lakeview dashboard.
        :param parent_path: str (optional)
          The workspace path of the folder to contain the migrated Lakeview dashboard.
        :param update_parameter_syntax: bool (optional)
          Flag to indicate if mustache parameter syntax ({{ param }}) should be auto-updated to named syntax
          (:param) when converting datasets in the dashboard.

        :returns: :class:`Dashboard`
        """

        body = {}
        if display_name is not None:
            body["display_name"] = display_name
        if parent_path is not None:
            body["parent_path"] = parent_path
        if source_dashboard_id is not None:
            body["source_dashboard_id"] = source_dashboard_id
        if update_parameter_syntax is not None:
            body["update_parameter_syntax"] = update_parameter_syntax
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/lakeview/dashboards/migrate", body=body, headers=headers)
        return Dashboard.from_dict(res)

    def publish(
        self, dashboard_id: str, *, embed_credentials: Optional[bool] = None, warehouse_id: Optional[str] = None
    ) -> PublishedDashboard:
        """Publish the current draft dashboard.

        :param dashboard_id: str
          UUID identifying the dashboard to be published.
        :param embed_credentials: bool (optional)
          Flag to indicate if the publisher's credentials should be embedded in the published dashboard. These
          embedded credentials will be used to execute the published dashboard's queries.
        :param warehouse_id: str (optional)
          The ID of the warehouse that can be used to override the warehouse which was set in the draft.

        :returns: :class:`PublishedDashboard`
        """

        body = {}
        if embed_credentials is not None:
            body["embed_credentials"] = embed_credentials
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", body=body, headers=headers)
        return PublishedDashboard.from_dict(res)

    def trash(self, dashboard_id: str):
        """Trash a dashboard.

        :param dashboard_id: str
          UUID identifying the dashboard.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/lakeview/dashboards/{dashboard_id}", headers=headers)

    def unpublish(self, dashboard_id: str):
        """Unpublish the dashboard.

        :param dashboard_id: str
          UUID identifying the published dashboard.


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", headers=headers)

    def update(
        self,
        dashboard_id: str,
        dashboard: Dashboard,
        *,
        dataset_catalog: Optional[str] = None,
        dataset_schema: Optional[str] = None,
    ) -> Dashboard:
        """Update a draft dashboard.

        :param dashboard_id: str
          UUID identifying the dashboard.
        :param dashboard: :class:`Dashboard`
        :param dataset_catalog: str (optional)
          Sets the default catalog for all datasets in this dashboard. Does not impact table references that
          use fully qualified catalog names (ex: samples.nyctaxi.trips). Leave blank to keep each dataset’s
          existing configuration.
        :param dataset_schema: str (optional)
          Sets the default schema for all datasets in this dashboard. Does not impact table references that
          use fully qualified schema names (ex: nyctaxi.trips). Leave blank to keep each dataset’s existing
          configuration.

        :returns: :class:`Dashboard`
        """

        body = dashboard.as_dict()
        query = {}
        if dataset_catalog is not None:
            query["dataset_catalog"] = dataset_catalog
        if dataset_schema is not None:
            query["dataset_schema"] = dataset_schema
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", query=query, body=body, headers=headers
        )
        return Dashboard.from_dict(res)

    def update_schedule(self, dashboard_id: str, schedule_id: str, schedule: Schedule) -> Schedule:
        """Update dashboard schedule.

        :param dashboard_id: str
          UUID identifying the dashboard to which the schedule belongs.
        :param schedule_id: str
          UUID identifying the schedule.
        :param schedule: :class:`Schedule`
          The schedule to update.

        :returns: :class:`Schedule`
        """

        body = schedule.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT", f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}", body=body, headers=headers
        )
        return Schedule.from_dict(res)


class LakeviewEmbeddedAPI:
    """Token-based Lakeview APIs for embedding dashboards in external applications."""

    def __init__(self, api_client):
        self._api = api_client

    def get_published_dashboard_token_info(
        self, dashboard_id: str, *, external_value: Optional[str] = None, external_viewer_id: Optional[str] = None
    ) -> GetPublishedDashboardTokenInfoResponse:
        """Get a required authorization details and scopes of a published dashboard to mint an OAuth token.

        :param dashboard_id: str
          UUID identifying the published dashboard.
        :param external_value: str (optional)
          Provided external value to be included in the custom claim.
        :param external_viewer_id: str (optional)
          Provided external viewer id to be included in the custom claim.

        :returns: :class:`GetPublishedDashboardTokenInfoResponse`
        """

        query = {}
        if external_value is not None:
            query["external_value"] = external_value
        if external_viewer_id is not None:
            query["external_viewer_id"] = external_viewer_id
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published/tokeninfo", query=query, headers=headers
        )
        return GetPublishedDashboardTokenInfoResponse.from_dict(res)
