# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import _repeated_dict

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class ListTagAssignmentsResponse:
    next_page_token: Optional[str] = None
    """Pagination token to request the next page of tag assignments"""

    tag_assignments: Optional[List[TagAssignment]] = None

    def as_dict(self) -> dict:
        """Serializes the ListTagAssignmentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tag_assignments:
            body["tag_assignments"] = [v.as_dict() for v in self.tag_assignments]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListTagAssignmentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tag_assignments:
            body["tag_assignments"] = self.tag_assignments
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListTagAssignmentsResponse:
        """Deserializes the ListTagAssignmentsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            tag_assignments=_repeated_dict(d, "tag_assignments", TagAssignment),
        )


@dataclass
class ListTagPoliciesResponse:
    next_page_token: Optional[str] = None

    tag_policies: Optional[List[TagPolicy]] = None

    def as_dict(self) -> dict:
        """Serializes the ListTagPoliciesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tag_policies:
            body["tag_policies"] = [v.as_dict() for v in self.tag_policies]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListTagPoliciesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.tag_policies:
            body["tag_policies"] = self.tag_policies
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListTagPoliciesResponse:
        """Deserializes the ListTagPoliciesResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), tag_policies=_repeated_dict(d, "tag_policies", TagPolicy)
        )


@dataclass
class TagAssignment:
    entity_type: str
    """The type of entity to which the tag is assigned. Allowed values are apps, dashboards,
    geniespaces"""

    entity_id: str
    """The identifier of the entity to which the tag is assigned. For apps, the entity_id is the app
    name"""

    tag_key: str
    """The key of the tag. The characters , . : / - = and leading/trailing spaces are not allowed"""

    tag_value: Optional[str] = None
    """The value of the tag"""

    def as_dict(self) -> dict:
        """Serializes the TagAssignment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.entity_id is not None:
            body["entity_id"] = self.entity_id
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.tag_key is not None:
            body["tag_key"] = self.tag_key
        if self.tag_value is not None:
            body["tag_value"] = self.tag_value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TagAssignment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.entity_id is not None:
            body["entity_id"] = self.entity_id
        if self.entity_type is not None:
            body["entity_type"] = self.entity_type
        if self.tag_key is not None:
            body["tag_key"] = self.tag_key
        if self.tag_value is not None:
            body["tag_value"] = self.tag_value
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TagAssignment:
        """Deserializes the TagAssignment from a dictionary."""
        return cls(
            entity_id=d.get("entity_id", None),
            entity_type=d.get("entity_type", None),
            tag_key=d.get("tag_key", None),
            tag_value=d.get("tag_value", None),
        )


@dataclass
class TagPolicy:
    tag_key: str

    create_time: Optional[str] = None
    """Timestamp when the tag policy was created"""

    description: Optional[str] = None

    id: Optional[str] = None

    update_time: Optional[str] = None
    """Timestamp when the tag policy was last updated"""

    values: Optional[List[Value]] = None

    def as_dict(self) -> dict:
        """Serializes the TagPolicy into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.tag_key is not None:
            body["tag_key"] = self.tag_key
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.values:
            body["values"] = [v.as_dict() for v in self.values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TagPolicy into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.create_time is not None:
            body["create_time"] = self.create_time
        if self.description is not None:
            body["description"] = self.description
        if self.id is not None:
            body["id"] = self.id
        if self.tag_key is not None:
            body["tag_key"] = self.tag_key
        if self.update_time is not None:
            body["update_time"] = self.update_time
        if self.values:
            body["values"] = self.values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TagPolicy:
        """Deserializes the TagPolicy from a dictionary."""
        return cls(
            create_time=d.get("create_time", None),
            description=d.get("description", None),
            id=d.get("id", None),
            tag_key=d.get("tag_key", None),
            update_time=d.get("update_time", None),
            values=_repeated_dict(d, "values", Value),
        )


@dataclass
class Value:
    name: str

    def as_dict(self) -> dict:
        """Serializes the Value into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Value into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Value:
        """Deserializes the Value from a dictionary."""
        return cls(name=d.get("name", None))


class TagPoliciesAPI:
    """The Tag Policy API allows you to manage policies for governed tags in Databricks. For Terraform usage, see
    the [Tag Policy Terraform documentation]. Permissions for tag policies can be managed using the [Account
    Access Control Proxy API].

    [Account Access Control Proxy API]: https://docs.databricks.com/api/workspace/accountaccesscontrolproxy
    [Tag Policy Terraform documentation]: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/tag_policy
    """

    def __init__(self, api_client):
        self._api = api_client

    def create_tag_policy(self, tag_policy: TagPolicy) -> TagPolicy:
        """Creates a new tag policy, making the associated tag key governed. For Terraform usage, see the [Tag
        Policy Terraform documentation]. To manage permissions for tag policies, use the [Account Access
        Control Proxy API].

        [Account Access Control Proxy API]: https://docs.databricks.com/api/workspace/accountaccesscontrolproxy
        [Tag Policy Terraform documentation]: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/tag_policy

        :param tag_policy: :class:`TagPolicy`

        :returns: :class:`TagPolicy`
        """

        body = tag_policy.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.1/tag-policies", body=body, headers=headers)
        return TagPolicy.from_dict(res)

    def delete_tag_policy(self, tag_key: str):
        """Deletes a tag policy by its associated governed tag's key, leaving that tag key ungoverned. For
        Terraform usage, see the [Tag Policy Terraform documentation].

        [Tag Policy Terraform documentation]: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/tag_policy

        :param tag_key: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.1/tag-policies/{tag_key}", headers=headers)

    def get_tag_policy(self, tag_key: str) -> TagPolicy:
        """Gets a single tag policy by its associated governed tag's key. For Terraform usage, see the [Tag
        Policy Terraform documentation]. To list granted permissions for tag policies, use the [Account Access
        Control Proxy API].

        [Account Access Control Proxy API]: https://docs.databricks.com/api/workspace/accountaccesscontrolproxy
        [Tag Policy Terraform documentation]: https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/tag_policy

        :param tag_key: str

        :returns: :class:`TagPolicy`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/tag-policies/{tag_key}", headers=headers)
        return TagPolicy.from_dict(res)

    def list_tag_policies(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[TagPolicy]:
        """Lists the tag policies for all governed tags in the account. For Terraform usage, see the [Tag Policy
        Terraform documentation]. To list granted permissions for tag policies, use the [Account Access
        Control Proxy API].

        [Account Access Control Proxy API]: https://docs.databricks.com/api/workspace/accountaccesscontrolproxy
        [Tag Policy Terraform documentation]: https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/tag_policies

        :param page_size: int (optional)
          The maximum number of results to return in this request. Fewer results may be returned than
          requested. If unspecified or set to 0, this defaults to 1000. The maximum value is 1000; values
          above 1000 will be coerced down to 1000.
        :param page_token: str (optional)
          An optional page token received from a previous list tag policies call.

        :returns: Iterator over :class:`TagPolicy`
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
            json = self._api.do("GET", "/api/2.1/tag-policies", query=query, headers=headers)
            if "tag_policies" in json:
                for v in json["tag_policies"]:
                    yield TagPolicy.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_tag_policy(self, tag_key: str, tag_policy: TagPolicy, update_mask: str) -> TagPolicy:
        """Updates an existing tag policy for a single governed tag. For Terraform usage, see the [Tag Policy
        Terraform documentation]. To manage permissions for tag policies, use the [Account Access Control
        Proxy API].

        [Account Access Control Proxy API]: https://docs.databricks.com/api/workspace/accountaccesscontrolproxy
        [Tag Policy Terraform documentation]: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/tag_policy

        :param tag_key: str
        :param tag_policy: :class:`TagPolicy`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`TagPolicy`
        """

        body = tag_policy.as_dict()
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

        res = self._api.do("PATCH", f"/api/2.1/tag-policies/{tag_key}", query=query, body=body, headers=headers)
        return TagPolicy.from_dict(res)


class WorkspaceEntityTagAssignmentsAPI:
    """Manage tag assignments on workspace-scoped objects."""

    def __init__(self, api_client):
        self._api = api_client

    def create_tag_assignment(self, tag_assignment: TagAssignment) -> TagAssignment:
        """Create a tag assignment

        :param tag_assignment: :class:`TagAssignment`

        :returns: :class:`TagAssignment`
        """

        body = tag_assignment.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/entity-tag-assignments", body=body, headers=headers)
        return TagAssignment.from_dict(res)

    def delete_tag_assignment(self, entity_type: str, entity_id: str, tag_key: str):
        """Delete a tag assignment

        :param entity_type: str
          The type of entity to which the tag is assigned. Allowed values are apps, dashboards, geniespaces
        :param entity_id: str
          The identifier of the entity to which the tag is assigned. For apps, the entity_id is the app name
        :param tag_key: str
          The key of the tag. The characters , . : / - = and leading/trailing spaces are not allowed


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags/{tag_key}", headers=headers
        )

    def get_tag_assignment(self, entity_type: str, entity_id: str, tag_key: str) -> TagAssignment:
        """Get a tag assignment

        :param entity_type: str
          The type of entity to which the tag is assigned. Allowed values are apps, dashboards, geniespaces
        :param entity_id: str
          The identifier of the entity to which the tag is assigned. For apps, the entity_id is the app name
        :param tag_key: str
          The key of the tag. The characters , . : / - = and leading/trailing spaces are not allowed

        :returns: :class:`TagAssignment`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags/{tag_key}", headers=headers
        )
        return TagAssignment.from_dict(res)

    def list_tag_assignments(
        self, entity_type: str, entity_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[TagAssignment]:
        """List the tag assignments for an entity

        :param entity_type: str
          The type of entity to which the tag is assigned. Allowed values are apps, dashboards, geniespaces
        :param entity_id: str
          The identifier of the entity to which the tag is assigned. For apps, the entity_id is the app name
        :param page_size: int (optional)
          Optional. Maximum number of tag assignments to return in a single page
        :param page_token: str (optional)
          Pagination token to go to the next page of tag assignments. Requests first page if absent.

        :returns: Iterator over :class:`TagAssignment`
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
                "GET", f"/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags", query=query, headers=headers
            )
            if "tag_assignments" in json:
                for v in json["tag_assignments"]:
                    yield TagAssignment.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update_tag_assignment(
        self, entity_type: str, entity_id: str, tag_key: str, tag_assignment: TagAssignment, update_mask: str
    ) -> TagAssignment:
        """Update a tag assignment

        :param entity_type: str
          The type of entity to which the tag is assigned. Allowed values are apps, dashboards, geniespaces
        :param entity_id: str
          The identifier of the entity to which the tag is assigned. For apps, the entity_id is the app name
        :param tag_key: str
          The key of the tag. The characters , . : / - = and leading/trailing spaces are not allowed
        :param tag_assignment: :class:`TagAssignment`
        :param update_mask: str
          The field mask must be a single string, with multiple fields separated by commas (no spaces). The
          field path is relative to the resource object, using a dot (`.`) to navigate sub-fields (e.g.,
          `author.given_name`). Specification of elements in sequence or map fields is not allowed, as only
          the entire collection field can be specified. Field names must exactly match the resource field
          names.

          A field mask of `*` indicates full replacement. It’s recommended to always explicitly list the
          fields being updated and avoid using `*` wildcards, as it can lead to unintended results if the API
          changes in the future.

        :returns: :class:`TagAssignment`
        """

        body = tag_assignment.as_dict()
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
            f"/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags/{tag_key}",
            query=query,
            body=body,
            headers=headers,
        )
        return TagAssignment.from_dict(res)
