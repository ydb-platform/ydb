#!/usr/bin/env python
"""Identity secret engine module."""
import logging

from hvac import exceptions, utils
from hvac.api.vault_api_base import VaultApiBase
from hvac.constants.identity import ALLOWED_GROUP_TYPES, DEFAULT_MOUNT_POINT

logger = logging.getLogger(__name__)


class Identity(VaultApiBase):
    """Identity Secrets Engine (API).

    Reference: https://www.vaultproject.io/api/secret/identity/entity.html
    """

    def create_or_update_entity(
        self,
        name,
        entity_id=None,
        metadata=None,
        policies=None,
        disabled=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create or update an Entity.

        Supported methods:
            POST: /{mount_point}/entity. Produces: 200 application/json

        :param entity_id: ID of the entity. If set, updates the corresponding existing entity.
        :type entity_id: str | unicode
        :param name: Name of the entity.
        :type name: str | unicode
        :param metadata: Metadata to be associated with the entity.
        :type metadata: dict
        :param policies: Policies to be tied to the entity.
        :type policies: str | unicode
        :param disabled: Whether the entity is disabled. Disabled entities' associated tokens cannot be used, but are
            not revoked.
        :type disabled: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response for creates, the generic response object for updates, of the request.
        :rtype: dict | requests.Response
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )
        params = utils.remove_nones(
            {
                "id": entity_id,
                "name": name,
                "metadata": metadata,
                "policies": policies,
                "disabled": disabled,
            }
        )
        api_path = utils.format_url("/v1/{mount_point}/entity", mount_point=mount_point)
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def create_or_update_entity_by_name(
        self,
        name,
        metadata=None,
        policies=None,
        disabled=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create or update an entity by a given name.

        Supported methods:
            POST: /{mount_point}/entity/name/{name}. Produces: 200 application/json

        :param name: Name of the entity.
        :type name: str | unicode
        :param metadata: Metadata to be associated with the entity.
        :type metadata: dict
        :param policies: Policies to be tied to the entity.
        :type policies: str | unicode
        :param disabled: Whether the entity is disabled. Disabled
            entities' associated tokens cannot be used, but are not revoked.
        :type disabled: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response for creates, the generic response of the request for updates.
        :rtype: requests.Response | dict
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )
        params = utils.remove_nones(
            {
                "metadata": metadata,
                "policies": policies,
                "disabled": disabled,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/name/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_entity(self, entity_id, mount_point=DEFAULT_MOUNT_POINT):
        """Query an entity by its identifier.

        Supported methods:
            GET: /auth/{mount_point}/entity/id/{id}. Produces: 200 application/json

        :param entity_id: Identifier of the entity.
        :type entity_id: str
        :param mount_point: The "path" the secret engine was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/id/{id}",
            mount_point=mount_point,
            id=entity_id,
        )
        return self._adapter.get(url=api_path)

    def read_entity_by_name(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Query an entity by its name.

        Supported methods:
            GET: /{mount_point}/entity/name/{name}. Produces: 200 application/json

        :param name: Name of the entity.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/name/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def update_entity(
        self,
        entity_id,
        name=None,
        metadata=None,
        policies=None,
        disabled=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Update an existing entity.

        Supported methods:
            POST: /{mount_point}/entity/id/{id}. Produces: 200 application/json

        :param entity_id: Identifier of the entity.
        :type entity_id: str | unicode
        :param name: Name of the entity.
        :type name: str | unicode
        :param metadata: Metadata to be associated with the entity.
        :type metadata: dict
        :param policies: Policies to be tied to the entity.
        :type policies: str | unicode
        :param disabled: Whether the entity is disabled. Disabled entities' associated tokens cannot be used, but
            are not revoked.
        :type disabled: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response where available, otherwise the generic response object, of the request.
        :rtype: dict | requests.Response
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )
        params = utils.remove_nones(
            {
                "name": name,
                "metadata": metadata,
                "policies": policies,
                "disabled": disabled,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/id/{id}",
            mount_point=mount_point,
            id=entity_id,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def delete_entity(self, entity_id, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an entity and all its associated aliases.

        Supported methods:
            DELETE: /{mount_point}/entity/id/:id. Produces: 204 (empty body)

        :param entity_id: Identifier of the entity.
        :type entity_id: str
        :param mount_point: The "path" the secret engine was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/id/{id}",
            mount_point=mount_point,
            id=entity_id,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def delete_entity_by_name(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an entity and all its associated aliases, given the entity name.

        Supported methods:
            DELETE: /{mount_point}/entity/name/{name}. Produces: 204 (empty body)

        :param name: Name of the entity.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/name/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def list_entities(self, method="LIST", mount_point=DEFAULT_MOUNT_POINT):
        """List available entities entities by their identifiers.

        :param method: Supported methods:
            LIST: /{mount_point}/entity/id. Produces: 200 application/json
            GET: /{mount_point}/entity/id?list=true. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        if method == "LIST":
            api_path = utils.format_url(
                "/v1/{mount_point}/entity/id", mount_point=mount_point
            )
            response = self._adapter.list(
                url=api_path,
            )

        elif method == "GET":
            api_path = utils.format_url(
                "/v1/{mount_point}/entity/id?list=true", mount_point=mount_point
            )
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; LIST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response

    def list_entities_by_name(self, method="LIST", mount_point=DEFAULT_MOUNT_POINT):
        """List available entities by their names.

        :param method: Supported methods:
            LIST: /{mount_point}/entity/name. Produces: 200 application/json
            GET: /{mount_point}/entity/name?list=true. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        if method == "LIST":
            api_path = utils.format_url(
                "/v1/{mount_point}/entity/name", mount_point=mount_point
            )
            response = self._adapter.list(
                url=api_path,
            )

        elif method == "GET":
            api_path = utils.format_url(
                "/v1/{mount_point}/entity/name?list=true", mount_point=mount_point
            )
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; LIST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response

    def merge_entities(
        self,
        from_entity_ids,
        to_entity_id,
        force=None,
        mount_point=DEFAULT_MOUNT_POINT,
        conflicting_alias_ids_to_keep=None,
    ):
        """Merge many entities into one entity.

        Supported methods:
            POST: /{mount_point}/entity/merge. Produces: 204 (empty body)

        :param from_entity_ids: Entity IDs which needs to get merged.
        :type from_entity_ids: array
        :param to_entity_id: Entity ID into which all the other entities need to get merged.
        :type to_entity_id: str | unicode
        :param force: Setting this will follow the 'mine' strategy for merging MFA secrets. If there are secrets of the
            same type both in entities that are merged from and in entity into which all others are getting merged,
            secrets in the destination will be unaltered. If not set, this API will throw an error containing all the
            conflicts.
        :type force: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :param conflicting_alias_ids_to_keep: A list of entity aliases to keep in the case where the to-Entity and
            from-Entity have aliases with the same mount accessor. In the case where alias share mount accessors, the
            alias ID given in this list will be kept or merged, and the other alias will be deleted. Note that merges
            requiring this parameter must have only one from-Entity.
            Requires Vault 1.12 or higher
        :type conflicting_alias_ids_to_keep: list
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "from_entity_ids": from_entity_ids,
                "to_entity_id": to_entity_id,
                "force": force,
                "conflicting_alias_ids_to_keep": conflicting_alias_ids_to_keep,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/entity/merge", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def create_or_update_entity_alias(
        self,
        name,
        canonical_id,
        mount_accessor,
        alias_id=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create a new alias for an entity.

        Supported methods:
            POST: /{mount_point}/entity-alias. Produces: 200 application/json

        :param name: Name of the alias. Name should be the identifier of the client in the authentication source. For
            example, if the alias belongs to userpass backend, the name should be a valid username within userpass
            backend. If alias belongs to GitHub, it should be the GitHub username.
        :type name: str | unicode
        :param alias_id: ID of the entity alias. If set, updates the  corresponding entity alias.
        :type alias_id: str | unicode
        :param canonical_id: Entity ID to which this alias belongs to.
        :type canonical_id: str | unicode
        :param mount_accessor: Accessor of the mount to which the alias should belong to.
        :type mount_accessor: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "id": alias_id,
                "name": name,
                "canonical_id": canonical_id,
                "mount_accessor": mount_accessor,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/entity-alias", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_entity_alias(self, alias_id, mount_point=DEFAULT_MOUNT_POINT):
        """Query the entity alias by its identifier.

        Supported methods:
            GET: /{mount_point}/entity-alias/id/{id}. Produces: 200 application/json

        :param alias_id: Identifier of entity alias.
        :type alias_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/entity-alias/id/{id}",
            mount_point=mount_point,
            id=alias_id,
        )
        return self._adapter.get(
            url=api_path,
        )

    def update_entity_alias(
        self,
        alias_id,
        name,
        canonical_id,
        mount_accessor,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Update an existing entity alias.

        Supported methods:
            POST: /{mount_point}/entity-alias/id/{id}. Produces: 200 application/json

        :param alias_id: Identifier of the entity alias.
        :type alias_id: str | unicode
        :param name: Name of the alias. Name should be the identifier of the client in the authentication source. For
            example, if the alias belongs to userpass backend, the name should be a valid username within userpass
            backend. If alias belongs to GitHub, it should be the GitHub username.
        :type name: str | unicode
        :param canonical_id: Entity ID to which this alias belongs to.
        :type canonical_id: str | unicode
        :param mount_accessor: Accessor of the mount to which the alias should belong to.
        :type mount_accessor: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response where available, otherwise the generic response object, of the request.
        :rtype: dict | requests.Response
        """
        params = utils.remove_nones(
            {
                "name": name,
                "canonical_id": canonical_id,
                "mount_accessor": mount_accessor,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/entity-alias/id/{id}",
            mount_point=mount_point,
            id=alias_id,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def list_entity_aliases(self, method="LIST", mount_point=DEFAULT_MOUNT_POINT):
        """List available entity aliases by their identifiers.

        :param method: Supported methods:
            LIST: /{mount_point}/entity-alias/id. Produces: 200 application/json
            GET: /{mount_point}/entity-alias/id?list=true. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The the JSON response of the request.
        :rtype: dict
        """

        if method == "LIST":
            api_path = utils.format_url(
                "/v1/{mount_point}/entity-alias/id", mount_point=mount_point
            )
            response = self._adapter.list(
                url=api_path,
            )

        elif method == "GET":
            api_path = utils.format_url(
                "/v1/{mount_point}/entity-alias/id?list=true", mount_point=mount_point
            )
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; LIST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response

    def delete_entity_alias(self, alias_id, mount_point=DEFAULT_MOUNT_POINT):
        """Delete a entity alias.

        Supported methods:
            DELETE: /{mount_point}/entity-alias/id/{alias_id}. Produces: 204 (empty body)

        :param alias_id: Identifier of the entity.
        :type alias_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/entity-alias/id/{id}",
            mount_point=mount_point,
            id=alias_id,
        )
        return self._adapter.delete(
            url=api_path,
        )

    @staticmethod
    def validate_member_id_params_for_group_type(
        group_type, params, member_group_ids, member_entity_ids
    ):
        """Determine whether member ID parameters can be sent with a group create / update request.

        These parameters are only allowed for the internal group type. If they're set for an external group type, Vault
        returns a "error" response.

        :param group_type: Type of the group, internal or external
        :type group_type: str | unicode
        :param params: Params dict to conditionally add the member entity/group ID's to.
        :type params: dict
        :param member_group_ids:  Group IDs to be assigned as group members.
        :type member_group_ids: str | unicode
        :param member_entity_ids: Entity IDs to be assigned as  group members.
        :type member_entity_ids: str | unicode
        :return: Params dict with conditionally added member entity/group ID's.
        :rtype: dict
        """
        if group_type == "external":
            if member_entity_ids is not None:
                logger.warning(
                    "InvalidRequest: member entities can't be set manually for external groups ignoring member_entity_ids argument."
                )
        else:
            params["member_entity_ids"] = member_entity_ids

        if group_type == "external":
            if member_group_ids is not None:
                logger.warning(
                    "InvalidRequest: member groups can't be set for external groups; ignoring member_group_ids argument."
                )
        else:
            params["member_group_ids"] = member_group_ids

        return params

    def create_or_update_group(
        self,
        name,
        group_id=None,
        group_type="internal",
        metadata=None,
        policies=None,
        member_group_ids=None,
        member_entity_ids=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create or update a Group.

        Supported methods:
            POST: /{mount_point}/group. Produces: 200 application/json

        :param name: Name of the group.
        :type name: str | unicode
        :param group_id: ID of the group. If set, updates the corresponding existing group.
        :type group_id: str | unicode
        :param group_type: Type of the group, internal or external. Defaults to internal.
        :type group_type: str | unicode
        :param metadata: Metadata to be associated with the group.
        :type metadata: dict
        :param policies: Policies to be tied to the group.
        :type policies: str | unicode
        :param member_group_ids:  Group IDs to be assigned as group members.
        :type member_group_ids: str | unicode
        :param member_entity_ids: Entity IDs to be assigned as  group members.
        :type member_entity_ids: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response where available, otherwise the generic response object, of the request.
        :rtype: dict | requests.Response
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )
        if group_type not in ALLOWED_GROUP_TYPES:
            error_msg = 'unsupported group_type argument provided "{arg}", allowed values: ({allowed_values})'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=group_type,
                    allowed_values=ALLOWED_GROUP_TYPES,
                )
            )
        params = utils.remove_nones(
            {
                "id": group_id,
                "name": name,
                "type": group_type,
                "metadata": metadata,
                "policies": policies,
            }
        )

        Identity.validate_member_id_params_for_group_type(
            group_type=group_type,
            params=params,
            member_group_ids=member_group_ids,
            member_entity_ids=member_entity_ids,
        )

        api_path = utils.format_url("/v1/{mount_point}/group", mount_point=mount_point)
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_group(self, group_id, mount_point=DEFAULT_MOUNT_POINT):
        """Query the group by its identifier.

        Supported methods:
            GET: /{mount_point}/group/id/{id}. Produces: 200 application/json

        :param group_id: Identifier of the group.
        :type group_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/group/id/{id}",
            mount_point=mount_point,
            id=group_id,
        )
        return self._adapter.get(
            url=api_path,
        )

    def update_group(
        self,
        group_id,
        name,
        group_type="internal",
        metadata=None,
        policies=None,
        member_group_ids=None,
        member_entity_ids=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Update an existing group.

        Supported methods:
            POST: /{mount_point}/group/id/{id}. Produces: 200 application/json

        :param group_id: Identifier of the entity.
        :type group_id: str | unicode
        :param name: Name of the group.
        :type name: str | unicode
        :param group_type: Type of the group, internal or external. Defaults to internal.
        :type group_type: str | unicode
        :param metadata: Metadata to be associated with the group.
        :type metadata: dict
        :param policies: Policies to be tied to the group.
        :type policies: str | unicode
        :param member_group_ids:  Group IDs to be assigned as group members.
        :type member_group_ids: str | unicode
        :param member_entity_ids: Entity IDs to be assigned as group members.
        :type member_entity_ids: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response where available, otherwise the generic response object, of the request.
        :rtype: dict | requests.Response
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )
        if group_type not in ALLOWED_GROUP_TYPES:
            error_msg = 'unsupported group_type argument provided "{arg}", allowed values: ({allowed_values})'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=group_type,
                    allowed_values=ALLOWED_GROUP_TYPES,
                )
            )
        params = utils.remove_nones(
            {
                "name": name,
                "type": group_type,
                "metadata": metadata,
                "policies": policies,
            }
        )

        Identity.validate_member_id_params_for_group_type(
            group_type=group_type,
            params=params,
            member_group_ids=member_group_ids,
            member_entity_ids=member_entity_ids,
        )

        api_path = utils.format_url(
            "/v1/{mount_point}/group/id/{id}",
            mount_point=mount_point,
            id=group_id,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def delete_group(self, group_id, mount_point=DEFAULT_MOUNT_POINT):
        """Delete a group.

        Supported methods:
            DELETE: /{mount_point}/group/id/{id}. Produces: 204 (empty body)

        :param group_id: Identifier of the entity.
        :type group_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/group/id/{id}",
            mount_point=mount_point,
            id=group_id,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def list_groups(self, method="LIST", mount_point=DEFAULT_MOUNT_POINT):
        """List available groups by their identifiers.

        :param method: Supported methods:
            LIST: /{mount_point}/group/id. Produces: 200 application/json
            GET: /{mount_point}/group/id?list=true. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """

        if method == "LIST":
            api_path = utils.format_url(
                "/v1/{mount_point}/group/id", mount_point=mount_point
            )
            response = self._adapter.list(
                url=api_path,
            )

        elif method == "GET":
            api_path = utils.format_url(
                "/v1/{mount_point}/group/id?list=true", mount_point=mount_point
            )
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; LIST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response

    def list_groups_by_name(self, method="LIST", mount_point=DEFAULT_MOUNT_POINT):
        """List available groups by their names.

        :param method: Supported methods:
            LIST: /{mount_point}/group/name. Produces: 200 application/json
            GET: /{mount_point}/group/name?list=true. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """

        if method == "LIST":
            api_path = utils.format_url(
                "/v1/{mount_point}/group/name", mount_point=mount_point
            )
            response = self._adapter.list(
                url=api_path,
            )

        elif method == "GET":
            api_path = utils.format_url(
                "/v1/{mount_point}/group/name?list=true", mount_point=mount_point
            )
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; LIST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response

    def create_or_update_group_by_name(
        self,
        name,
        group_type="internal",
        metadata=None,
        policies=None,
        member_group_ids=None,
        member_entity_ids=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create or update a group by its name.

        Supported methods:
            POST: /{mount_point}/group/name/{name}. Produces: 200 application/json

        :param name: Name of the group.
        :type name: str | unicode
        :param group_type: Type of the group, internal or external. Defaults to internal.
        :type group_type: str | unicode
        :param metadata: Metadata to be associated with the group.
        :type metadata: dict
        :param policies: Policies to be tied to the group.
        :type policies: str | unicode
        :param member_group_ids: Group IDs to be assigned as group members.
        :type member_group_ids: str | unicode
        :param member_entity_ids: Entity IDs to be assigned as group members.
        :type member_entity_ids: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )
        if group_type not in ALLOWED_GROUP_TYPES:
            error_msg = 'unsupported group_type argument provided "{arg}", allowed values: ({allowed_values})'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=group_type,
                    allowed_values=ALLOWED_GROUP_TYPES,
                )
            )
        params = utils.remove_nones(
            {
                "type": group_type,
                "metadata": metadata,
                "policies": policies,
            }
        )
        if group_type != "external":
            external_only_params = utils.remove_nones(
                {
                    "member_group_ids": member_group_ids,
                    "member_entity_ids": member_entity_ids,
                }
            )
            params.update(external_only_params)
        api_path = utils.format_url(
            "/v1/{mount_point}/group/name/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_group_by_name(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Query a group by its name.

        Supported methods:
            GET: /{mount_point}/group/name/{name}. Produces: 200 application/json

        :param name: Name of the group.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/group/name/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_group_by_name(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete a group, given its name.

        Supported methods:
            DELETE: /{mount_point}/group/name/{name}. Produces: 204 (empty body)

        :param name: Name of the group.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/group/name/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def create_or_update_group_alias(
        self,
        name,
        alias_id=None,
        mount_accessor=None,
        canonical_id=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Creates or update a group alias.

        Supported methods:
            POST: /{mount_point}/group-alias. Produces: 200 application/json

        :param alias_id: ID of the group alias. If set, updates the corresponding existing group alias.
        :type alias_id: str | unicode
        :param name: Name of the group alias.
        :type name: str | unicode
        :param mount_accessor: Mount accessor to which this alias belongs to
        :type mount_accessor: str | unicode
        :param canonical_id: ID of the group to which this is an alias.
        :type canonical_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "id": alias_id,
                "name": name,
                "mount_accessor": mount_accessor,
                "canonical_id": canonical_id,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/group-alias", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def update_group_alias(
        self,
        entity_id,
        name,
        mount_accessor=None,
        canonical_id=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Update an existing group alias.

        Supported methods:
            POST: /{mount_point}/group-alias/id/{id}. Produces: 200 application/json

        :param entity_id: ID of the group alias.
        :type entity_id: str | unicode
        :param name: Name of the group alias.
        :type name: str | unicode
        :param mount_accessor: Mount accessor to which this alias belongs
            toMount accessor to which this alias belongs to.
        :type mount_accessor: str | unicode
        :param canonical_id: ID of the group to which this is an alias.
        :type canonical_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "name": name,
                "mount_accessor": mount_accessor,
                "canonical_id": canonical_id,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/group-alias/id/{id}",
            mount_point=mount_point,
            id=entity_id,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_group_alias(self, alias_id, mount_point=DEFAULT_MOUNT_POINT):
        """Query the group alias by its identifier.

        Supported methods:
            GET: /{mount_point}/group-alias/id/:id. Produces: 200 application/json

        :param alias_id: ID of the group alias.
        :type alias_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/group-alias/id/{id}",
            mount_point=mount_point,
            id=alias_id,
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_group_alias(self, entity_id, mount_point=DEFAULT_MOUNT_POINT):
        """Delete a group alias.

        Supported methods:
            DELETE: /{mount_point}/group-alias/id/{id}. Produces: 204 (empty body)

        :param entity_id: ID of the group alias.
        :type entity_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/group-alias/id/{id}",
            mount_point=mount_point,
            id=entity_id,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def list_group_aliases(self, method="LIST", mount_point=DEFAULT_MOUNT_POINT):
        """List available group aliases by their identifiers.

        :param method: Supported methods:
            LIST: /{mount_point}/group-alias/id. Produces: 200 application/json
            GET: /{mount_point}/group-alias/id?list=true. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The "data" key from the JSON response of the request.
        :rtype: dict
        """

        if method == "LIST":
            api_path = utils.format_url(
                "/v1/{mount_point}/group-alias/id", mount_point=mount_point
            )
            response = self._adapter.list(
                url=api_path,
            )
        elif method == "GET":
            api_path = utils.format_url(
                "/v1/{mount_point}/group-alias/id?list=true", mount_point=mount_point
            )
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; LIST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response

    def lookup_entity(
        self,
        name=None,
        entity_id=None,
        alias_id=None,
        alias_name=None,
        alias_mount_accessor=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Query an entity based on the given criteria.

        The criteria can be name, id, alias_id, or a combination of alias_name and alias_mount_accessor.

        Supported methods:
            POST: /{mount_point}/lookup/entity. Produces: 200 application/json

        :param name: Name of the entity.
        :type name: str | unicode
        :param entity_id: ID of the entity.
        :type entity_id: str | unicode
        :param alias_id: ID of the alias.
        :type alias_id: str | unicode
        :param alias_name: Name of the alias. This should be supplied in conjunction with alias_mount_accessor.
        :type alias_name: str | unicode
        :param alias_mount_accessor: Accessor of the mount to which the alias belongs to. This should be supplied in conjunction with alias_name.
        :type alias_mount_accessor: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request if a entity / entity alias is found in the lookup, None otherwise.
        :rtype: dict | None
        """
        params = {}
        if name is not None:
            params["name"] = name
        elif entity_id is not None:
            params["id"] = entity_id
        elif alias_id is not None:
            params["alias_id"] = alias_id
        elif alias_name is not None and alias_mount_accessor is not None:
            params["alias_name"] = alias_name
            params["alias_mount_accessor"] = alias_mount_accessor
        api_path = utils.format_url(
            "/v1/{mount_point}/lookup/entity", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def lookup_group(
        self,
        name=None,
        group_id=None,
        alias_id=None,
        alias_name=None,
        alias_mount_accessor=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Query a group based on the given criteria.

        The criteria can be name, id, alias_id, or a combination of alias_name and alias_mount_accessor.

        Supported methods:
            POST: /{mount_point}/lookup/group. Produces: 200 application/json

        :param name: Name of the group.
        :type name: str | unicode
        :param group_id: ID of the group.
        :type group_id: str | unicode
        :param alias_id: ID of the alias.
        :type alias_id: str | unicode
        :param alias_name: Name of the alias. This should be supplied in conjunction with alias_mount_accessor.
        :type alias_name: str | unicode
        :param alias_mount_accessor: Accessor of the mount to which the alias belongs to. This should be supplied in conjunction with alias_name.
        :type alias_mount_accessor: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request if a group / group alias is found in the lookup, None otherwise.
        :rtype: dict | None
        """
        params = {}
        if name is not None:
            params["name"] = name
        elif group_id is not None:
            params["id"] = group_id
        elif alias_id is not None:
            params["alias_id"] = alias_id
        elif alias_name is not None and alias_mount_accessor is not None:
            params["alias_name"] = alias_name
            params["alias_mount_accessor"] = alias_mount_accessor
        api_path = utils.format_url(
            "/v1/{mount_point}/lookup/group", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def configure_tokens_backend(self, issuer=None, mount_point=DEFAULT_MOUNT_POINT):
        """Update configurations for OIDC-compliant identity tokens issued by Vault.

        Supported methods:
            POST: {mount_point}/oidc/config.

        :param issuer: Issuer URL to be used in the iss claim of the token. If not set, Vault's api_addr will be used.
            The issuer is a case sensitive URL using the https scheme that contains scheme, host, and optionally, port
            number and path components, but no query or fragment components.
        :type issuer: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The a dict or the response of the configure_tokens_backend request. dict returned when messages
            are included in the response body.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "issuer": issuer,
            }
        )

        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/config",
            mount_point=mount_point,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_tokens_backend_configuration(self, mount_point=DEFAULT_MOUNT_POINT):
        """Query vault identity tokens configurations.

        Supported methods:
            GET: {mount_point}/oidc/config.

        :return: The response of the read_tokens_backend_configuration request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/config",
            mount_point=mount_point,
        )
        return self._adapter.get(
            url=api_path,
        )

    def create_named_key(
        self,
        name,
        rotation_period="24h",
        verification_ttl="24h",
        allowed_client_ids=None,
        algorithm="RS256",
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create or update a named key which is used by a role to sign tokens.

        Supported methods:
            POST: {mount_point}/oidc/key/:name.

        :param name: Name of the named key.
        :type name: str | unicode
        :param rotation_period: How often to generate a new signing key. Can be specified as a number of seconds or as
            a time string like "30m" or "6h".
        :type rotation_period: str | unicode
        :param verification_ttl: Controls how long the public portion of a signing key will be available for
            verification after being rotated.
        :type verification_ttl: str | unicode
        :param allowed_client_ids: List of role client ids allowed to use this key for signing.
            If empty, no roles are allowed. If "*", all roles are allowed.
        :type allowed_client_ids: list
        :param algorithm: Signing algorithm to use. Allowed values are: RS256 (default), RS384, RS512, ES256, ES384,
            ES512, EdDSA.
        :type algorithm: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the create_a_named_key request.
        :rtype: dict
        """
        params = {
            "name": name,
            "rotation_period": rotation_period,
            "verification_ttl": verification_ttl,
            "allowed_client_ids": allowed_client_ids,
            "algorithm": algorithm,
        }

        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/key/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_named_key(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Query a named key and returns its configurations.

        Supported methods:
            GET: {mount_point}/oidc/key/:name.

        :param name: Name of the key.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the read_a_named_key request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/key/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_named_key(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete a named key.

        Supported methods:
            DELETE: {mount_point}/oidc/key/:name.

        :param name: Name of the key.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the delete_a_named_key request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/key/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def list_named_keys(self, mount_point=DEFAULT_MOUNT_POINT):
        """List all named keys.

        Supported methods:
            LIST: {mount_point}/oidc/key.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the list_named_keys request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/key",
            mount_point=mount_point,
        )
        return self._adapter.list(
            url=api_path,
        )

    def rotate_named_key(self, name, verification_ttl, mount_point=DEFAULT_MOUNT_POINT):
        """Rotate a named key.

        Supported methods:
            POST: {mount_point}/oidc/key/:name/rotate.

        :param name: Name of the key to be rotated.
        :type name: str | unicode
        :param verification_ttl: Controls how long the public portion of the key will be available for verification after being rotated.
            Setting verification_ttl here will override the verification_ttl set on the key.
        :type verification_ttl: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the rotate_a_named_key request.
        :rtype: dict
        """
        params = {
            "verification_ttl": verification_ttl,
        }
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/key/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def create_or_update_role(
        self,
        name,
        key,
        template=None,
        client_id=None,
        ttl="24h",
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create or update a role.

        ID tokens are generated against a role and signed against a named key.

        Supported methods:
            POST: {mount_point}/oidc/role/:name.

        :param name: Name of the role.
        :type name: str | unicode
        :param key: A configured named key, the key must already exist.
        :type key: str | unicode
        :param template: The template string to use for generating tokens. This may be in stringified JSON or
            base64 format.
        :type template: str | unicode
        :param client_id: Optional client ID. A random ID will be generated if left unset.
        :type client_id: str | unicode
        :param ttl: TTL of the tokens generated against the role. Can be specified as a number of seconds or as a time
            string like "30m" or "6h".
        :type ttl: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the create_or_update_a_role request.
        :rtype: dict
        """
        params = utils.remove_nones(
            {
                "key": key,
                "template": template,
                "client_id": client_id,
                "ttl": ttl,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/role/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Query a role and returns its configuration.

        Supported methods:
            GET: {mount_point}/oidc/role/:name.

        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the read_a_role request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/role/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Deletes a role.

        Supported methods:
            DELETE: {mount_point}/oidc/role/:name.


        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the delete_a_role request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/role/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def list_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        This endpoint will list all signing keys.

        Supported methods:
            LIST: {mount_point}/oidc/role.


        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the list_roles request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/role",
            mount_point=mount_point,
        )
        return self._adapter.list(
            url=api_path,
        )

    def generate_signed_id_token(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Generate a signed ID (OIDC) token.

        Supported methods:
            GET: {mount_point}/oidc/token/:name.

        :param name: The name of the role against which to generate a signed ID token
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the generate_a_signed_id_token request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/token/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def introspect_signed_id_token(
        self, token, client_id=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Verify the authenticity and active state of a signed ID token.

        Supported methods:
            POST: {mount_point}/oidc/introspect.


        :param token: A signed OIDC compliant ID token
        :type token: str | unicode
        :param client_id: Specifying the client ID optimizes validation time
        :type client_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the introspect_a_signed_id_token request.
        :rtype: dict
        """
        params = utils.remove_nones(
            {
                "token": token,
                "client_id": client_id,
            }
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/introspect",
            mount_point=mount_point,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_well_known_configurations(self, mount_point=DEFAULT_MOUNT_POINT):
        """Retrieve a set of claims about the identity tokens' configuration.

        The response is a compliant OpenID Provider Configuration Response.

        Supported methods:
            GET: {mount_point}/oidc/.well-known/openid-configuration.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the read_well_known_configurations request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/.well-known/openid-configuration",
            mount_point=mount_point,
        )
        return self._adapter.get(
            url=api_path,
        )

    def read_active_public_keys(self, mount_point=DEFAULT_MOUNT_POINT):
        """Retrieve the public portion of named keys.

        Clients can use this to validate the authenticity of an identity token.

        Supported methods:
            GET: {mount_point}/oidc/.well-known/openid-configuration.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the read_active_public_keys request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/oidc/.well-known/keys",
            mount_point=mount_point,
        )
        return self._adapter.get(
            url=api_path,
        )
