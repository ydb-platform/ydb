# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
from collections.abc import Awaitable, Callable
from functools import partial
from pprint import pformat
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from kubernetes_asyncio.dynamic.client import DynamicClient


class Resource:
    """Represents an API resource type, containing the information required to build urls for requests"""

    def __init__(
        self,
        prefix: str | None = None,
        group: str | None = None,
        api_version: str | None = None,
        kind: str | None = None,
        namespaced: bool = False,
        verbs: str | None = None,
        name: str | None = None,
        preferred: bool = False,
        client: "DynamicClient | None" = None,
        singular_name: str | None = None,
        short_names: str | None = None,
        categories: str | None = None,
        subresources: dict | None = None,
        **kwargs,
    ):
        if None in (api_version, kind, prefix):
            raise ValueError("At least prefix, kind, and api_version must be provided")

        self.prefix = prefix
        self.group = group
        self.api_version = api_version
        self.kind = kind
        self.namespaced = namespaced
        self.verbs = verbs
        self.name = name
        self.preferred = preferred
        self.client = client
        self.singular_name = singular_name or (name[:-1] if name else "")
        self.short_names = short_names
        self.categories = categories
        self.subresources = {
            k: Subresource(self, **v) for k, v in (subresources or {}).items()
        }

        self.extra_args = kwargs

    def to_dict(self) -> dict[str, Any]:
        d = {
            "_type": "Resource",
            "prefix": self.prefix,
            "group": self.group,
            "api_version": self.api_version,
            "kind": self.kind,
            "namespaced": self.namespaced,
            "verbs": self.verbs,
            "name": self.name,
            "preferred": self.preferred,
            "singularName": self.singular_name,
            "shortNames": self.short_names,
            "categories": self.categories,
            "subresources": {k: sr.to_dict() for k, sr in self.subresources.items()},
        }
        d.update(self.extra_args)
        return d

    @property
    def group_version(self) -> str | None:
        if self.group:
            return f"{self.group}/{self.api_version}"
        return self.api_version

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.group_version}/{self.name})>"

    @property
    def urls(self) -> dict[str, str]:
        full_prefix = f"{self.prefix}/{self.group_version}"
        resource_name = self.name.lower() if self.name else ""
        return {
            "base": f"/{full_prefix}/{resource_name}",
            "namespaced_base": f"/{full_prefix}/namespaces/{{namespace}}/{resource_name}",
            "full": f"/{full_prefix}/{resource_name}/{{name}}",
            "namespaced_full": f"/{full_prefix}/namespaces/{{namespace}}/{resource_name}/{{name}}",
        }

    def path(self, name: str | None = None, namespace: str | None = None) -> str:
        url_type = []
        path_params = {}
        if self.namespaced and namespace:
            url_type.append("namespaced")
            path_params["namespace"] = namespace
        if name:
            url_type.append("full")
            path_params["name"] = name
        else:
            url_type.append("base")
        return self.urls["_".join(url_type)].format(**path_params)

    def __getattr__(self, name: str) -> Callable[..., "Awaitable[ResourceInstance]"]:
        if name in self.subresources:
            return self.subresources[name]  # type: ignore
        return partial(getattr(self.client, name), self)


class ResourceList(Resource):
    """Represents a list of API objects"""

    def __init__(
        self,
        client: "DynamicClient",
        group: str | None = None,
        api_version: str | None = None,
        base_kind: str | None = None,
        kind: str | None = None,
        base_resource_lookup=None,
    ):
        self.client = client
        self.group = group or ""
        self.api_version = api_version or "v1"
        self.kind = kind or f"{base_kind}List"
        self.base_kind = base_kind or ""
        self.base_resource_lookup = base_resource_lookup
        self.__base_resource = None

    async def _ainit(self):
        if self.base_kind and self.api_version and self.group:
            self.__base_resource = await self.client.resources.get(
                group=self.group, api_version=self.api_version, kind=self.base_kind
            )

    @classmethod
    async def create_list(
        cls, client, group="", api_version="v1", base_kind="", kind=None
    ):
        self = cls(
            client=client,
            group=group,
            api_version=api_version,
            base_kind=base_kind,
            kind=kind,
        )
        await self._ainit()
        return self

    # TODO: This code appears to be unused, or at least untested
    async def base_resource(self):
        if self.__base_resource:
            return self.__base_resource
        elif self.base_resource_lookup:
            self.__base_resource = await self.client.resources.get(
                **self.base_resource_lookup
            )
            return self.__base_resource
        elif self.base_kind:
            self.__base_resource = await self.client.resources.get(
                group=self.group, api_version=self.api_version, kind=self.base_kind
            )
            return self.__base_resource
        return None

    # TODO: This code appears to be unused, or at least untested
    async def _items_to_resources(self, body):
        """Takes a List body and return a dictionary with the following structure:
        {
            'api_version': str,
            'kind': str,
            'items': [{
                'resource': Resource,
                'name': str,
                'namespace': str,
            }]
        }
        """
        if body is None:
            raise ValueError(
                "You must provide a body when calling methods on a ResourceList"
            )

        api_version = body["apiVersion"]
        kind = body["kind"]
        items = body.get("items")
        if not items:
            raise ValueError(
                "The `items` field in the body must be populated when calling methods on a ResourceList"
            )

        if self.kind != kind:
            raise ValueError(
                f"Methods on a {self.kind} must be called with a body containing the same kind."
                f" Received {kind} instead"
            )

        return {
            "api_version": api_version,
            "kind": kind,
            "items": [await self._item_to_resource(item) for item in items],
        }

    # TODO: This code appears to be unused, or at least untested
    async def _item_to_resource(self, item):
        metadata = item.get("metadata", {})
        resource = await self.base_resource()
        if not resource:
            api_version = item.get("apiVersion", self.api_version)
            kind = item.get("kind", self.base_kind)
            resource = await self.client.resources.get(
                api_version=api_version, kind=kind
            )
        return {
            "resource": resource,
            "definition": item,
            "name": metadata.get("name"),
            "namespace": metadata.get("namespace"),
        }

    async def get(self, body, name=None, namespace=None, **kwargs):
        if name:
            raise ValueError(
                "Operations on ResourceList objects do not support the `name` argument"
            )
        resource_list = await self._items_to_resources(body)
        response = copy.deepcopy(body)

        response["items"] = [
            item["resource"]
            .get(name=item["name"], namespace=item["namespace"] or namespace, **kwargs)
            .to_dict()
            for item in resource_list["items"]
        ]
        return ResourceInstance(self, response)

    async def delete(self, body, name=None, namespace=None, **kwargs):
        if name:
            raise ValueError(
                "Operations on ResourceList objects do not support the `name` argument"
            )
        resource_list = await self._items_to_resources(body)
        response = copy.deepcopy(body)

        response["items"] = [
            item["resource"]
            .delete(
                name=item["name"], namespace=item["namespace"] or namespace, **kwargs
            )
            .to_dict()
            for item in resource_list["items"]
        ]
        return ResourceInstance(self, response)

    async def verb_mapper(self, verb, body, **kwargs):
        resource_list = await self._items_to_resources(body)
        response = copy.deepcopy(body)
        response["items"] = [
            getattr(item["resource"], verb)(body=item["definition"], **kwargs).to_dict()
            for item in resource_list["items"]
        ]
        return ResourceInstance(self, response)

    async def create(self, *args, **kwargs):
        return await self.verb_mapper("create", *args, **kwargs)

    async def replace(self, *args, **kwargs):
        return await self.verb_mapper("replace", *args, **kwargs)

    async def patch(self, *args, **kwargs):
        return await self.verb_mapper("patch", *args, **kwargs)

    def to_dict(self):
        return {
            "_type": "ResourceList",
            "group": self.group,
            "api_version": self.api_version,
            "kind": self.kind,
            "base_kind": self.base_kind,
        }

    # This code is not executed in any test scenario - is it needed?
    def __getattr__(self, name: str) -> Any:
        if self.base_resource():
            return getattr(self.base_resource(), name)
        return None


class Subresource(Resource):
    """Represents a subresource of an API resource. This generally includes operations
    like scale, as well as status objects for an instantiated resource
    """

    def __init__(self, parent, **kwargs):  # noqa
        # super().__init__()
        self.parent = parent
        self.prefix = parent.prefix
        self.group = parent.group
        self.api_version = parent.api_version
        self.kind = kwargs.pop("kind")
        self.name = kwargs.pop("name")
        self.subresource = kwargs.pop("subresource", None) or self.name.split("/")[1]
        self.namespaced = kwargs.pop("namespaced", False)
        self.verbs = kwargs.pop("verbs", None)
        self.extra_args = kwargs

    # TODO(fabianvf): Determine proper way to handle differences between resources + subresources
    async def create(
        self,
        body=None,
        name: str | None = None,
        namespace: str | None = None,
        **kwargs: Any,
    ):
        name = name
        if name is None:
            name = body.get("metadata", {}).get("name") if body else None
        body = self.parent.client.serialize_body(body)
        if self.parent.namespaced:
            namespace = self.parent.client.ensure_namespace(
                self.parent, namespace, body
            )
        path = self.path(name=name, namespace=namespace)
        return await self.parent.client.request("post", path, body=body, **kwargs)

    @property
    def urls(self) -> dict[str, str]:
        full_prefix = f"{self.prefix}/{self.group_version}"
        return {
            "full": f"/{full_prefix}/{self.parent.name}/{{name}}/{self.subresource}",
            "namespaced_full": f"/{full_prefix}/namespaces/{{namespace}}/{self.parent.name}/{{name}}/{self.subresource}",
        }

    def __getattr__(self, name):
        return partial(getattr(self.parent.client, name), self)

    def to_dict(self) -> dict[str, str]:
        d = {
            "kind": self.kind,
            "name": self.name,
            "subresource": self.subresource,
            "namespaced": self.namespaced,
            "verbs": self.verbs,
        }
        d.update(self.extra_args)
        return d


class ResourceInstance:
    """A parsed instance of an API resource. It exists solely to
    ease interaction with API objects by allowing attributes to
    be accessed with '.' notation.
    """

    def __init__(self, client, instance):
        self.client = client
        # If we have a list of resources, then set the apiVersion and kind of
        # each resource in 'items'
        kind = instance["kind"]
        if kind.endswith("List") and "items" in instance:
            kind = instance["kind"][:-4]
            if instance["items"] is None:
                instance["items"] = []
            for item in instance["items"]:
                if "apiVersion" not in item:
                    item["apiVersion"] = instance["apiVersion"]
                if "kind" not in item:
                    item["kind"] = kind

        self.attributes = self.__deserialize(instance)
        self.__initialised = True

    def __deserialize(self, field):
        if isinstance(field, dict):
            return ResourceField(
                params={k: self.__deserialize(v) for k, v in field.items()}
            )
        elif isinstance(field, (list, tuple)):
            return [self.__deserialize(item) for item in field]
        else:
            return field

    def __serialize(self, field):
        if isinstance(field, ResourceField):
            return {k: self.__serialize(v) for k, v in field.__dict__.items()}
        elif isinstance(field, (list, tuple)):
            return [self.__serialize(item) for item in field]
        elif isinstance(field, ResourceInstance):
            return field.to_dict()
        else:
            return field

    def to_dict(self):
        return self.__serialize(self.attributes)

    def to_str(self):
        return repr(self)

    def __repr__(self):
        return f"ResourceInstance[{self.attributes.kind}]:\n  {'  '.join(yaml.safe_dump(self.to_dict()).splitlines(True))}"

    def __getattr__(self, name):
        if "_ResourceInstance__initialised" not in self.__dict__:
            return super().__getattr__(name)  # type: ignore
        return getattr(self.attributes, name)

    def __setattr__(self, name, value):
        if "_ResourceInstance__initialised" not in self.__dict__:
            return super().__setattr__(name, value)
        elif name in self.__dict__:
            return super().__setattr__(name, value)
        else:
            self.attributes[name] = value

    def __getitem__(self, name):
        return self.attributes[name]

    def __setitem__(self, name, value):
        self.attributes[name] = value

    def __dir__(self):
        return dir(type(self)) + list(self.attributes.__dict__.keys())


class ResourceField:
    """A parsed instance of an API resource attribute. It exists
    solely to ease interaction with API objects by allowing
    attributes to be accessed with '.' notation
    """

    def __init__(self, params):
        self.__dict__.update(**params)

    def __repr__(self):
        return pformat(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __getitem__(self, name: str):
        return self.__dict__.get(name)

    # Here resource.items will return items if available or resource.__dict__.items function if not
    # resource.get will call resource.__dict__.get after attempting resource.__dict__.get('get')
    def __getattr__(self, name):
        return self.__dict__.get(name, getattr(self.__dict__, name, None))

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __dir__(self):
        return dir(type(self)) + list(self.__dict__.keys())

    def __iter__(self):
        yield from self.__dict__.items()

    def to_dict(self) -> dict:
        ret = self.__serialize(self)
        if isinstance(ret, dict):
            return ret
        return {"to_dict": ret}

    def __serialize(self, field) -> dict | list:
        if isinstance(field, ResourceField):
            return {k: self.__serialize(v) for k, v in field.__dict__.items()}
        if isinstance(field, (list, tuple)):
            return [self.__serialize(item) for item in field]
        return field
