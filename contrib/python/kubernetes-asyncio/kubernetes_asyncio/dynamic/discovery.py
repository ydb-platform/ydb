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

import hashlib
import json
import logging
import os
import tempfile
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Generator
from functools import partial
from typing import TYPE_CHECKING, Any

from aiohttp.client_exceptions import ContentTypeError
from urllib3.exceptions import MaxRetryError, ProtocolError

from kubernetes_asyncio import __version__

if TYPE_CHECKING:
    from kubernetes_asyncio.dynamic.client import DynamicClient

from kubernetes_asyncio.dynamic.exceptions import (
    NotFoundError,
    ResourceNotFoundError,
    ResourceNotUniqueError,
    ServiceUnavailableError,
)
from kubernetes_asyncio.dynamic.resource import Resource, ResourceList

DISCOVERY_PREFIX = "apis"
logger = logging.getLogger(__name__)


class ResourceGroup:
    """Helper class for Discoverer container"""

    def __init__(self, preferred, resources=None):
        self.preferred = preferred
        self.resources = resources or {}

    def to_dict(self):
        return {
            "_type": "ResourceGroup",
            "preferred": self.preferred,
            "resources": self.resources,
        }


class Discoverer:
    """
    A convenient container for storing discovered API resources. Allows
    easy searching and retrieval of specific resources.

    Subclasses implement the abstract methods with different loading strategies.
    """

    __version: dict[str, Any]

    def __init__(self, client: "DynamicClient", cache_file: str | None = None) -> None:
        self.client = client
        assert self.client.configuration.host
        default_cache_id = self.client.configuration.host.encode("utf-8")
        try:
            default_cachefile_name = f"osrcp-{hashlib.md5(default_cache_id, usedforsecurity=False).hexdigest()}.json"
        except TypeError:
            # usedforsecurity is only supported in 3.9+
            default_cachefile_name = (
                f"osrcp-{hashlib.md5(default_cache_id).hexdigest()}.json"
            )
        self.__cache_file = cache_file or os.path.join(
            tempfile.gettempdir(), default_cachefile_name
        )
        self._cache: dict[str, dict | str]

    def __await__(self) -> Generator[Any, None, "Discoverer"]:
        async def closure():
            await self.__init_cache()
            return self

        return closure().__await__()

    async def __aenter__(self) -> "Discoverer":
        await self.__init_cache()
        return self

    async def __init_cache(self, refresh: bool = False) -> None:
        if refresh or not os.path.exists(self.__cache_file):
            self._cache = {"library_version": __version__}
            refresh = True
        else:
            try:
                with open(self.__cache_file) as f:
                    self._cache = json.load(f, cls=partial(CacheDecoder, self.client))  # type: ignore
                if self._cache.get("library_version") != __version__:
                    # Version mismatch, need to refresh cache
                    await self.invalidate_cache()
            except Exception as e:
                logger.error("load cache error: %s", e)
                await self.invalidate_cache()
        await self._load_server_info()
        await self.discover()
        if refresh:
            self._write_cache()

    def _write_cache(self) -> None:
        try:
            with open(self.__cache_file, "w") as f:
                json.dump(self._cache, f, cls=CacheEncoder)
        except Exception:
            # Failing to write the cache isn't a big enough error to crash on
            pass

    async def invalidate_cache(self) -> None:
        await self.__init_cache(refresh=True)

    @property
    @abstractmethod
    async def api_groups(self):
        pass

    @abstractmethod
    async def search(
        self,
        prefix: str | None = None,
        group: str | None = None,
        api_version: str | None = None,
        kind: str | None = None,
        **kwargs: Any,
    ) -> list[Resource]:
        pass

    @abstractmethod
    async def discover(self):
        pass

    @property
    def version(self) -> dict[str, Any]:
        return self.__version

    async def default_groups(
        self, request_resources: bool = False
    ) -> dict[str, dict[str, dict[str, ResourceGroup]]]:
        groups = {
            "api": {
                "": {
                    "v1": (
                        ResourceGroup(
                            True,
                            resources=await self.get_resources_for_api_version(
                                "api", "", "v1", True
                            ),
                        )
                        if request_resources
                        else ResourceGroup(True)
                    )
                }
            },
            DISCOVERY_PREFIX: {
                "": {
                    "v1": ResourceGroup(
                        True, resources={"List": [ResourceList(self.client)]}
                    )
                }
            },
        }
        return groups

    async def parse_api_groups(
        self, request_resources: bool = False, update: bool = False
    ) -> dict:
        """Discovers all API groups present in the cluster"""
        if not self._cache.get("resources") or update:
            self._cache["resources"] = self._cache.get("resources", {})
            response = await self.client.request("GET", f"/{DISCOVERY_PREFIX}")
            groups_response = response.groups

            groups = await self.default_groups(request_resources=request_resources)

            for group in groups_response:
                new_group = {}
                for version_raw in group["versions"]:
                    version = version_raw["version"]
                    resource_group = (
                        self._cache.get("resources", {})  # type: ignore
                        .get(DISCOVERY_PREFIX, {})  # type: ignore
                        .get(group["name"], {})
                        .get(version)
                    )
                    preferred = version_raw == group["preferredVersion"]
                    resources = resource_group.resources if resource_group else {}
                    if request_resources:
                        resources = await self.get_resources_for_api_version(
                            DISCOVERY_PREFIX, group["name"], version, preferred
                        )
                    new_group[version] = ResourceGroup(preferred, resources=resources)
                groups[DISCOVERY_PREFIX][group["name"]] = new_group
            self._cache["resources"].update(groups)  # type: ignore
            self._write_cache()

        return self._cache["resources"]  # type: ignore

    async def _load_server_info(self) -> None:
        def just_json(_, serialized):
            return serialized

        if not self._cache.get("version"):
            try:
                self._cache["version"] = {
                    "kubernetes": await self.client.request(
                        "get", "/version", serializer=just_json
                    )
                }
            except (ValueError, MaxRetryError) as e:
                if isinstance(e, MaxRetryError) and not isinstance(
                    e.reason, ProtocolError
                ):
                    raise
                if (
                    self.client.configuration.host is None
                    or not self.client.configuration.host.startswith("https://")
                ):
                    raise ValueError(
                        f"Host value {self.client.configuration.host} should start with https:// when talking to HTTPS endpoint"
                    ) from e
                else:
                    raise

        self.__version = self._cache["version"]  # type: ignore

    async def get_resources_for_api_version(
        self,
        prefix: str | None,
        group: str | None,
        version: str | None,
        preferred: bool,
    ) -> dict[str, list[Resource]]:
        """returns a dictionary of resources associated with provided (prefix, group, version)"""

        resources: dict[str, list[Resource]] = defaultdict(list)
        subresources: dict[str, dict] = {}

        path = "/".join(filter(None, [prefix, group, version]))
        try:
            response = await self.client.request("GET", path)
            resources_response = response.resources or []
        except (ServiceUnavailableError, ContentTypeError):
            # Handle both service unavailable errors and content type errors
            # (e.g., when server returns 503 with text/plain)
            resources_response = []

        resources_raw = list(filter(lambda r: "/" not in r["name"], resources_response))
        subresources_raw = list(filter(lambda r: "/" in r["name"], resources_response))
        for subresource in subresources_raw:
            # Handle resources with >2 parts in their name
            resource, name = subresource["name"].split("/", 1)
            if not subresources.get(resource):
                subresources[resource] = {}
            subresources[resource][name] = subresource

        for resource in resources_raw:
            # Prevent duplicate keys
            for key in ("prefix", "group", "api_version", "client", "preferred"):
                resource.pop(key, None)

            resourceobj = Resource(
                prefix=prefix,
                group=group,
                api_version=version,
                client=self.client,
                preferred=preferred,
                subresources=subresources.get(resource["name"]),
                **resource,
            )
            resources[resource["kind"]].append(resourceobj)

            resource_list = ResourceList(
                self.client,
                group=group,
                api_version=version,
                base_kind=resource["kind"],
            )
            resources[resource_list.kind].append(resource_list)  # type: ignore
        return resources

    async def get(self, **kwargs: Any) -> Resource:
        """Same as search, but will throw an error if there are multiple or no
        results. If there are multiple results and only one is an exact match
        on api_version, that resource will be returned.
        """
        results = await self.search(**kwargs)
        # If there are multiple matches, prefer exact matches on api_version
        if len(results) > 1 and kwargs.get("api_version"):
            results = [
                result
                for result in results
                if result.group_version == kwargs["api_version"]
            ]
        # If there are multiple matches, prefer non-List kinds
        if len(results) > 1 and not all(isinstance(x, ResourceList) for x in results):
            results = [
                result for result in results if not isinstance(result, ResourceList)
            ]
        if len(results) == 1:
            return results[0]
        elif not results:
            raise ResourceNotFoundError(f"No matches found for {kwargs}")
        else:
            raise ResourceNotUniqueError(
                f"Multiple matches found for {kwargs}: {results}"
            )


class LazyDiscoverer(Discoverer):
    """A convenient container for storing discovered API resources. Allows
    easy searching and retrieval of specific resources.

    Resources for the cluster are loaded lazily.
    """

    def __init__(self, client, cache_file):
        self.__resources = None
        Discoverer.__init__(self, client, cache_file)
        self.__update_cache = False

    async def discover(self):
        self.__resources = await self.parse_api_groups(request_resources=False)

    def __maybe_write_cache(self):
        if self.__update_cache:
            self._write_cache()
            self.__update_cache = False

    @property
    async def api_groups(self):
        groups = await self.parse_api_groups(request_resources=False, update=True)
        return groups["apis"].keys()

    async def search(
        self,
        prefix: str | None = None,
        group: str | None = None,
        api_version: str | None = None,
        kind: str | None = None,
        **kwargs: Any,
    ) -> list[Resource]:
        # In first call, ignore ResourceNotFoundError and set default value for results
        try:
            results = await self.__search(
                self.__build_search(prefix, group, api_version, kind, **kwargs),
                self.__resources,
                [],
            )
        except ResourceNotFoundError:
            results = []
        if not results:
            await self.invalidate_cache()
            results = await self.__search(
                self.__build_search(prefix, group, api_version, kind, **kwargs),
                self.__resources,
                [],
            )
        self.__maybe_write_cache()
        return results

    async def __search(self, parts, resources, req_params):
        part = parts[0]
        if part != "*":
            resource_part = resources.get(part)
            if not resource_part:
                return []
            elif isinstance(resource_part, ResourceGroup):
                if len(req_params) != 2:
                    raise ValueError(
                        f"prefix and group params should be present, have {req_params}"
                    )
                # Check if we've requested resources for this group
                if not resource_part.resources:
                    prefix, group, version = req_params[0], req_params[1], part
                    try:
                        resource_part.resources = (
                            await self.get_resources_for_api_version(
                                prefix, group, part, resource_part.preferred
                            )
                        )
                    except NotFoundError as e:
                        raise ResourceNotFoundError from e

                    self._cache["resources"][prefix][group][version] = resource_part  # type: ignore
                    self.__update_cache = True
                return await self.__search(
                    parts[1:], resource_part.resources, req_params
                )
            elif isinstance(resource_part, dict):
                # In this case parts [0] will be a specified prefix, group, version
                # as we recurse
                return await self.__search(
                    parts[1:], resource_part, req_params + [part]
                )
            else:
                if parts[1] != "*" and isinstance(parts[1], dict):
                    for _resource in resource_part:
                        for term, value in parts[1].items():
                            if getattr(_resource, term) == value:
                                return [_resource]
                    return []
                else:
                    return resource_part
        else:
            matches = []
            for key in resources.keys():
                matches.extend(
                    await self.__search([key] + parts[1:], resources, req_params)
                )
            return matches

    @staticmethod
    def __build_search(prefix=None, group=None, api_version=None, kind=None, **kwargs):
        if not group and api_version and "/" in api_version:
            group, api_version = api_version.split("/")

        items = [prefix, group, api_version, kind, kwargs]
        return [x or "*" for x in items]

    async def __aiter__(self):
        assert self.__resources is not None
        for prefix, groups in self.__resources.items():
            for group, versions in groups.items():
                for version, rg in versions.items():
                    # Request resources for this groupVersion if we haven't yet
                    if not rg.resources:
                        rg.resources = await self.get_resources_for_api_version(
                            prefix, group, version, rg.preferred
                        )
                        self._cache["resources"][prefix][group][version] = rg  # type: ignore
                        self.__update_cache = True
                    for resource in rg.resources:
                        yield resource
        self.__maybe_write_cache()


class EagerDiscoverer(Discoverer):
    """A convenient container for storing discovered API resources. Allows
    easy searching and retrieval of specific resources.

    All resources are discovered for the cluster upon object instantiation.
    """

    def update(self, resources):
        self.__resources = resources

    def __init__(self, client, cache_file):
        self.__resources = None
        Discoverer.__init__(self, client, cache_file)

    async def discover(self):
        self.__resources = await self.parse_api_groups(request_resources=True)

    @property
    async def api_groups(self):
        """list available api groups"""
        groups = await self.parse_api_groups(request_resources=True, update=True)
        return groups["apis"].keys()

    async def search(
        self,
        prefix: str | None = None,
        group: str | None = None,
        api_version: str | None = None,
        kind: str | None = None,
        **kwargs: Any,
    ) -> list[Resource]:
        """Takes keyword arguments and returns matching resources. The search
        will happen in the following order:
            prefix: The api prefix for a resource, ie, /api, /oapi, /apis. Can usually be ignored
            group: The api group of a resource. Will also be extracted from api_version if it is present there
            api_version: The api version of a resource
            kind: The kind of the resource
            arbitrary arguments (see below), in random order

        The arbitrary arguments can be any valid attribute for an Resource object
        """
        results = self.__search(
            self.__build_search(prefix, group, api_version, kind, **kwargs),
            self.__resources,
        )
        if not results:
            await self.invalidate_cache()
            results = self.__search(
                self.__build_search(prefix, group, api_version, kind, **kwargs),
                self.__resources,
            )
        return results

    @staticmethod
    def __build_search(prefix=None, group=None, api_version=None, kind=None, **kwargs):
        if not group and api_version and "/" in api_version:
            group, api_version = api_version.split("/")

        items = [prefix, group, api_version, kind, kwargs]
        return [x or "*" for x in items]

    def __search(self, parts, resources):
        part = parts[0]
        resource_part = resources.get(part)

        if part != "*" and resource_part:
            if isinstance(resource_part, ResourceGroup):
                return self.__search(parts[1:], resource_part.resources)
            elif isinstance(resource_part, dict):
                return self.__search(parts[1:], resource_part)
            else:
                if parts[1] != "*" and isinstance(parts[1], dict):
                    for _resource in resource_part:
                        for term, value in parts[1].items():
                            if getattr(_resource, term) == value:
                                return [_resource]
                    return []
                else:
                    return resource_part
        elif part == "*":
            matches = []
            for key in resources.keys():
                matches.extend(self.__search([key] + parts[1:], resources))
            return matches
        return []

    def __iter__(self):
        assert self.__resources is not None
        for _, groups in self.__resources.items():
            for _, versions in groups.items():
                for _, resources in versions.items():
                    for _, resource in resources.items():
                        yield resource


class CacheEncoder(json.JSONEncoder):
    def default(self, o):
        return o.to_dict()


class CacheDecoder(json.JSONDecoder):
    def __init__(self, client, *args, **kwargs):
        self.client = client
        json.JSONDecoder.__init__(self, object_hook=self._object_hook, *args, **kwargs)  # noqa: B026

    def _object_hook(self, obj):
        if "_type" not in obj:
            return obj
        _type = obj.pop("_type")
        if _type == "Resource":
            return Resource(client=self.client, **obj)
        elif _type == "ResourceList":
            return ResourceList(self.client, **obj)
        elif _type == "ResourceGroup":
            return ResourceGroup(
                obj["preferred"], resources=self._object_hook(obj["resources"])
            )
        return obj
