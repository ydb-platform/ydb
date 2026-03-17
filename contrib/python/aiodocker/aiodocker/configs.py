from __future__ import annotations

import json
from base64 import b64encode
from typing import Any, List, Mapping, Optional, Sequence

from .utils import clean_filters, clean_map


class DockerConfigs:
    def __init__(self, docker):
        self.docker = docker

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, str | Sequence[str]]] = None,
    ) -> List[Mapping]:
        """
        Return a list of configs

        Args:
            filters: a dict with a list of filters

        Available filters:
            id=<config id>
            label=<key> or label=<key>=value
            name=<config name>
            names=<config name>
        """

        params = {"filters": clean_filters(filters)}
        response = await self.docker._query_json("configs", method="GET", params=params)
        return response

    async def create(
        self,
        name: str,
        data: str,
        *,
        b64: bool = False,
        labels: Optional[Mapping[str, str]] = None,
        templating: Optional[Mapping] = None,
    ) -> Mapping[str, Any]:
        """
        Create a config

        Args:
            name: name of the config
            labels: user-defined key/value metadata
            data: config data
            b64: True if data is already Base64-url-safe-encoded
            templating: Driver represents a driver (network, logging, secrets).

        Returns:
            a dict with info of the created config
        """

        b64_data = None
        if data is not None:
            b64_data = data if b64 else b64encode(data.encode()).decode()

        headers = None
        request = {
            "Name": name,
            "Labels": labels,
            "Data": b64_data,
            "Templating": templating,
        }

        request_data = json.dumps(clean_map(request))
        response = await self.docker._query_json(
            "configs/create", method="POST", data=request_data, headers=headers
        )
        return response

    async def inspect(self, config_id: str) -> Mapping[str, Any]:
        """
        Inspect a config

        Args:
            config_id: ID of the config

        Returns:
            a dict with info about a config
        """

        response = await self.docker._query_json(f"configs/{config_id}", method="GET")
        return response

    async def delete(self, config_id: str) -> bool:
        """
        Remove a config

        Args:
            config_id: ID or name of the config

        Returns:
            True if successful
        """

        async with self.docker._query(f"configs/{config_id}", method="DELETE"):
            return True

    async def update(
        self,
        config_id: str,
        version: str,
        *,
        name: Optional[str] = None,
        data: Optional[str] = None,
        b64: bool = False,
        labels: Optional[Mapping[str, str]] = None,
        templating: Optional[Mapping] = None,
    ) -> bool:
        """
        Update a config.

        Args:
            config_id: ID of the config.
            name: name of the config
            labels: user-defined key/value metadata
            data: config data
            b64: True if data is already Base64-url-safe-encoded
            templating: Driver represents a driver (network, logging, secrets).

        Returns:
            True if successful.
        """

        inspect_config = await self.inspect(config_id)
        spec = inspect_config["Spec"]

        b64_data = None
        if data is not None:
            b64_data = data if b64 else b64encode(data.encode()).decode()
            spec["Data"] = b64_data

        if name is not None:
            spec["Name"] = name

        if labels is not None:
            spec["Labels"] = labels

        if templating is not None:
            spec["Templating"] = templating

        params = {"version": version}
        request_data = json.dumps(clean_map(spec))

        await self.docker._query_json(
            f"configs/{config_id}/update",
            method="POST",
            data=request_data,
            params=params,
        )
        return True
