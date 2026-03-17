from __future__ import annotations

import json
from base64 import b64encode
from typing import Any, List, Mapping, Optional, Sequence

from .utils import clean_filters, clean_map


class DockerSecrets:
    def __init__(self, docker):
        self.docker = docker

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, str | Sequence[str]]] = None,
    ) -> List[Mapping]:
        """
        Return a list of secrets

        Args:
            filters: a dict with a list of filters

        Available filters:
            id=<secret id>
            label=<key> or label=<key>=value
            name=<secret name>
            names=<secret name>
        """

        params = {"filters": clean_filters(filters)}
        response = await self.docker._query_json("secrets", method="GET", params=params)
        return response

    async def create(
        self,
        name: str,
        data: str,
        *,
        b64: bool = False,
        labels: Optional[Mapping[str, str]] = None,
        driver: Optional[Mapping] = None,
        templating: Optional[Mapping] = None,
    ) -> Mapping[str, Any]:
        """
        Create a secret

        Args:
            name: name of the secret
            labels: user-defined key/value metadata
            data: data to store as secret
            b64: True if data is already Base64-url-safe-encoded
            driver: Driver represents a driver (network, logging, secrets).
            templating: Driver represents a driver (network, logging, secrets).

        Returns:
            a dict with info of the created secret
        """

        b64_data = None
        if data is not None:
            b64_data = data if b64 else b64encode(data.encode()).decode()

        headers = None
        request = {
            "Name": name,
            "Labels": labels,
            "Data": b64_data,
            "Driver": driver,
            "Templating": templating,
        }

        request_data = json.dumps(clean_map(request))
        response = await self.docker._query_json(
            "secrets/create", method="POST", data=request_data, headers=headers
        )
        return response

    async def inspect(self, secret_id: str) -> Mapping[str, Any]:
        """
        Inspect a secret

        Args:
            secret_id: ID of the secret

        Returns:
            a dict with info about a secret
        """

        response = await self.docker._query_json(f"secrets/{secret_id}", method="GET")
        return response

    async def delete(self, secret_id: str) -> bool:
        """
        Remove a secret

        Args:
            secret_id: ID of the secret

        Returns:
            True if successful
        """

        async with self.docker._query(f"secrets/{secret_id}", method="DELETE"):
            return True

    async def update(
        self,
        secret_id: str,
        version: str,
        *,
        name: Optional[str] = None,
        data: Optional[str] = None,
        b64: bool = False,
        labels: Optional[Mapping[str, str]] = None,
        driver: Optional[Mapping] = None,
        templating: Optional[Mapping] = None,
    ) -> bool:
        """
        Update a secret.

        Args:
            secret_id: ID of the secret.
            name: name of the secret
            labels: user-defined key/value metadata
            data: data to store as secret
            b64: True if data is already Base64-url-safe-encoded
            driver: Driver represents a driver (network, logging, secrets).
            templating: Driver represents a driver (network, logging, secrets).

        Returns:
            True if successful.
        """

        inspect_secret = await self.inspect(secret_id)
        spec = inspect_secret["Spec"]

        b64_data = None
        if data is not None:
            b64_data = data if b64 else b64encode(data.encode()).decode()
            spec["Data"] = b64_data

        if name is not None:
            spec["Name"] = name

        if labels is not None:
            spec["Labels"] = labels

        if driver is not None:
            spec["Driver"] = driver

        if templating is not None:
            spec["Templating"] = templating

        params = {"version": version}
        request_data = json.dumps(clean_map(spec))

        await self.docker._query_json(
            f"secrets/{secret_id}/update",
            method="POST",
            data=request_data,
            params=params,
        )
        return True
