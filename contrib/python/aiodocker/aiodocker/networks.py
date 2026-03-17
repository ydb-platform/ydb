from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .utils import clean_filters


class DockerNetworks:
    def __init__(self, docker):
        self.docker = docker

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, str | Sequence[str]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return a list of networks

        Args:
            filters: a dict with a list of filters

        Available filters:
            dangling=<boolean>
            driver=<driver-name>
            id=<network-id>
            label=<key> or label=<key>=<value> of a network label.
            name=<network-name>
            scope=["swarm"|"global"|"local"]
            type=["custom"|"builtin"]
        """
        params = {} if filters is None else {"filters": clean_filters(filters)}

        data = await self.docker._query_json("networks", params=params)
        return data

    async def create(self, config: Dict[str, Any]) -> DockerNetwork:
        bconfig = json.dumps(config, sort_keys=True).encode("utf-8")
        data = await self.docker._query_json(
            "networks/create", method="POST", data=bconfig
        )
        return DockerNetwork(self.docker, data["Id"])

    async def get(self, net_specs: str) -> DockerNetwork:
        data = await self.docker._query_json(f"networks/{net_specs}", method="GET")
        return DockerNetwork(self.docker, data["Id"])

    async def prune(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Delete unused networks

        Args:
            filters: Filter expressions to limit which networks are pruned.
                    Available filters:
                    - until: Only remove networks created before given timestamp
                    - label: Only remove networks with (or without, if label!=<key> is used) the specified labels

        Returns:
            Dictionary containing information about deleted networks
        """
        params = {}
        if filters is not None:
            params["filters"] = clean_filters(filters)

        response = await self.docker._query_json(
            "networks/prune", "POST", params=params
        )
        return response


class DockerNetwork:
    def __init__(self, docker, id_):
        self.docker = docker
        self.id = id_

    async def show(self) -> Dict[str, Any]:
        data = await self.docker._query_json(f"networks/{self.id}")
        return data

    async def delete(self) -> bool:
        async with self.docker._query(f"networks/{self.id}", method="DELETE") as resp:
            return resp.status == 204

    async def connect(self, config: Dict[str, Any]) -> None:
        bconfig = json.dumps(config, sort_keys=True).encode("utf-8")
        await self.docker._query_json(
            f"networks/{self.id}/connect", method="POST", data=bconfig
        )

    async def disconnect(self, config: Dict[str, Any]) -> None:
        bconfig = json.dumps(config, sort_keys=True).encode("utf-8")
        await self.docker._query_json(
            f"networks/{self.id}/disconnect",
            method="POST",
            data=bconfig,
        )
