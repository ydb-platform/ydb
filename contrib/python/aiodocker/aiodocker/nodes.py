from __future__ import annotations

from typing import Any, List, Mapping, Optional, Sequence

from .utils import clean_filters


class DockerSwarmNodes:
    def __init__(self, docker):
        self.docker = docker

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, str | Sequence[str]]] = None,
    ) -> List[Mapping]:
        """
        Return a list of swarm's nodes.

        Args:
            filters: a dict with a list of filters

        Available filters:
            id=<node id>
            label=<engine label>
            membership=(accepted|pending)`
            name=<node name>
            role=(manager|worker)`
        """

        params = {"filters": clean_filters(filters)}

        response = await self.docker._query_json("nodes", method="GET", params=params)

        return response

    async def inspect(self, *, node_id: str) -> Mapping[str, Any]:
        """
        Inspect a node

        Args:
            node_id: The ID or name of the node
        """

        response = await self.docker._query_json(f"nodes/{node_id}", method="GET")
        return response

    async def update(
        self, *, node_id: str, version: int, spec: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """
        Update the spec of a node.

        Args:
            node_id: The ID or name of the node
            version: version number of the node being updated
            spec: fields to be updated
        """

        params = {"version": version}

        if "Role" in spec:
            assert spec["Role"] in {"worker", "manager"}

        if "Availability" in spec:
            assert spec["Availability"] in {"active", "pause", "drain"}

        response = await self.docker._query_json(
            f"nodes/{node_id}/update",
            method="POST",
            params=params,
            data=spec,
        )
        return response

    async def remove(self, *, node_id: str, force: bool = False) -> Mapping[str, Any]:
        """
        Remove a node from a swarm.

        Args:
            node_id: The ID or name of the node
        """

        params = {"force": force}

        response = await self.docker._query_json(
            f"nodes/{node_id}", method="DELETE", params=params
        )
        return response
