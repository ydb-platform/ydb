from __future__ import annotations

from typing import Mapping


class DockerSystem:
    def __init__(self, docker) -> None:
        self.docker = docker

    async def info(self) -> Mapping:
        """
        Get system information, similar to `docker info`.

        Returns:
            A dict with docker engine info.
        """

        response = await self.docker._query_json("info", method="GET")

        return response
