from typing import Any, Dict, List

from annet.connectors import AdapterWithConfig
from annet.deploy import Fetcher
from annet.storage import Device


class StubFetcher(Fetcher, AdapterWithConfig):
    @classmethod
    def with_config(cls, **kwargs: Dict[str, Any]) -> Fetcher:
        return cls(**kwargs)

    async def fetch_packages(
        self,
        devices: list[Device],
        processes: int = 1,
        max_slots: int = 0,
    ) -> tuple[dict[Device, str], dict[Device, Any]]:
        raise NotImplementedError()

    async def fetch(
        self,
        devices: list[Device],
        files_to_download: dict[str, list[str]] | None = None,
        processes: int = 1,
        max_slots: int = 0,
    ):
        raise NotImplementedError()
