# Copyright 2026 The HuggingFace Team. All rights reserved.
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


import json
from collections import deque
from typing import Iterator, Literal, Optional, TypedDict, Union

import httpx

from ..utils._headers import build_hf_headers
from ..utils._http import hf_raise_for_status
from .sse_client import SSEClient
from .types import ApiGetReloadEventSourceData, ApiGetReloadRequest


HOT_RELOADING_PORT = 7887


class MultiReplicaStreamEvent(TypedDict):
    kind: Literal["event"]
    event: ApiGetReloadEventSourceData


class MultiReplicaStreamReplicaHash(TypedDict):
    kind: Literal["replicaHash"]
    hash: str


class MultiReplicaStreamFullMatch(TypedDict):
    kind: Literal["fullMatch"]


class ReloadClient:
    def __init__(
        self,
        *,
        host: str,
        subdomain: str,
        replica_hash: str,
        token: Optional[str],
    ):
        base_host = host.replace(subdomain, f"{subdomain}--{HOT_RELOADING_PORT}")
        self.replica_hash = replica_hash
        self.client = httpx.Client(
            base_url=f"{base_host}/--replicas/+{replica_hash}",
            headers=build_hf_headers(token=token),
        )

    def get_reload(self, reload_id: str) -> Iterator[ApiGetReloadEventSourceData]:
        req = ApiGetReloadRequest(reloadId=reload_id)
        with self.client.stream("POST", "/get-reload", json=req) as res:
            hf_raise_for_status(res)
            for event in SSEClient(res.iter_bytes()).events():
                if event.event == "message":
                    yield json.loads(event.data)


def multi_replica_reload_events(
    commit_sha: str,
    host: str,
    subdomain: str,
    replica_hashes: list[str],
    token: Optional[str],
) -> Iterator[Union[MultiReplicaStreamEvent, MultiReplicaStreamReplicaHash, MultiReplicaStreamFullMatch]]:
    clients = [
        ReloadClient(
            host=host,
            subdomain=subdomain,
            replica_hash=hash,
            token=token,
        )
        for hash in replica_hashes
    ]

    first_client_events: dict[int, ApiGetReloadEventSourceData] = {}
    for client_index, client in enumerate(clients):
        if len(clients) > 1:
            yield {"kind": "replicaHash", "hash": client.replica_hash}
        full_match = True
        replay: deque[ApiGetReloadEventSourceData] = deque()
        for event_index, event in enumerate(client.get_reload(commit_sha)):
            if client_index == 0:
                first_client_events[event_index] = event
            elif full_match := full_match and first_client_events.get(event_index) == event:
                replay.append(event)
                continue
            while replay:
                yield {"kind": "event", "event": replay.popleft()}
            yield {"kind": "event", "event": event}
        if client_index > 0 and full_match:
            yield {"kind": "fullMatch"}
