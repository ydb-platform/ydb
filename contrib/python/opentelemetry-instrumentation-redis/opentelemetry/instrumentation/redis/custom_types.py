# Copyright The OpenTelemetry Authors
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

from __future__ import annotations

from typing import Any, Callable, TypeVar

import redis.asyncio.client
import redis.asyncio.cluster
import redis.client
import redis.cluster
import redis.connection

from opentelemetry.trace import Span

RequestHook = Callable[
    [Span, redis.connection.Connection, list[Any], dict[str, Any]], None
]
ResponseHook = Callable[[Span, redis.connection.Connection, Any], None]

AsyncPipelineInstance = TypeVar(
    "AsyncPipelineInstance",
    redis.asyncio.client.Pipeline,
    redis.asyncio.cluster.ClusterPipeline,
)
AsyncRedisInstance = TypeVar(
    "AsyncRedisInstance", redis.asyncio.Redis, redis.asyncio.RedisCluster
)
PipelineInstance = TypeVar(
    "PipelineInstance",
    redis.client.Pipeline,
    redis.cluster.ClusterPipeline,
)
RedisInstance = TypeVar(
    "RedisInstance", redis.client.Redis, redis.cluster.RedisCluster
)
R = TypeVar("R")
