from dataclasses import dataclass
from enum import IntEnum
import typing

try:
    from ydb.public.api.protos import ydb_coordination_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_coordination_pb2


class ConsistencyMode(IntEnum):
    UNSET = ydb_coordination_pb2.ConsistencyMode.CONSISTENCY_MODE_UNSET
    STRICT = ydb_coordination_pb2.ConsistencyMode.CONSISTENCY_MODE_STRICT
    RELAXED = ydb_coordination_pb2.ConsistencyMode.CONSISTENCY_MODE_RELAXED


class RateLimiterCountersMode(IntEnum):
    UNSET = ydb_coordination_pb2.RateLimiterCountersMode.RATE_LIMITER_COUNTERS_MODE_UNSET
    AGGREGATED = ydb_coordination_pb2.RateLimiterCountersMode.RATE_LIMITER_COUNTERS_MODE_AGGREGATED
    DETAILED = ydb_coordination_pb2.RateLimiterCountersMode.RATE_LIMITER_COUNTERS_MODE_DETAILED


@dataclass
class NodeConfig:
    attach_consistency_mode: ConsistencyMode
    rate_limiter_counters_mode: RateLimiterCountersMode
    read_consistency_mode: ConsistencyMode
    self_check_period_millis: int
    session_grace_period_millis: int

    @staticmethod
    def from_proto(msg: ydb_coordination_pb2.Config) -> "NodeConfig":
        return NodeConfig(
            attach_consistency_mode=msg.attach_consistency_mode,
            rate_limiter_counters_mode=msg.rate_limiter_counters_mode,
            read_consistency_mode=msg.read_consistency_mode,
            self_check_period_millis=msg.self_check_period_millis,
            session_grace_period_millis=msg.session_grace_period_millis,
        )

    def to_proto(self) -> ydb_coordination_pb2.Config:
        return ydb_coordination_pb2.Config(
            attach_consistency_mode=self.attach_consistency_mode,
            rate_limiter_counters_mode=self.rate_limiter_counters_mode,
            read_consistency_mode=self.read_consistency_mode,
            self_check_period_millis=self.self_check_period_millis,
            session_grace_period_millis=self.session_grace_period_millis,
        )


class DescribeResult:
    @staticmethod
    def from_proto(msg: ydb_coordination_pb2.DescribeNodeResponse) -> "NodeConfig":
        result = ydb_coordination_pb2.DescribeNodeResult()
        msg.operation.result.Unpack(result)
        return NodeConfig.from_proto(result.config)


@dataclass
class AcquireSemaphoreResult:
    req_id: int
    acquired: bool
    status: int

    @staticmethod
    def from_proto(msg: ydb_coordination_pb2.SessionResponse.AcquireSemaphoreResult) -> "AcquireSemaphoreResult":
        return AcquireSemaphoreResult(
            req_id=msg.req_id,
            acquired=msg.acquired,
            status=msg.status,
        )


@dataclass
class CreateSemaphoreResult:
    req_id: int
    status: int

    @staticmethod
    def from_proto(msg: ydb_coordination_pb2.SessionResponse.CreateSemaphoreResult) -> "CreateSemaphoreResult":
        return CreateSemaphoreResult(
            req_id=msg.req_id,
            status=msg.status,
        )


@dataclass
class DescribeLockResult:
    req_id: int
    status: int
    watch_added: bool
    count: int
    data: bytes
    ephemeral: bool
    limit: int
    name: str
    owners: list
    waiters: list

    @staticmethod
    def from_proto(msg: ydb_coordination_pb2.SessionResponse.DescribeSemaphoreResult) -> "DescribeLockResult":
        return DescribeLockResult(
            req_id=msg.req_id,
            status=msg.status,
            watch_added=msg.watch_added,
            count=msg.semaphore_description.count,
            data=msg.semaphore_description.data,
            ephemeral=msg.semaphore_description.ephemeral,
            limit=msg.semaphore_description.limit,
            name=msg.semaphore_description.name,
            owners=msg.semaphore_description.owners,
            waiters=msg.semaphore_description.waiters,
        )
