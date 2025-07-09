#include "kqp_gateway.h"

namespace NKikimr::NKqp {

void IKqpGateway::TExecPhysicalRequest::FillRequestFrom(IKqpGateway::TExecPhysicalRequest& request, const IKqpGateway::TExecPhysicalRequest& from) {
    request.AllowTrailingResults = from.AllowTrailingResults;
    request.QueryType = from.QueryType;
    request.PerRequestDataSizeLimit = from.PerRequestDataSizeLimit;
    request.MaxShardCount = from.MaxShardCount;
    request.DataShardLocks = from.DataShardLocks;
    request.LocksOp = from.LocksOp;
    request.AcquireLocksTxId = from.AcquireLocksTxId;
    request.Timeout = from.Timeout;
    request.CancelAfter = from.CancelAfter;
    request.MaxComputeActors = from.MaxComputeActors;
    request.MaxAffectedShards = from.MaxAffectedShards;
    request.TotalReadSizeLimitBytes = from.TotalReadSizeLimitBytes;
    request.MkqlMemoryLimit = from.MkqlMemoryLimit;
    request.PerShardKeysSizeLimitBytes = from.PerShardKeysSizeLimitBytes;
    request.StatsMode = from.StatsMode;
    request.ProgressStatsPeriod = from.ProgressStatsPeriod;
    request.Snapshot = from.Snapshot;
    request.ResourceManager_ = from.ResourceManager_;
    request.CaFactory_ = from.CaFactory_;
    request.IsolationLevel = from.IsolationLevel;
    request.RlPath = from.RlPath;
    request.NeedTxId = from.NeedTxId;
    request.UseImmediateEffects = from.UseImmediateEffects;
    request.UserTraceId = from.UserTraceId;
    request.OutputChunkMaxSize = from.OutputChunkMaxSize;
}

}
