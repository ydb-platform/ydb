#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/library/actors/core/actorid.h>

#include <contrib/libs/protobuf/src/google/protobuf/map.h>

#include <util/generic/fwd.h>
#include <util/stream/output.h>

namespace NYql::NPq::NProto {

class StreamingDisposition;

} // namespace NYql::NPq::NProto

namespace NKikimr::NKqp {

struct TUserRequestContext : public TAtomicRefCount<TUserRequestContext> {
    TString TraceId;
    TString Database;
    TString DatabaseId;
    TString SessionId;
    bool UseBatchPool = false;

    // Workload manager info
    TString PoolId;
    std::optional<NResourcePool::TPoolSettings> PoolConfig;

    // Script execution info
    TString CurrentExecutionId;
    i64 CurrentExecutionGeneration = 0;
    TString CustomerSuppliedId;
    NActors::TActorId RunScriptActorId;

    // Streaming query info
    bool IsStreamingQuery = false;
    TString CheckpointId;
    TString StreamingQueryPath;
    TString WatermarkLateEventsPolicy;
    std::shared_ptr<NYql::NPq::NProto::StreamingDisposition> StreamingDisposition;

    TUserRequestContext() = default;

    TUserRequestContext(const TString& traceId, const TString& database, const TString& sessionId)
        : TraceId(traceId)
        , Database(database)
        , SessionId(sessionId)
    {}

    TUserRequestContext(const TString& traceId, const TString& database, const TString& sessionId, const TString& currentExecutionId, const TString& customerSuppliedId, NActors::TActorId runScriptActorId)
        : TraceId(traceId)
        , Database(database)
        , SessionId(sessionId)
        , CurrentExecutionId(currentExecutionId)
        , CustomerSuppliedId(customerSuppliedId)
        , RunScriptActorId(runScriptActorId)
    {}

    void Out(IOutputStream& o) const;
};

void SerializeCtxToMap(const TUserRequestContext& ctx, google::protobuf::Map<TString, TString>& resultMap);

} // namespace NKikimr::NKqp

template<>
inline void Out<NKikimr::NKqp::TUserRequestContext>(IOutputStream& o, const NKikimr::NKqp::TUserRequestContext &x) {
    return x.Out(o);
}
