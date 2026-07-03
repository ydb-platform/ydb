#pragma once

#include <ydb/core/grpc_services/local_rate_limiter.h>
#include <ydb/core/metering/stream_ru_calculator.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/scheme_board/events.h>

#include <util/datetime/base.h>

#include <optional>

namespace NKikimr::NPQ {

class TRlContext {
public:
    TRlContext() {
    }

    TRlContext(const NGRpcService::IRequestCtxBaseMtSafe* reqCtx) {
        Path.DatabaseName = reqCtx->GetDatabaseName().GetOrElse("");
        Path.Token = reqCtx->GetSerializedToken();

        if (auto rlPath = reqCtx->GetRlPath()) {
            Path.ResourcePath = rlPath->ResourcePath;
            Path.CoordinationNode = rlPath->CoordinationNode;
        }
    }

    TRlContext(const TString& coordinationNode,
               const TString& resourcePath,
               const TString& databaseName,
               const TString& token)
        : Path({coordinationNode, resourcePath, databaseName, token}) {
    }

    operator bool() const { return !Path.ResourcePath.empty() && !Path.CoordinationNode.empty(); };
    const NRpcService::TRlFullPath GetPath() const { return Path; }

private:
    NRpcService::TRlFullPath Path;
};

class TRlHelpers: public NMetering::TStreamRequestUnitsCalculator {
public:
    TRlHelpers(const std::optional<TString>& topicPath, const TRlContext ctx, ui64 blockSize, bool subscribe, const TDuration& waitDuration = TDuration::Minutes(1));

protected:
    enum EWakeupTag: ui64 {
        RlInit = 0,
        RlInitNoResource = 1,

        RlAllowed = 2,
        RlNoResource = 3,

        RecheckAcl = 4,
    };

    void Bootstrap(const TActorId selfId, const NActors::TActorContext& ctx);
    void PassAway(const TActorId selfId);

    bool IsQuotaRequired() const;
    bool IsQuotaInflight() const;

    // True when the topic is metered by Request Units. Unlike IsQuotaRequired()
    // this does not require the rate-limiter context to be resolved yet, so it
    // can be used to decide whether the RL path has to be looked up.
    bool IsRequestUnitsMeteringMode() const;

    // Rate-limiter context is normally taken from the request in the
    // constructor. Requests dispatched via DoLocalRpc (e.g. the SQS-over-topic
    // HTTP proxy) carry no RlPath, so the context has to be built from the
    // database serverless attributes and injected afterwards.
    void SetRlContext(const TRlContext& ctx);

    void RequestInitQuota(ui64 amount, const TActorContext& ctx, NWilson::TTraceId traceId = {});
    void RequestDataQuota(ui64 amount, const TActorContext& ctx, NWilson::TTraceId traceId = {});

    bool MaybeRequestQuota(ui64 amount, EWakeupTag tag, const TActorContext& ctx, NWilson::TTraceId traceId = {});
    void RequestQuota(ui64 amount, EWakeupTag success, EWakeupTag timeout, const TActorContext& ctx, NWilson::TTraceId traceId = {});

    void OnWakeup(EWakeupTag tag);

    const TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode>& GetMeteringMode() const;
    void SetMeteringMode(NKikimrPQ::TPQTabletConfig::EMeteringMode mode);

    ui64 CalcRuConsumption(ui64 payloadSize);

    void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev);

private:
    const std::optional<TString> TopicPath;
    TRlContext Ctx;
    const bool Subscribe;
    const TDuration WaitDuration;

    TActorId RlActor;
    TActorId SubscriberId;

    TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode> MeteringMode;
};

} // namespace NKikimr::NPQ
