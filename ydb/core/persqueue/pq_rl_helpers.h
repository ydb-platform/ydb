#pragma once

#include <ydb/core/grpc_services/local_rate_limiter.h>
#include <ydb/core/metering/stream_ru_calculator.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/datetime/base.h>

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

    operator bool() const { return !Path.ResourcePath.Empty() && !Path.CoordinationNode.Empty(); };
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

    void RequestInitQuota(ui64 amount, const TActorContext& ctx);
    void RequestDataQuota(ui64 amount, const TActorContext& ctx);

    bool MaybeRequestQuota(ui64 amount, EWakeupTag tag, const TActorContext& ctx);
    void RequestQuota(ui64 amount, EWakeupTag success, EWakeupTag timeout, const TActorContext& ctx);

    void OnWakeup(EWakeupTag tag);

    const TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode>& GetMeteringMode() const;
    void SetMeteringMode(NKikimrPQ::TPQTabletConfig::EMeteringMode mode);

    ui64 CalcRuConsumption(ui64 payloadSize);

    void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev);

private:
    const std::optional<TString> TopicPath;
    const TRlContext Ctx;
    const bool Subscribe;
    const TDuration WaitDuration;

    TActorId RlActor;
    TActorId SubscriberId;

    TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode> MeteringMode;
};

}
