#pragma once

#include <ydb/core/grpc_services/local_rate_limiter.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/datetime/base.h>

namespace NKikimr::NGRpcProxy::V1 {

class TRlHelpers {
public:
    explicit TRlHelpers(NGRpcService::IRequestCtxBase* reqCtx, ui64 blockSize, const TDuration& waitDuration);

protected:
    enum EWakeupTag: ui64 {
        RlInit = 0,
        RlAllowed = 1,
        RlNoResource = 2,
        RecheckAcl = 3,
    };

    bool IsQuotaRequired() const;
    bool MaybeRequestQuota(ui64 amount, EWakeupTag tag, const TActorContext& ctx);
    void OnWakeup(EWakeupTag tag);

    const TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode>& GetMeteringMode() const;
    void SetMeteringMode(NKikimrPQ::TPQTabletConfig::EMeteringMode mode);

    ui64 CalcRuConsumption(ui64 payloadSize);

private:
    NGRpcService::IRequestCtxBase* const Request;
    const ui64 BlockSize;
    const TDuration WaitDuration;

    ui64 PayloadBytes;
    TActorId RlActor;
    TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode> MeteringMode;
};

}
