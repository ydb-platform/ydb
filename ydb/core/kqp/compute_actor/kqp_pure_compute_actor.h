#pragma once

#include "kqp_compute_actor.h"
#include "kqp_compute_actor_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/scheduler/new/kqp_compute_actor.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/library/yverify_stream/yverify_stream.h>


namespace NKikimr::NKqp {

class TKqpComputeActor : public TSchedulableComputeActorBase<TKqpComputeActor> {
    using TBase = TSchedulableComputeActorBase<TKqpComputeActor>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPUTE_ACTOR;
    }

    TKqpComputeActor(const TActorId& executerId, ui64 txId, NDqProto::TDqTask* task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        NWilson::TTraceId traceId, TIntrusivePtr<NActors::TProtoArenaHolder> arena,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
        TSchedulableOptions schedulableOptions,
        NKikimrConfig::TTableServiceConfig::EBlockTrackingMode mode,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database);

    void DoBootstrap();

    STFUNC(StateFunc);

protected:
    ui64 CalcMkqlMemoryLimit() override;

    void CheckRunStatus() override;

public:
    void FillExtraStats(NDqProto::TDqComputeActorStats* dst, bool last);

private:
    void PassAway() override;

private:
    void HandleExecute(TEvKqpCompute::TEvScanInitActor::TPtr& ev);

    void HandleExecute(TEvKqpCompute::TEvScanData::TPtr& ev);

    void HandleExecute(TEvKqpCompute::TEvScanError::TPtr& ev);

    bool IsDebugLogEnabled(const TActorSystem* actorSystem);

private:
    NMiniKQL::TKqpScanComputeContext ComputeCtx;
    TMaybe<NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta> Meta;
    NMiniKQL::TKqpScanComputeContext::TScanData* ScanData = nullptr;
    TActorId SysViewActorId;
    const TDqTaskRunnerParameterProvider ParameterProvider;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const NKikimrConfig::TTableServiceConfig::EBlockTrackingMode BlockTrackingMode;
    const TMaybe<ui8> ArrayBufferMinFillPercentage;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString Database;
};

} // namespace NKikimr::NKqp
