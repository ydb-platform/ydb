#pragma once

#include "kqp_compute_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_actor.h>
#include <ydb/core/sys_view/scan.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/udf_store/wasm/query_compartment_scope.h>

#include <optional>


namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;

class TKqpComputeActor : public NScheduler::TSchedulableComputeActorBase<TKqpComputeActor> {
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
        NScheduler::TSchedulableOptions schedulableOptions,
        NKikimrConfig::TTableServiceConfig::EBlockTrackingMode mode,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database
    );

    void DoBootstrap();

    STFUNC(StateFunc);

    ui64 GetSourcesState();

    void PollSources(ui64 prevFreeSpace);

    // After a worker slice, pull the next source batch on the endpoint and
    // queue another run only when the read actor actually had rows.
    void OnWorkerSliceInputsConsumed(NYql::NDq::ERunStatus status) {
        if (SourcesMap.empty() || !TaskScheduling || TaskScheduling->ObserveOnly) {
            return;
        }
        bool polled = false;
        if (!DqSliceCompletionOnWorker()) {
            polled = TBase::PollSourcesForScheduledRun(status);
        } else if (TBase::SourcesMayHaveMore) {
            // The read actor may hold the rest of a large read. Polling it from the
            // worker races its handlers, so hand the poll back to the endpoint.
            TBase::SourcesMayHaveMore = false;
            this->Send(this->SelfId(), new TEvDqCompute::TEvResumeExecution{EResumeSource::CAPollAsync});
        }
        bool buffered = false;
        // A run blocked on output resumes on the channel ack. Rerunning it for the
        // rows it could not process is a busy loop.
        if (!polled && status != NYql::NDq::ERunStatus::PendingOutput) {
            for (const auto& [_, source] : SourcesMap) {
                if (source.Buffer && !source.Buffer->Empty()) {
                    buffered = true;
                    break;
                }
            }
        }
        if (polled || buffered) {
            TaskScheduling->RequestExecution();
        }
    }

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

    void HandleExecute(TEvKqpCompute::TEvScanWarning::TPtr& ev);

    bool IsDebugLogEnabled(const TActorSystem* actorSystem);

    ui64 CalculateFreeSpace() const;

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
    std::optional<NUdfStore::NWasm::TQueryCompartmentScope> WasmQueryCompartment_;
};

} // namespace NKikimr::NKqp
