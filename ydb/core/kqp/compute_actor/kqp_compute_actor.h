#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/kqp_compute.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NMiniKQL {

class TKqpScanComputeContext;

TComputationNodeFactory GetKqpActorComputeFactory(TKqpScanComputeContext* computeCtx);

} // namespace NMiniKQL

namespace NKqp {

IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask&& task,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits);

IActor* CreateKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
    NYql::NDqProto::TDqTask&& task, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits,
    TIntrusivePtr<TKqpCounters> counters);

namespace NComputeActor {

enum class EShardState : ui32 {
    Initial,
    Starting,
    Running,
    PostRunning, //We already recieve all data, we has not processed it yet.
    Resolving,
};

std::string_view EShardStateToString(EShardState state);

bool FindSchemeErrorInIssues(const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues);

struct TShardState {
    ui64 TabletId;
    TSmallVec<TSerializedTableRange> Ranges;
    EShardState State = EShardState::Initial;
    ui32 Generation = 0;
    bool SubscribedOnTablet = false;
    ui32 RetryAttempt = 0;
    ui32 TotalRetries = 0;
    bool AllowInstantRetry = true;
    TDuration LastRetryDelay;
    TActorId RetryTimer;
    ui32 ResolveAttempt = 0;
    TActorId ActorId;
    TOwnedCellVec LastKey;

    TString PrintLastKey(TConstArrayRef<NScheme::TTypeId> keyTypes) const;

    explicit TShardState(ui64 tabletId)
        : TabletId(tabletId) {}

    TDuration CalcRetryDelay();
    void ResetRetry();

    TString ToString(TConstArrayRef<NScheme::TTypeId> keyTypes) const;
    const TSmallVec<TSerializedTableRange> GetScanRanges(TConstArrayRef<NScheme::TTypeId> keyTypes) const;
};

} // namespace NComputeActor

} // namespace NKqp
} // namespace NKikimr
