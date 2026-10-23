#include "kqp_vector_executer.h"
#include "kqp_executer_stats.h"

#include <ydb/core/base/appdata.h>

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <algorithm>
#include <util/generic/scope.h>
#include <util/system/hp_timer.h>

namespace NKikimr::NKqp {
namespace {

class TSingleVectorStream : public NMiniKQL::TComputationValue<TSingleVectorStream> {
public:
    TSingleVectorStream(NMiniKQL::TMemoryUsageInfo* info, const NMiniKQL::THolderFactory& factory, TString target)
        : TComputationValue(info)
        , Factory(factory)
        , Target(std::move(target))
    {}

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& row) override {
        if (Fetched) {
            return NUdf::EFetchStatus::Finish;
        }
        Fetched = true;
        NUdf::TUnboxedValue* items = nullptr;
        row = Factory.CreateDirectArrayHolder(1, items);
        // A NULL (e.g. missing scalar-subquery row) follows the same empty-search
        // behavior as the normal vector transform.
        if (!Target.empty()) {
            items[0] = NMiniKQL::MakeString(Target);
        }
        return NUdf::EFetchStatus::Ok;
    }

    const NMiniKQL::THolderFactory& Factory;
    TString Target;
    bool Fetched = false;
};

} // namespace

std::unique_ptr<TKqpVectorExecution> TKqpVectorExecution::Prepare(
    const IKqpGateway::TExecPhysicalRequest& request, TKqpTasksGraph& graph,
    const TIntrusivePtr<TUserRequestContext>& context, bool hasBuffer)
{
    if (request.Transactions.size() != 1) {
        return nullptr;
    }
    const auto& tx = request.Transactions.front();
    const auto* plan = tx.Body->GetVectorExecution();
    if (!plan) {
        return nullptr;
    }
    if (hasBuffer || request.AcquireLocksTxId
        || request.IsolationLevel != NKqpProto::ISOLATION_LEVEL_READ_STALE
        || request.LocksOp != ELocksOp::Unspecified || request.FlushEffects
        || context->UseBatchPool || context->IsStreamingQuery || !context->CurrentExecutionId.empty()
        || request.SaveQueryPhysicalGraph || request.QueryPhysicalGraph || request.RlPath
        || request.TotalReadSizeLimitBytes || request.CollectAffectedRows || request.ProgressStatsPeriod
        || CollectFullStats(request.StatsMode) || !request.MaxComputeActors || !request.ResourceManager_ || !request.CaFactory_
        || (!context->PoolId.empty() && context->PoolId != NResourcePool::DEFAULT_POOL_ID)) {
        return nullptr;
    }
    auto& stageInfo = graph.GetStageInfo(graph.MakeStageId(0, plan->GetSearchStageIndex()));
    const auto& search = tx.Body->GetStages(plan->GetSearchStageIndex()).GetInputs(0).GetVectorSearch();
    auto settings = *graph.CreateVectorSearchSettings(stageInfo, search);
    // Bound native workspace and the single result batch. Larger searches use
    // normal compute actors with their incremental quota and flow control.
    if (!settings.GetPostingCovers() || settings.GetHasPrefix() || settings.GetTopK() > 1024
        || settings.GetLevelTop() > 64 || settings.GetIndexLevels() > 8
        || settings.GetIndexSettings().vector_dimension() > 4096 || settings.OutputColumnsSize() > 16) {
        return nullptr;
    }
    // This coordinator emits one bounded result part. Preserve tighter client
    // chunk limits through the general executor's normal flow control.
    const ui64 resultBound = 1024 + ui64(settings.GetTopK()) * plan->ProjectionSize() * 64;
    if (request.OutputChunkMaxSize && request.OutputChunkMaxSize < resultBound) {
        return nullptr;
    }
    const ui64 maxTargetBytes = ui64(settings.GetIndexSettings().vector_dimension()) * sizeof(float) + 1;
    for (const auto& column : settings.GetOutputColumns()) {
        if (column.GetType() != NScheme::NTypeIds::Int64 && column.GetType() != NScheme::NTypeIds::Uint64
            && column.GetType() != NScheme::NTypeIds::Float) {
            return nullptr;
        }
    }
    for (const auto& column : settings.GetMainTableKeyColumns()) {
        if (column.GetType() != NScheme::NTypeIds::Int64 && column.GetType() != NScheme::NTypeIds::Uint64) {
            return nullptr;
        }
    }
    TString target;
    {
        auto guard = request.TxAlloc->TypeEnv.BindAllocator();
        NUdf::TUnboxedValue value;
        if (plan->GetTarget().HasParamValue()) {
            value = tx.Params->GetParameterUnboxedValue(plan->GetTarget().GetParamValue().GetParamName()).second;
        } else {
            const auto& bytes = plan->GetTarget().GetLiteralValue().value().bytes_value();
            if (bytes.size() > maxTargetBytes) {
                return nullptr;
            }
            target = bytes;
        }
        if (plan->HasTargetMemberIndex()) {
            if (plan->GetTargetStructOptional() && value) {
                value = value.GetOptionalValue();
            }
            if (value) {
                value = value.GetElement(plan->GetTargetMemberIndex());
            }
        }
        if (value) {
            const auto bytes = value.AsStringRef();
            if (bytes.Size() > maxTargetBytes) {
                return nullptr;
            }
            target = TString(bytes);
        }
    }
    return std::unique_ptr<TKqpVectorExecution>(new TKqpVectorExecution(request, std::move(settings), std::move(target), context, graph.GetMeta().TxId));
}

TKqpVectorExecution::TKqpVectorExecution(const IKqpGateway::TExecPhysicalRequest& request,
    NKikimrTxDataShard::TKqpVectorSearchSettings settings, TString target,
    TIntrusivePtr<TUserRequestContext> context, ui64 txId)
    : Request(request)
    , Settings(std::move(settings))
    , Plan(*request.Transactions.front().Body->GetVectorExecution())
    , TxId(txId)
    , Target(std::move(target))
    , Context(std::move(context))
{}

TKqpVectorExecution::~TKqpVectorExecution() {
    // The source and its inner reads share the executer's mailbox. PassAway is
    // synchronous; their allocator must remain alive until they are all stopped.
    if (Input) {
        Input->PassAway();
    }
}

TMaybe<TString> TKqpVectorExecution::Start(const TActorId& parent, NYql::NDq::IDqAsyncIoFactory& factory,
    const IKqpGateway::TKqpSnapshot& snapshot, ui64 querySpanId, const NWilson::TTraceId& traceId)
{
    const auto started = GetCycleCountFast();
    Y_DEFER { CpuTimeUs += NHPTimer::GetSeconds(GetCycleCountFast() - started) * 1'000'000; };
    auto rm = Request.ResourceManager_;
    const double memoryPoolPercent = Context->PoolConfig
        ? Context->PoolConfig->TotalMemoryLimitPercentPerNode : 100;
    Quota = CreateQueryQuotaManager(MakeIntrusive<NRm::TTxState>(rm, TxId, TInstant::Now(),
        Context->PoolId, memoryPoolPercent, Settings.GetDatabase(), Request.CaFactory_->GetVerboseMemoryLimitException()));
    ui64 limit = Request.CaFactory_->MkqlLightProgramMemoryLimit.load();
    if (!limit) {
        limit = 1_MB;
    }
    if (Request.MkqlMemoryLimit) {
        limit = std::min(limit, Request.MkqlMemoryLimit);
    }
    const auto admission = Quota->AllocateTasks(1, limit);
    if (!admission) {
        return admission.GetFailReason();
    }
    Alloc = std::make_shared<TTxAllocatorState>(AppData()->FunctionRegistry,
        Request.TxAlloc->TimeProvider, Request.TxAlloc->RandomProvider);
    auto guard = Alloc->TypeEnv.BindAllocator();
    Alloc->Alloc->SetLimit(limit);
    auto taskQuota = CreateTaskQuotaManager(Quota, limit);
    YQL_ENSURE(taskQuota->AllocateQuota(limit, false));
    // As in a normal compute task, the initial reservation is a guarantee,
    // not an additional query memory limit. Charge every growth to the same
    // resource manager and retain the configured per-query ceiling.
    Alloc->Alloc->Ref().SetIncreaseMemoryLimitCallback(
        [quota = std::move(taskQuota), alloc = Alloc->Alloc.get(), maxMemory = Request.MkqlMemoryLimit](ui64 current, ui64 required) {
            if ((!maxMemory || required <= maxMemory) && quota->AllocateQuota(required - current, false)) {
                alloc->SetLimit(required);
            }
        });
    ResultType = ImportTypeFromProto(Request.Transactions.front().Body->GetResults(0).GetItemType(), Alloc->TypeEnv);
    if (snapshot.IsValid()) {
        Settings.MutableSnapshot()->SetStep(snapshot.Step);
        Settings.MutableSnapshot()->SetTxId(snapshot.TxId);
    }
    Settings.SetUseFollowers(true);
    Settings.SetQuerySpanId(querySpanId);

    NYql::NDqProto::TTaskInput desc;
    desc.MutableTransform()->SetType("VectorSearchExecution");
    desc.MutableTransform()->MutableSettings()->PackFrom(Settings);
    const THashMap<TString, TString> empty;
    NUdf::TUnboxedValue stream(Alloc->HolderFactory.Create<TSingleVectorStream>(Alloc->HolderFactory, std::move(Target)));
    auto [input, actor] = factory.CreateDqInputTransform({
        .InputDesc = desc,
        .InputIndex = 0,
        .StatsLevel = NYql::NDq::TCollectStatsLevel::Basic,
        .TxId = Quota->GetTx()->TxId,
        .TaskId = 1,
        .TransformInput = stream,
        .SecureParams = empty,
        .TaskParams = empty,
        .ComputeActorId = parent,
        .TaskCounters = {},
        .TypeEnv = Alloc->TypeEnv,
        .HolderFactory = Alloc->HolderFactory,
        .Alloc = Alloc->Alloc,
        .TraceId = NWilson::TTraceId(traceId),
    });
    TActivationContext::RegisterWithSameMailbox(actor, parent);
    Input = input;
    return {};
}

bool TKqpVectorExecution::Poll(NYql::NDq::TDqSerializedBatch& result) {
    const auto started = GetCycleCountFast();
    Y_DEFER { CpuTimeUs += NHPTimer::GetSeconds(GetCycleCountFast() - started) * 1'000'000; };
    auto guard = Alloc->TypeEnv.BindAllocator();
    NMiniKQL::TUnboxedValueBatch batch;
    bool finished = false;
    TMaybe<TInstant> watermark;
    Input->GetAsyncInputData(batch, watermark, finished, Max<i64>());
    if (!finished) {
        YQL_ENSURE(batch.empty(), "Vector search must finish ranking before emitting rows");
        return false;
    }
    // TopSort compares SQL Optional<Float> values, including NULL and NaN.
    // Usually the search has already produced that order, so avoid a second
    // sort unless these special values require it.
    NMiniKQL::TUnboxedValueVector ordered;
    batch.ForEachRow([&](const NUdf::TUnboxedValue& row) { ordered.emplace_back(row); });
    const auto less = [index = Settings.GetScoreColumnIndex()](const auto& left, const auto& right) {
        return NMiniKQL::CompareValues(NUdf::EDataSlot::Float, true, true,
            left.GetElement(index), right.GetElement(index)) < 0;
    };
    if (!std::is_sorted(ordered.begin(), ordered.end(), less)) {
        std::sort(ordered.begin(), ordered.end(), less);
    }
    NMiniKQL::TUnboxedValueBatch projected;
    for (const auto& row : ordered) {
        NUdf::TUnboxedValue* fields = nullptr;
        NUdf::TUnboxedValue output = Alloc->HolderFactory.CreateDirectArrayHolder(Plan.ProjectionSize(), fields);
        for (ui32 i = 0; i < Plan.ProjectionSize(); ++i) {
            const auto& column = Plan.GetProjection(i);
            auto value = row.GetElement(column.GetSourceIndex());
            if (column.GetIntegerToString()) {
                fields[i] = NMiniKQL::MakeString(column.GetSignedInteger()
                    ? ToString(value.Get<i64>()) : ToString(value.Get<ui64>()));
            } else {
                fields[i] = value;
            }
        }
        projected.emplace_back(std::move(output));
    }
    NYql::NDq::TDqDataSerializer serializer(Alloc->TypeEnv, Alloc->HolderFactory,
        NYql::NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0,
        NMiniKQL::EValuePackerVersion::V0, NYql::DefaultDatumValidationMode);
    result = serializer.Serialize(projected, ResultType);
    return true;
}

void TKqpVectorExecution::FinishStats(TQueryExecutionStats& stats) {
    NYql::NDqProto::TDqTaskStats sourceStats;
    if (Input) {
        Input->FillExtraStats(&sourceStats, true, nullptr);
    }
    // The shared statistics exporter aggregates four lanes at a time.
    stats.ComputeCpuTimeUs.Resize(4);
    stats.ComputeCpuTimeUs.SetNonZero(0, static_cast<ui64>(CpuTimeUs) + sourceStats.GetCpuTimeUs());
    stats.AddBufferStats(std::move(sourceStats));
}

} // namespace NKikimr::NKqp
