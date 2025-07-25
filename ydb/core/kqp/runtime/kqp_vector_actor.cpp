#include "kqp_vector_actor.h"
#include "kqp_read_actor.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <util/string/vector.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NDataShard;

class TKqpVectorResolveActor : public NActors::TActorBootstrapped<TKqpVectorResolveActor>, public NYql::NDq::IDqComputeActorAsyncInput {

public:
    TKqpVectorResolveActor(
        NKikimrTxDataShard::TKqpVectorResolveSettings&& settings,
        ui64 inputIndex,
        const NUdf::TUnboxedValue& input,
        NYql::NDq::TCollectStatsLevel statsLevel,
        NYql::NDq::TTxId txId,
        ui64 taskId,
        const NActors::TActorId& computeActorId,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory,
        std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
        const NWilson::TTraceId& traceId,
        TIntrusivePtr<TKqpCounters> counters)
        : Settings(std::move(settings))
        , LogPrefix(TStringBuilder() << "VectorResolveActor, inputIndex: " << inputIndex << ", CA Id " << computeActorId)
        , InputIndex(inputIndex)
        , Input(input)
        , TxId(txId)
        , TaskId(taskId)
        , ComputeActorId(computeActorId)
        , TypeEnv(typeEnv)
        , HolderFactory(holderFactory)
        , Alloc(alloc)
        , Counters(counters)
        , TraceId(traceId)
        , MySpan(TWilsonKqp::VectorResolveActor, NWilson::TTraceId(traceId), "VectorResolveActor")
    {
        IngressStats.Level = statsLevel;

        Snapshot = IKqpGateway::TKqpSnapshot(Settings.GetSnapshot().GetStep(), Settings.GetSnapshot().GetTxId());

        InitResultColumns();
    }

    virtual ~TKqpVectorResolveActor() {
        if (Input.HasValue() && Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();

            NKikimr::NMiniKQL::TUnboxedValueDeque emptyList;
            emptyList.swap(PendingRows);
        }
    }

    void Bootstrap() {
        //Counters->VectorResolveActorsCount->Inc();

        CA_LOG_D("Start vector resolve actor");
        Become(&TKqpVectorResolveActor::StateFunc);
    }

private:
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDq::TSourceState&) override {}
    void LoadState(const NYql::NDq::TSourceState&) override {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) override {}

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvNewAsyncInputDataArrived, HandleRead);
                hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, OnAsyncInputError);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    void PassAway() final {
        //Counters->VectorResolveActorsCount->Dec();

        if (ReadActorId) {
            Send(ReadActorId, new TEvents::TEvPoison);
            ReadActorId = {};
        }

        {
            auto guard = BindAllocator();
            Input.Clear();
            NKikimr::NMiniKQL::TUnboxedValueDeque emptyList;
            emptyList.swap(PendingRows);
        }

        MySpan.End();

        TActorBootstrapped<TKqpVectorResolveActor>::PassAway();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");

        i64 totalDataSize = LevelsFinished ? ReplyResult(batch, freeSpace) : 0;
        finished = false;
        if (!PendingRows.size()) {
            auto status = FetchRows();
            if (PendingRows.size()) {
                LevelsFinished = false;
                ResolvedLevel = 0;
                PrevClusters.clear();
                ContinueResolveClusters();
                Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
            } else {
                finished = (status == NUdf::EFetchStatus::Finish);
            }
        }

        CA_LOG_D("Returned " << totalDataSize << " bytes, finished: " << finished);
        return totalDataSize;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        google::protobuf::Any result;
        NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
        for (auto& lock : Locks) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }
        result.PackFrom(resultInfo);
        return result;
    }

    NUdf::EFetchStatus FetchRows() {
        auto guard = BindAllocator();

        NUdf::EFetchStatus status;
        NUdf::TUnboxedValue currentValue;

        while ((status = Input.Fetch(currentValue)) == NUdf::EFetchStatus::Ok) {
            PendingRows.push_back(std::move(currentValue));
        }

        return status;
    }

    void ContinueResolveClusters() {
        if (Failed) {
            return;
        }
        if (!Settings.HasIndexSettings()) {
            RuntimeError("Index settings are required", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }
        while (!LevelsFinished) {
            if (!LevelClusters.size()) {
                LevelClusters.clear();
                if (!ResolvedLevel) {
                    LevelClusters.insert(0);
                } else {
                    for (auto cluster: PrevClusters) {
                        if (!(cluster & NKikimr::NTableIndex::PostingParentFlag)) {
                            LevelClusters.insert(cluster);
                        }
                    }
                }
                if (!LevelClusters.size()) {
                    // All clusters are invalid (0) or already leaf (PostingParentFlag is set)
                    LevelsFinished = true;
                    break;
                }
                NextClusters.clear();
                NextClusters.resize(PendingRows.size());
                CurClusters.reset();
            }
            while (LevelClusters.size() > 0) {
                auto cluster = *LevelClusters.begin();
                if (ResolvedLevel > 0 ? !CurClusters : !RootClusters) {
                    ReadChildClusters(cluster);
                    return;
                }
                auto & clusters = (ResolvedLevel > 0 ? CurClusters : RootClusters);
                auto & clusterIds = (ResolvedLevel > 0 ? CurClusterIds : RootClusterIds);
                for (size_t i = 0; i < PendingRows.size(); i++) {
                    if (ResolvedLevel > 0 && PrevClusters[i] != cluster) {
                        continue;
                    }
                    auto embedding = PendingRows[i].GetElement(Settings.GetVectorColumnIndex());
                    auto cluster = clusters->FindCluster(embedding.AsStringRef());
                    if (!cluster.has_value()) {
                        // embedding is invalid
                        NextClusters[i] = 0;
                    } else {
                        NextClusters[i] = clusterIds[*cluster];
                    }
                }
                CurClusters.reset();
                LevelClusters.erase(cluster);
            }
            PrevClusters = std::move(NextClusters);
            ResolvedLevel++;
        }
        if (!Failed) {
            NotifyCA();
        }
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    static TTableRange RawParentRange(NTableIndex::TClusterId parent) {
        auto from = parent > 0 ? TCell::Make(parent - 1) : TCell(); // null is -infinity
        auto to = TCell::Make(parent); // missing key suffix is +infinity
        return TTableRange({&from, 1}, false, {&to, 1}, true);
    }

    static TSerializedTableRange ParentRange(NTableIndex::TClusterId parent) {
        auto from = parent > 0 ? TCell::Make(parent - 1) : TCell(); // null is -infinity
        auto to = TCell::Make(parent); // missing key suffix is +infinity
        return TSerializedTableRange({&from, 1}, false, {&to, 1}, true);
    }

    void ReadChildClusters(NTableIndex::TClusterId parent) {
        if (ReadingChildClusters) {
            return;
        }
        ReadingChildClusters = true;
        ReadingChildClustersOf = parent;
        ReadUsingActor(parent);
    }

    void ReadUsingActor(NTableIndex::TClusterId parent) {
        auto range = ParentRange(parent);
        auto arena = MakeIntrusive<NActors::TProtoArenaHolder>();
        auto src = arena->Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
        *src->MutableTable() = Settings.GetLevelTable();
        range.Serialize(*src->MutableFullRange());
        src->SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        if (Snapshot.IsValid()) {
            src->MutableSnapshot()->SetTxId(Snapshot.TxId);
            src->MutableSnapshot()->SetStep(Snapshot.Step);
        }
        if (Settings.HasLockTxId()) {
            src->SetLockTxId(Settings.GetLockTxId());
            if (Settings.HasLockMode()) {
                src->SetLockMode(Settings.GetLockMode());
            }
        }
        if (Settings.HasLockNodeId()) {
            src->SetLockNodeId(Settings.GetLockNodeId());
        }

        // Level table key is parent+id
        auto ui64Type = NScheme::ProtoColumnTypeFromTypeInfoMod(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "");
        src->AddKeyColumnTypes(ui64Type.TypeId);
        src->AddKeyColumnTypes(ui64Type.TypeId);
        *src->AddKeyColumnTypeInfos() = NKikimrProto::TTypeInfo();
        *src->AddKeyColumnTypeInfos() = NKikimrProto::TTypeInfo();

        auto* meta = src->AddColumns();
        meta->SetId(Settings.GetLevelTableParentColumnId());
        meta->SetName(NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn);
        meta->SetType(ui64Type.TypeId);
        *meta->MutableTypeInfo() = NKikimrProto::TTypeInfo();
        meta->SetNotNull(true);

        meta = src->AddColumns();
        meta->SetId(Settings.GetLevelTableClusterColumnId());
        meta->SetName(NTableIndex::NTableVectorKmeansTreeIndex::IdColumn);
        meta->SetType(ui64Type.TypeId);
        *meta->MutableTypeInfo() = NKikimrProto::TTypeInfo();
        meta->SetNotNull(true);

        auto stringType = NScheme::ProtoColumnTypeFromTypeInfoMod(NScheme::TTypeInfo(NScheme::NTypeIds::String), "");
        meta = src->AddColumns();
        meta->SetId(Settings.GetLevelTableCentroidColumnId());
        meta->SetName(NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn);
        meta->SetType(stringType.TypeId);
        *meta->MutableTypeInfo() = NKikimrProto::TTypeInfo();
        meta->SetNotNull(true);

        auto [ readActorInput, readActor ] = CreateKqpReadActor(src, arena, this->SelfId(),
            0, IngressStats.Level, TxId, TaskId, TypeEnv, HolderFactory, Alloc, TraceId, Counters);

        ReadActorInput = readActorInput;
        ReadActorId = RegisterWithSameMailbox(readActor);

        HandleRead(nullptr);
    }

    void HandleRead(TEvNewAsyncInputDataArrived::TPtr) {
        TMaybe<TInstant> watermark;
        ui64 freeSpace = 32*1024*1024; // FIXME The value doesn't really matter, but where to take it from?
        NKikimr::NMiniKQL::TUnboxedValueBatch rows;
        bool finished = false;
        {
            auto guard = BindAllocator();
            ReadActorInput->GetAsyncInputData(rows, watermark, finished, freeSpace);
            rows.ForEachRow([&](NUdf::TUnboxedValue& value) {
                NTableIndex::TClusterId parent = value.GetElement(0).Get<ui64>();
                if (parent != ReadingChildClustersOf) {
                    RuntimeError("Returned clusters for invalid parent", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                    return;
                }
                NTableIndex::TClusterId child = value.GetElement(1).Get<ui64>();
                auto centroid = value.GetElement(2);
                FetchedClusters[child] = TString(centroid.AsStringRef());
            });
        }
        if (finished) {
            auto extra = ReadActorInput->ExtraData();
            if (extra) {
                NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
                YQL_ENSURE(extra->UnpackTo(&resultInfo));
                for (size_t i = 0; i < resultInfo.LocksSize(); i++) {
                    Locks.push_back(resultInfo.GetLocks(i));
                }
            }
            Send(ReadActorId, new TEvents::TEvPoison);
            ReadActorId = {};
            ReadActorInput = nullptr;
            ReadingChildClusters = false;
            // Convert to NKikimr::NKMeans::TClusters
            if (!Failed) {
                ParseFetchedClusters();
            }
        }
        {
            auto guard = BindAllocator();
            rows.clear();
        }
    }

    void NotifyCA() {
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void OnAsyncInputError(const IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        RuntimeError("Error reading from level table", ev->Get()->FatalCode, ev->Get()->Issues);
    }

    void ParseFetchedClusters() {
        TString error;
        auto clusters = NKikimr::NKMeans::CreateClusters(Settings.GetIndexSettings(), 0, error);
        if (!clusters) {
            // Index settings are invalid for some reason
            RuntimeError(error, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }
        TVector<ui64> clusterIds;
        TVector<TString> clusterRows;
        for (auto & pp: FetchedClusters) {
            clusterIds.push_back(pp.first);
            clusterRows.push_back(std::move(pp.second));
        }
        if (!clusters->SetClusters(std::move(clusterRows))) {
            // Clusters are invalid for some reason
            RuntimeError("Child clusters of "+std::to_string(ReadingChildClustersOf)+" are invalid", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }
        FetchedClusters.clear();
        if (!ReadingChildClustersOf) {
            // Cache root clusters in RootClusters for future batches
            RootClusters = std::move(clusters);
            RootClusterIds = std::move(clusterIds);
        } else {
            CurClusters = std::move(clusters);
            CurClusterIds = std::move(clusterIds);
        }
        ContinueResolveClusters();
    }

    i64 ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) {
        auto guard = BindAllocator();

        i64 totalSize = 0;

        while (PendingRows.size() > 0 && freeSpace > 0) {
            i64 rowSize = 0;
            NUdf::TUnboxedValue currentValue = std::move(PendingRows.back());
            PendingRows.pop_back();

            NUdf::TUnboxedValue* rowItems = nullptr;
            // Output columns: Cluster ID + Source table PK [ + Data Columns ]
            auto newValue = HolderFactory.CreateDirectArrayHolder(1 + Settings.CopyColumnIndexesSize(), rowItems);

            *rowItems++ = NUdf::TUnboxedValuePod((ui64)PrevClusters[PendingRows.size()]);
            rowSize += sizeof(NUdf::TUnboxedValuePod);

            for (size_t i = 0; i < Settings.CopyColumnIndexesSize(); i++) {
                auto colIdx = Settings.GetCopyColumnIndexes(i);
                *rowItems++ = currentValue.GetElement(colIdx);
                rowSize += NMiniKQL::GetUnboxedValueSize(currentValue.GetElement(colIdx), ColumnTypeInfos[colIdx]).AllocatedBytes;
            }

            totalSize += rowSize;
            freeSpace -= rowSize;
            SentBytes += rowSize;
            SentRows++;

            batch.emplace_back(std::move(newValue));
        }

        return totalSize;
    }

    void FillExtraStats(NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats* mstats) override {
        if (last) {
            NDqProto::TDqTableStats* tableStats = nullptr;
            for (size_t i = 0; i < stats->TablesSize(); ++i) {
                auto* table = stats->MutableTables(i);
                if (table->GetTablePath() == Settings.GetLevelTable().GetTablePath()) {
                    tableStats = table;
                }
            }
            if (!tableStats) {
                tableStats = stats->AddTables();
                tableStats->SetTablePath(Settings.GetLevelTable().GetTablePath());
            }

            auto consumedRows = mstats ? mstats->Inputs[InputIndex]->RowsConsumed : SentRows;

            tableStats->SetReadRows(tableStats->GetReadRows() + consumedRows);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + (mstats ? mstats->Inputs[InputIndex]->BytesConsumed : SentBytes));
            // No idea how many partitions were affected by all ReadActors (they may overlap)
            //tableStats->SetAffectedPartitions(tableStats->GetAffectedPartitions() + AffectedShards);
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        Failed = true;

        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        if (MySpan) {
            MySpan.EndError(issues.ToOneLineString());
        }

        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

    void InitResultColumns() {
        YQL_ENSURE(Settings.InputColumnTypesSize() > 0);
        YQL_ENSURE(Settings.InputColumnTypesSize() == Settings.InputColumnTypeInfosSize());
        ColumnTypeInfos.reserve(Settings.InputColumnTypesSize());
        for (size_t i = 0; i < Settings.InputColumnTypesSize(); i++) {
            ColumnTypeInfos.push_back(NScheme::TypeInfoFromProto(Settings.GetInputColumnTypes(i), Settings.GetInputColumnTypeInfos(i)));
        }
    }

private:
    // Parameters

    const NKikimrTxDataShard::TKqpVectorResolveSettings Settings;
    const TString LogPrefix;
    const ui64 InputIndex;
    NUdf::TUnboxedValue Input;
    NYql::NDq::TDqAsyncStats IngressStats;
    NYql::NDq::TTxId TxId;
    ui64 TaskId;
    const NActors::TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<TKqpCounters> Counters;
    NWilson::TTraceId TraceId;

    IKqpGateway::TKqpSnapshot Snapshot;
    TVector<NScheme::TTypeInfo> ColumnTypeInfos;

    // State

    NKikimr::NMiniKQL::TUnboxedValueDeque PendingRows;
    bool ReadingChildClusters = false;
    NTableIndex::TClusterId ReadingChildClustersOf = {};

    NYql::NDq::IDqComputeActorAsyncInput* ReadActorInput = nullptr;
    TActorId ReadActorId = {};
    bool Failed = false;

    TMap<NTableIndex::TClusterId, TString> FetchedClusters;
    ui32 ResolvedLevel = 0;
    bool LevelsFinished = false;
    TVector<NTableIndex::TClusterId> PrevClusters;
    TVector<NTableIndex::TClusterId> NextClusters;
    TSet<NTableIndex::TClusterId> LevelClusters;
    std::unique_ptr<NKikimr::NKMeans::IClusters> RootClusters;
    TVector<NTableIndex::TClusterId> RootClusterIds;
    std::unique_ptr<NKikimr::NKMeans::IClusters> CurClusters;
    TVector<NTableIndex::TClusterId> CurClusterIds;

    TVector<NKikimrDataEvents::TLock> Locks;

    ui64 SentRows = 0;
    ui64 SentBytes = 0;

    NWilson::TSpan MySpan;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpVectorResolveActor(
    NKikimrTxDataShard::TKqpVectorResolveSettings&& settings,
    ui64 inputIndex,
    const NUdf::TUnboxedValue& input,
    NYql::NDq::TCollectStatsLevel statsLevel,
    NYql::NDq::TTxId txId,
    ui64 taskId,
    const NActors::TActorId& computeActorId,
    const NMiniKQL::TTypeEnvironment& typeEnv,
    const NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
    const NWilson::TTraceId& traceId,
    TIntrusivePtr<TKqpCounters> counters) {
    auto actor = new TKqpVectorResolveActor(std::move(settings), inputIndex, input, statsLevel, txId,
        taskId, computeActorId, typeEnv, holderFactory, alloc, traceId, counters);
    return {actor, actor};
}

void RegisterKqpVectorResolveActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterInputTransform<NKikimrTxDataShard::TKqpVectorResolveSettings>(
        "VectorResolveInputTransformer", [counters](NKikimrTxDataShard::TKqpVectorResolveSettings&& settings,
            NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) -> std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> {
            return CreateKqpVectorResolveActor(std::move(settings),
                args.InputIndex, args.TransformInput, args.StatsLevel, args.TxId, args.TaskId, args.ComputeActorId,
                args.TypeEnv, args.HolderFactory, args.Alloc, args.TraceId, counters);
        });
}

} // namespace NKqp
} // namespace NKikimr
