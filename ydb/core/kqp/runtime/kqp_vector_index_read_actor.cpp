#include "kqp_vector_index_read_actor.h"
#include "kqp_read_actor.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <util/string/vector.h>

#include <algorithm>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NDataShard;
using NTableIndex::NKMeans::TClusterId;

// Specialized input-transform actor for vector KMeans tree index search.
//
// Takes a single target vector value as input and performs the whole search:
//   1) Level traversal: descends the KMeans tree (IndexLevels rounds), at each
//      round reading child clusters from the level table and keeping the LevelTop
//      nearest to the target vector.
//   2) Posting scan: for each resulting leaf cluster, range-scans the posting
//      table to collect candidate primary keys.
//   3) Main read: point-looks-up the main table for the candidate PKs, ranks the
//      rows by distance to the target and keeps the TopK nearest.
// The TopK rows (with the requested output columns) are returned to the compute
// actor, already sorted by distance.
class TKqpVectorIndexReadActor : public NActors::TActorBootstrapped<TKqpVectorIndexReadActor>, public NYql::NDq::IDqComputeActorAsyncInput {

    enum class EPhase {
        WaitInput,    // waiting for the target vector
        Level,        // traversing the KMeans tree level table
        Posting,      // scanning posting table for candidate PKs
        Main,         // reading main table rows for candidate PKs
        Done,         // results ready to be drained
    };

public:
    TKqpVectorIndexReadActor(
        NKikimrTxDataShard::TKqpVectorIndexReadSettings&& settings,
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
        TIntrusivePtr<TKqpCounters> counters,
        TIntrusivePtr<TVectorIndexLevelsCache> levelsCache)
        : Settings(std::move(settings))
        , TopK(Settings.GetTopK())
        , LevelTop(std::max<ui32>(1, Settings.GetLevelTop()))
        , OverlapClusters(Settings.GetOverlapClusters() > 0 ? Settings.GetOverlapClusters() : 1)
        , OverlapRatio(Settings.GetOverlapRatio())
        , LogPrefix(TStringBuilder() << "VectorIndexReadActor, inputIndex: " << inputIndex << ", CA Id " << computeActorId)
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
        , LevelsCache(std::move(levelsCache))
        , MySpan(TWilsonKqp::VectorResolveActor, NWilson::TTraceId(traceId), "VectorIndexReadActor")
    {
        IngressStats.Level = statsLevel;
        Snapshot = IKqpGateway::TKqpSnapshot(Settings.GetSnapshot().GetStep(), Settings.GetSnapshot().GetTxId());

        // The KMeans level table is immutable, so its rows can be cached across
        // queries keyed by (level table path, parent cluster id). Only enable when
        // the process has provisioned a non-empty cache.
        const auto& levelTableId = Settings.GetLevelTable().GetTableId();
        LevelTablePathId = TPathId(levelTableId.GetOwnerId(), levelTableId.GetTableId());
        UseLevelCache = LevelsCache && LevelsCache->MaxBytes() > 0;

        MainKeyTypeInfos.reserve(Settings.MainTableKeyColumnsSize());
        for (const auto& pk : Settings.GetMainTableKeyColumns()) {
            MainKeyTypeInfos.push_back(NScheme::TypeInfoFromProto(pk.GetType(), pk.GetTypeInfo()));
        }
    }

    virtual ~TKqpVectorIndexReadActor() {
        if (Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();
            ClearResults();
        }
    }

    void Bootstrap() {
        CA_LOG_D("Start vector index read actor");
        Become(&TKqpVectorIndexReadActor::StateFunc);
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
        StopInnerRead();
        {
            auto guard = BindAllocator();
            Input.Clear();
            ClearResults();
        }
        MySpan.End();
        TActorBootstrapped<TKqpVectorIndexReadActor>::PassAway();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");

        if (Phase == EPhase::WaitInput && !Failed) {
            StartSearch();
        }

        i64 totalDataSize = 0;
        if (Phase == EPhase::Done) {
            totalDataSize = ReplyResult(batch, freeSpace);
        }

        finished = (Phase == EPhase::Done && ResultRows.empty());

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

    // ---- Search driver -----------------------------------------------------

    void StartSearch() {
        if (!Settings.HasIndexSettings()) {
            RuntimeError("Index settings are required", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }
        auto status = FetchTarget();
        if (status == NUdf::EFetchStatus::Yield) {
            // The target vector input has not arrived yet. Stay in WaitInput; the
            // compute actor re-polls GetAsyncInputData once the input is ready.
            return;
        }
        if (status != NUdf::EFetchStatus::Ok) {
            // Input stream ended without a usable target vector: empty result.
            Phase = EPhase::Done;
            return;
        }

        TString error;
        RankClusters = NKikimr::NKMeans::CreateClustersAutoDetect(Settings.GetIndexSettings(), TargetVector, 0, error);
        if (!RankClusters) {
            RuntimeError(error, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }

        // Begin level traversal from the root cluster.
        CurrentParents = {0};
        LevelsRemaining = std::max<ui32>(1, Settings.GetIndexLevels());
        CA_LOG_D("StartSearch: targetBytes=" << TargetVector.size() << " indexLevels=" << Settings.GetIndexLevels()
            << " levelTop=" << LevelTop << " topK=" << TopK);
        StartLevelRound();
    }

    // Fetches the single target-vector input row. Returns:
    //   Ok     - target captured, ready to search;
    //   Yield  - input not ready yet, the caller should retry on the next poll;
    //   Finish - input ended without a usable target (search yields empty result).
    NUdf::EFetchStatus FetchTarget() {
        auto guard = BindAllocator();
        NUdf::TUnboxedValue row;
        auto status = Input.Fetch(row);
        if (status != NUdf::EFetchStatus::Ok) {
            return status;
        }
        // The input is a single-field struct carrying the target vector value.
        auto value = row.GetElement(0);
        if (!value.IsString() && !value.IsEmbedded()) {
            return NUdf::EFetchStatus::Finish;
        }
        TargetVector = TString(value.AsStringRef());
        // Drain the rest of the input (there should be exactly one row).
        NUdf::TUnboxedValue extra;
        while (Input.Fetch(extra) == NUdf::EFetchStatus::Ok) {}
        return TargetVector.empty() ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Ok;
    }

    void StartLevelRound() {
        LevelChildren.clear();
        ParentQueue.assign(CurrentParents.begin(), CurrentParents.end());
        Phase = EPhase::Level;
        ProcessNextRead();
    }

    void OnLevelRoundDone() {
        // Rank all collected children of this level and keep the LevelTop nearest.
        CurrentParents.clear();
        if (!LevelChildren.empty()) {
            TString error;
            auto clusters = NKikimr::NKMeans::CreateClusters(Settings.GetIndexSettings(), 0, error);
            if (!clusters) {
                RuntimeError(error, NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                return;
            }
            TVector<TClusterId> ids;
            TVector<TString> centroids;
            ids.reserve(LevelChildren.size());
            centroids.reserve(LevelChildren.size());
            for (auto& [id, centroid] : LevelChildren) {
                ids.push_back(id);
                centroids.push_back(std::move(centroid));
            }
            if (!clusters->SetClusters(std::move(centroids))) {
                RuntimeError("Invalid centroids in level table", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                return;
            }
            std::vector<std::pair<ui32, double>> nearest;
            clusters->FindClusters(TargetVector, nearest, LevelTop, /* skipRatio */ 0.0);
            for (auto& [pos, _] : nearest) {
                CurrentParents.push_back(ids[pos]);
            }
        }
        LevelChildren.clear();

        {
            TStringBuilder sb;
            for (auto id : CurrentParents) { sb << id << ","; }
            CA_LOG_D("Level round done: selectedParents=[" << sb << "] levelsRemaining=" << (LevelsRemaining - 1));
        }
        --LevelsRemaining;
        if (LevelsRemaining == 0 || CurrentParents.empty()) {
            StartPosting();
        } else {
            StartLevelRound();
        }
    }

    void StartPosting() {
        // Candidate leaf clusters are now in CurrentParents.
        ParentQueue.assign(CurrentParents.begin(), CurrentParents.end());
        PostingKeys.clear();
        Phase = EPhase::Posting;
        ProcessNextRead();
    }

    void OnPostingDone() {
        CA_LOG_D("Posting done: candidatePKs=" << PostingKeys.size());
        if (PostingKeys.empty()) {
            Phase = EPhase::Done;
            NotifyCA();
            return;
        }
        StartMain();
    }

    void StartMain() {
        Phase = EPhase::Main;
        StartMainRead();
    }

    void OnMainDone() {
        CA_LOG_D("Main done: candidateRows=" << Candidates.size() << " topK=" << TopK);
        // Keep the TopK nearest rows by distance.
        auto guard = BindAllocator();
        const size_t keep = std::min<size_t>(TopK, Candidates.size());
        std::partial_sort(Candidates.begin(), Candidates.begin() + keep, Candidates.end(),
            [](const TCandidate& a, const TCandidate& b) { return a.Distance < b.Distance; });
        Candidates.resize(keep);
        for (auto& c : Candidates) {
            ResultRows.push_back(std::move(c.Row));
        }
        Candidates.clear();
        Phase = EPhase::Done;
        NotifyCA();
    }

    // Drives the per-parent sequence of level/posting reads.
    void ProcessNextRead() {
        if (Failed) {
            return;
        }
        if (ParentQueue.empty()) {
            if (Phase == EPhase::Level) {
                OnLevelRoundDone();
            } else if (Phase == EPhase::Posting) {
                OnPostingDone();
            }
            return;
        }
        auto parent = ParentQueue.front();
        ParentQueue.pop_front();
        if (Phase == EPhase::Level) {
            StartLevelRead(parent);
        } else {
            StartPostingRead(parent);
        }
    }

    // ---- Inner reads -------------------------------------------------------

    static TSerializedTableRange SingleColumnPointRange(ui64 value) {
        // Range matching rows whose first key column equals `value`:
        // (value-1, value] open on the left (null is -inf), missing suffix is +inf.
        auto from = value > 0 ? TCell::Make(value - 1) : TCell();
        auto to = TCell::Make(value);
        return TSerializedTableRange({&from, 1}, false, {&to, 1}, true);
    }

    // immutableFollowerRead: the table is an immutable index impl table being read
    // under stale-RO. Such reads skip the MVCC snapshot and go to followers.
    NKikimrTxDataShard::TKqpReadRangesSourceSettings* MakeSourceSettings(
        TIntrusivePtr<NActors::TProtoArenaHolder>& arena, const NKikimrTxDataShard::TKqpTransaction::TTableMeta& table,
        bool immutableFollowerRead)
    {
        arena = MakeIntrusive<NActors::TProtoArenaHolder>();
        auto* src = arena->Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
        src->SetDatabase(Settings.GetDatabase());
        if (Settings.HasPoolId()) {
            src->SetPoolId(Settings.GetPoolId());
        }
        *src->MutableTable() = table;
        src->SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        if (immutableFollowerRead) {
            // No snapshot: the read actor only routes to followers when no snapshot
            // is set; the table is immutable, so inconsistent reads are safe.
            src->SetUseFollowers(true);
            src->SetAllowInconsistentReads(true);
        } else if (Snapshot.IsValid()) {
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
        if (Settings.HasQuerySpanId()) {
            src->SetQuerySpanId(Settings.GetQuerySpanId());
        }
        return src;
    }

    static void AddUint64KeyColumnType(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src) {
        src->AddKeyColumnTypes(NScheme::NTypeIds::Uint64);
        src->AddKeyColumnTypeInfos();
    }

    static void AddColumnMetaKeyColumnType(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src,
        const NKikimrTxDataShard::TKqpTransaction::TColumnMeta& col)
    {
        src->AddKeyColumnTypes(col.GetType());
        if (col.HasTypeInfo()) {
            *src->AddKeyColumnTypeInfos() = col.GetTypeInfo();
        } else {
            src->AddKeyColumnTypeInfos();
        }
    }

    static void AddColumn(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src,
        ui32 id, const TString& name, ui32 typeId, const NKikimrProto::TTypeInfo* typeInfo, bool notNull)
    {
        auto* meta = src->AddColumns();
        meta->SetId(id);
        meta->SetName(name);
        meta->SetType(typeId);
        if (typeInfo) {
            *meta->MutableTypeInfo() = *typeInfo;
        } else {
            *meta->MutableTypeInfo() = NKikimrProto::TTypeInfo();
        }
        meta->SetNotNull(notNull);
    }

    void StartLevelRead(TClusterId parent) {
        ReadingParent = parent;

        // Serve the parent's children from the shared level cache when possible.
        if (UseLevelCache) {
            CurrentLevelCacheKey = TSerializedCellVec::Serialize({TCell::Make(parent)});
            auto cached = LevelsCache->Get(LevelTablePathId, CurrentLevelCacheKey);
            if (cached && !cached->BatchRows.empty()) {
                for (TConstArrayRef<TCell> row : cached->BatchRows) {
                    // Cached row layout: [id (Uint64), centroid (String)].
                    TClusterId child = row[0].AsValue<ui64>();
                    LevelChildren[child] = TString(row[1].AsBuf());
                }
                CachingCurrentLevel = false;
                ProcessNextRead();
                return;
            }
            // Cache miss: accumulate the read rows so we can populate the cache.
            CachingCurrentLevel = true;
            CurrentLevelBatch = TOwnedCellVecBatch();
        } else {
            CachingCurrentLevel = false;
        }

        TIntrusivePtr<NActors::TProtoArenaHolder> arena;
        auto* src = MakeSourceSettings(arena, Settings.GetLevelTable(), Settings.GetUseFollowers());
        SingleColumnPointRange(parent).Serialize(*src->MutableFullRange());

        // Level table key is (parent, id), both Uint64.
        AddUint64KeyColumnType(src);
        AddUint64KeyColumnType(src);

        AddColumn(src, Settings.GetLevelTableParentColumnId(), NTableIndex::NKMeans::ParentColumn, NScheme::NTypeIds::Uint64, nullptr, true);
        AddColumn(src, Settings.GetLevelTableClusterColumnId(), NTableIndex::NKMeans::IdColumn, NScheme::NTypeIds::Uint64, nullptr, true);
        AddColumn(src, Settings.GetLevelTableCentroidColumnId(), NTableIndex::NKMeans::CentroidColumn, NScheme::NTypeIds::String, nullptr, true);

        LaunchInnerRead(src, arena);
    }

    void StartPostingRead(TClusterId parent) {
        TIntrusivePtr<NActors::TProtoArenaHolder> arena;
        auto* src = MakeSourceSettings(arena, Settings.GetPostingTable(), Settings.GetUseFollowers());
        SingleColumnPointRange(parent).Serialize(*src->MutableFullRange());

        // Posting key is (__ydb_parent, <main PK columns>).
        AddUint64KeyColumnType(src);
        for (const auto& pk : Settings.GetMainTableKeyColumns()) {
            AddColumnMetaKeyColumnType(src, pk);
        }

        // Read just the PK columns (using posting table column ids).
        YQL_ENSURE(Settings.PostingTableKeyColumnIdsSize() == Settings.MainTableKeyColumnsSize() + 1);
        for (size_t i = 0; i < Settings.MainTableKeyColumnsSize(); ++i) {
            const auto& pk = Settings.GetMainTableKeyColumns(i);
            const NKikimrProto::TTypeInfo* ti = pk.HasTypeInfo() ? &pk.GetTypeInfo() : nullptr;
            AddColumn(src, Settings.GetPostingTableKeyColumnIds(i + 1), pk.GetName(), pk.GetType(), ti, pk.GetNotNull());
        }

        ReadingParent = parent;
        LaunchInnerRead(src, arena);
    }

    void StartMainRead() {
        TIntrusivePtr<NActors::TProtoArenaHolder> arena;
        // The main table is mutable, so it is always read from the leader with the
        // query snapshot (never from followers).
        auto* src = MakeSourceSettings(arena, Settings.GetMainTable(), /* immutableFollowerRead */ false);

        // The read actor consumes point lookups in the given order, matching them
        // against partition ranges in lockstep; so the points must be globally
        // sorted ascending by typed key comparison. The posting scan collects PKs
        // per leaf cluster, so they are not globally ordered yet.
        const auto* keyTypes = MainKeyTypeInfos.data();
        const ui32 keyCount = MainKeyTypeInfos.size();
        std::sort(PostingKeys.begin(), PostingKeys.end(), [keyTypes, keyCount](const TString& a, const TString& b) {
            TSerializedCellVec va(a);
            TSerializedCellVec vb(b);
            return CompareTypedCellVectors(va.GetCells().data(), vb.GetCells().data(), keyTypes, keyCount) < 0;
        });

        // Point lookups by primary key.
        auto* ranges = src->MutableRanges();
        for (const auto& key : PostingKeys) {
            ranges->AddKeyPoints(key);
        }

        for (const auto& pk : Settings.GetMainTableKeyColumns()) {
            AddColumnMetaKeyColumnType(src, pk);
        }
        for (const auto& col : Settings.GetOutputColumns()) {
            const NKikimrProto::TTypeInfo* ti = col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr;
            AddColumn(src, col.GetId(), col.GetName(), col.GetType(), ti, col.GetNotNull());
        }

        LaunchInnerRead(src, arena);
    }

    void LaunchInnerRead(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src, TIntrusivePtr<NActors::TProtoArenaHolder>& arena) {
        auto [readActorInput, readActor] = CreateKqpReadActor(src, arena, this->SelfId(),
            0, IngressStats.Level, TxId, TaskId, TypeEnv, HolderFactory, Alloc, TraceId, Counters);
        ReadActorInput = readActorInput;
        ReadActorId = RegisterWithSameMailbox(readActor);
        HandleRead(nullptr);
    }

    void StopInnerRead() {
        if (ReadActorId) {
            Send(ReadActorId, new TEvents::TEvPoison);
            ReadActorId = {};
        }
        ReadActorInput = nullptr;
    }

    void HandleRead(TEvNewAsyncInputDataArrived::TPtr) {
        if (!ReadActorInput) {
            return;
        }
        TMaybe<TInstant> watermark;
        ui64 freeSpace = 32 * 1024 * 1024;
        NKikimr::NMiniKQL::TUnboxedValueBatch rows;
        bool finished = false;
        {
            auto guard = BindAllocator();
            ReadActorInput->GetAsyncInputData(rows, watermark, finished, freeSpace);
            rows.ForEachRow([&](NUdf::TUnboxedValue& value) {
                ProcessReadRow(value);
            });
        }
        if (finished) {
            CollectLocks();
            StopInnerRead();
            {
                auto guard = BindAllocator();
                rows.clear();
            }
            if (!Failed) {
                if (Phase == EPhase::Level && CachingCurrentLevel && !CurrentLevelBatch.empty()) {
                    // Populate the shared level cache with this parent's children.
                    auto data = MakeIntrusive<TCachedLevelTableData>();
                    data->BatchRows = std::move(CurrentLevelBatch);
                    LevelsCache->Put(LevelTablePathId, CurrentLevelCacheKey, data);
                    CachingCurrentLevel = false;
                }
                if (Phase == EPhase::Main) {
                    // The main read is a single read (not per-parent), so finishing
                    // it means all candidate rows have been collected.
                    OnMainDone();
                } else {
                    ProcessNextRead();
                }
            }
            return;
        }
        auto guard = BindAllocator();
        rows.clear();
    }

    void ProcessReadRow(NUdf::TUnboxedValue& value) {
        switch (Phase) {
            case EPhase::Level: {
                TClusterId parent = value.GetElement(0).Get<ui64>();
                if (parent != ReadingParent) {
                    RuntimeError("Returned clusters for invalid parent", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                    return;
                }
                TClusterId child = value.GetElement(1).Get<ui64>();
                auto centroid = value.GetElement(2);
                auto centroidRef = centroid.AsStringRef();
                LevelChildren[child] = TString(centroidRef);
                if (CachingCurrentLevel) {
                    // Store [id (Uint64), centroid (String)]; Append copies the cells.
                    TCell cells[2] = {
                        TCell::Make(child),
                        TCell(centroidRef.Data(), centroidRef.Size()),
                    };
                    CurrentLevelBatch.Append(TConstArrayRef<TCell>(cells, 2));
                }
                break;
            }
            case EPhase::Posting: {
                // Build a serialized PK cell vec from the read PK columns.
                const ui32 n = MainKeyTypeInfos.size();
                TVector<TCell> cells(n);
                for (ui32 i = 0; i < n; ++i) {
                    cells[i] = NMiniKQL::MakeCell(MainKeyTypeInfos[i], value.GetElement(i), TypeEnv, /* copy */ true);
                }
                TString serialized = TSerializedCellVec::Serialize(cells);
                if (SeenKeys.insert(serialized).second) {
                    PostingKeys.push_back(std::move(serialized));
                }
                break;
            }
            case EPhase::Main: {
                auto embedding = value.GetElement(Settings.GetVectorColumnIndex());
                double distance = std::numeric_limits<double>::max();
                if (embedding.IsString() || embedding.IsEmbedded()) {
                    distance = RankClusters->CalcDistance(TargetVector, embedding.AsStringRef());
                }
                // Build a fresh output struct holder in OutputColumns order.
                NUdf::TUnboxedValue* items = nullptr;
                auto row = HolderFactory.CreateDirectArrayHolder(Settings.OutputColumnsSize(), items);
                for (ui32 i = 0; i < Settings.OutputColumnsSize(); ++i) {
                    items[i] = value.GetElement(i);
                }
                Candidates.push_back(TCandidate{distance, std::move(row)});
                break;
            }
            default:
                break;
        }
    }

    void CollectLocks() {
        auto extra = ReadActorInput->ExtraData();
        if (extra) {
            NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
            YQL_ENSURE(extra->UnpackTo(&resultInfo));
            for (size_t i = 0; i < resultInfo.LocksSize(); i++) {
                Locks.push_back(resultInfo.GetLocks(i));
            }
        }
    }

    void NotifyCA() {
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void OnAsyncInputError(const IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        RuntimeError("Error reading vector index tables", ev->Get()->FatalCode, ev->Get()->Issues);
    }

    i64 ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) {
        auto guard = BindAllocator();
        i64 totalSize = 0;
        while (!ResultRows.empty() && freeSpace > 0) {
            auto& row = ResultRows.front();
            const i64 rowSize = sizeof(NUdf::TUnboxedValuePod) * Settings.OutputColumnsSize();
            batch.emplace_back(std::move(row));
            ResultRows.pop_front();
            totalSize += rowSize;
            freeSpace -= rowSize;
            SentRows++;
            SentBytes += rowSize;
        }
        return totalSize;
    }

    void ClearResults() {
        NKikimr::NMiniKQL::TUnboxedValueDeque empty;
        empty.swap(ResultRows);
        Candidates.clear();
    }

    void FillExtraStats(NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats*) override {
        if (last) {
            NDqProto::TDqTableStats* tableStats = nullptr;
            const auto& path = Settings.GetMainTable().GetTablePath();
            for (size_t i = 0; i < stats->TablesSize(); ++i) {
                if (stats->GetTables(i).GetTablePath() == path) {
                    tableStats = stats->MutableTables(i);
                }
            }
            if (!tableStats) {
                tableStats = stats->AddTables();
                tableStats->SetTablePath(path);
            }
            tableStats->SetReadRows(tableStats->GetReadRows() + SentRows);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + SentBytes);
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

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

private:
    struct TCandidate {
        double Distance;
        NUdf::TUnboxedValue Row;
    };

    // Parameters
    const NKikimrTxDataShard::TKqpVectorIndexReadSettings Settings;
    const ui32 TopK;
    const ui32 LevelTop;
    const ui32 OverlapClusters;
    const double OverlapRatio;
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

    // State
    EPhase Phase = EPhase::WaitInput;
    bool Failed = false;
    TString TargetVector;
    TVector<NScheme::TTypeInfo> MainKeyTypeInfos;
    std::unique_ptr<NKikimr::NKMeans::IClusters> RankClusters;

    ui32 LevelsRemaining = 0;
    TVector<TClusterId> CurrentParents;
    TDeque<TClusterId> ParentQueue;
    TClusterId ReadingParent = 0;
    TMap<TClusterId, TString> LevelChildren;

    // Shared, process-wide cache of immutable level-table rows.
    TIntrusivePtr<TVectorIndexLevelsCache> LevelsCache;
    TPathId LevelTablePathId;
    bool UseLevelCache = false;
    bool CachingCurrentLevel = false;
    TString CurrentLevelCacheKey;
    TOwnedCellVecBatch CurrentLevelBatch;

    TVector<TString> PostingKeys;         // serialized PK cell vecs
    THashSet<TString> SeenKeys;

    TVector<TCandidate> Candidates;
    NKikimr::NMiniKQL::TUnboxedValueDeque ResultRows;

    NYql::NDq::IDqComputeActorAsyncInput* ReadActorInput = nullptr;
    TActorId ReadActorId = {};

    TVector<NKikimrDataEvents::TLock> Locks;
    ui64 SentRows = 0;
    ui64 SentBytes = 0;

    NWilson::TSpan MySpan;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpVectorIndexReadActor(
    NKikimrTxDataShard::TKqpVectorIndexReadSettings&& settings,
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
    TIntrusivePtr<TKqpCounters> counters,
    TIntrusivePtr<TVectorIndexLevelsCache> levelsCache)
{
    auto actor = new TKqpVectorIndexReadActor(std::move(settings), inputIndex, input, statsLevel, txId,
        taskId, computeActorId, typeEnv, holderFactory, alloc, traceId, counters, std::move(levelsCache));
    return {actor, actor};
}

void RegisterKqpVectorIndexReadActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters,
    TIntrusivePtr<TVectorIndexLevelsCache> levelsCache) {
    factory.RegisterInputTransform<NKikimrTxDataShard::TKqpVectorIndexReadSettings>(
        "VectorIndexReadInputTransformer", [counters, levelsCache](NKikimrTxDataShard::TKqpVectorIndexReadSettings&& settings,
            NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) -> std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> {
            return CreateKqpVectorIndexReadActor(std::move(settings),
                args.InputIndex, args.TransformInput, args.StatsLevel, args.TxId, args.TaskId, args.ComputeActorId,
                args.TypeEnv, args.HolderFactory, args.Alloc, args.TraceId, counters, levelsCache);
        });
}

} // namespace NKqp
} // namespace NKikimr
