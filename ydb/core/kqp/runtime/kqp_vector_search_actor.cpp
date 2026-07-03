#include "kqp_vector_search_actor.h"
#include "kqp_read_actor.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

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
class TKqpVectorSearchActor : public NActors::TActorBootstrapped<TKqpVectorSearchActor>, public NYql::NDq::IDqComputeActorAsyncInput {

    enum class EPhase {
        WaitInput,    // waiting for the target vector
        Level,        // traversing the KMeans tree level table
        Posting,      // scanning posting table for candidate PKs (and, for a
                      // non-covered index, the pipelined main reads they feed)
        Done,         // results ready to be drained
    };

    // Each inner read of a phase is tagged with which table it scans. The posting
    // and main reads of a non-covered search overlap (a main read is launched once
    // enough candidate PKs have accumulated, while the posting read is still
    // running), so a single EPhase no longer identifies a read's table -- the tag
    // does. See HandleRead.
    enum class EReadKind {
        Level,
        Posting,
        Main,
    };

public:
    TKqpVectorSearchActor(
        NKikimrTxDataShard::TKqpVectorSearchSettings&& settings,
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
        , PostingCovers(Settings.GetPostingCovers())
        , HasPrefix(Settings.GetHasPrefix())
        , LogPrefix(TStringBuilder() << "VectorSearchActor, inputIndex: " << inputIndex << ", CA Id " << computeActorId)
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
        , MySpan(TWilsonKqp::VectorResolveActor, NWilson::TTraceId(traceId), "VectorSearchActor")
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

        OutputColumnTypeInfos.reserve(Settings.OutputColumnsSize());
        for (const auto& col : Settings.GetOutputColumns()) {
            OutputColumnTypeInfos.push_back(NScheme::TypeInfoFromProto(col.GetType(), col.GetTypeInfo()));
        }

        if (PostingCovers) {
            BuildCoveredPostingColumns();
        }
    }

    // Plan the covered-path posting read row: output columns occupy positions
    // 0..N-1 (distinct, matching VectorColumnIndex and the result row layout);
    // each PK column reuses its output position if it is also an output column,
    // else is appended (recorded in CoveredExtraPkIndices). A column id must not
    // be requested twice or the datashard read rejects it. StartPostingRead emits
    // the columns straight from the proto using this plan.
    void BuildCoveredPostingColumns() {
        YQL_ENSURE(Settings.PostingOutputColumnIdsSize() == Settings.OutputColumnsSize());
        THashMap<ui32, ui32> idToPos;
        for (size_t i = 0; i < Settings.PostingOutputColumnIdsSize(); ++i) {
            idToPos.emplace(Settings.GetPostingOutputColumnIds(i), i);
        }
        CoveredPkPositions.resize(Settings.MainTableKeyColumnsSize());
        ui32 nextPos = Settings.OutputColumnsSize();
        for (size_t j = 0; j < Settings.MainTableKeyColumnsSize(); ++j) {
            ui32 id = Settings.GetPostingTableKeyColumnIds(j + 1);
            auto [it, inserted] = idToPos.emplace(id, nextPos);
            if (inserted) {
                CoveredExtraPkIndices.push_back(j);
                ++nextPos;
            }
            CoveredPkPositions[j] = it->second;
        }
    }

    virtual ~TKqpVectorSearchActor() {
        if (Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();
            ClearResults();
        }
    }

    void Bootstrap() {
        CA_LOG_D("Start vector search actor");
        Become(&TKqpVectorSearchActor::StateFunc);
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
        StopAllReads();
        {
            auto guard = BindAllocator();
            Input.Clear();
            ClearResults();
        }
        MySpan.End();
        TActorBootstrapped<TKqpVectorSearchActor>::PassAway();
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


        Y_ENSURE(Settings.GetIndexLevels() >= 1);
        LevelsRemaining = Settings.GetIndexLevels();

        if (HasPrefix) {
            // Prefixed index: the level-traversal roots are the prefix groups' root
            // clusters. Roots carrying the PostingParentFlag have no level-table
            // subtree (single cluster) and go straight to the posting scan.
            if (RootParents.empty()) {
                // No prefix group matched the predicate: empty result.
                Phase = EPhase::Done;
                return;
            }
            for (TClusterId root : RootParents) {
                if (NTableIndex::NKMeans::HasPostingParentFlag(root)) {
                    DirectPostingParents.push_back(root);
                } else {
                    CurrentParents.push_back(root);
                }
            }
            // Keep LevelTop nearest clusters per prefix group, mirroring the legacy
            // levelTopTotal = levelTop * numPrefixGroups.
            LevelTop *= std::max<size_t>(1, RootParents.size());
        } else {
            // Begin level traversal from the single root cluster.
            CurrentParents = {0};
        }

        CA_LOG_D("StartSearch: targetBytes=" << TargetVector.size() << " indexLevels=" << Settings.GetIndexLevels()
            << " levelTop=" << LevelTop << " topK=" << TopK << " hasPrefix=" << HasPrefix
            << " levelRoots=" << CurrentParents.size() << " directPosting=" << DirectPostingParents.size());

        BeginTraversal();
    }

    void BeginTraversal() {
        if (CurrentParents.empty()) {
            // All roots are direct posting partitions: skip level traversal.
            StartPosting();
        } else {
            StartLevelRound();
        }
    }

    void FlushPendingMainKeys() {
        TVector<TString> batch;
        batch.swap(PendingMainKeys);
        LaunchMainReadFor(std::move(batch));
    }

    // Dispatch buffered posting PKs into a pipelined main read (see PollActiveReads).
    // Non-covered posting rows stream in across many drain cycles; each cycle that
    // leaves keys buffered dispatches them straight into a main read that overlaps the
    // still-running posting read. Every posting drain cycle's keys go out immediately,
    // so nothing is ever left stranded and no explicit final-tail flush is needed.
    void MaybeFlushPendingMainKeys() {
        if (PostingCovers || PendingMainKeys.empty()) {
            return;
        }
        FlushPendingMainKeys();
    }

    // Fetches the target-vector input rows. The input is a stream of single- or
    // two-field structs: element 0 is the target vector; for a prefixed index
    // element 1 is the prefix group's root __ydb_parent id. The whole input is
    // drained (it is materialized upstream), collecting all root ids. Returns:
    //   Ok     - target captured, ready to search;
    //   Yield  - input not ready yet, the caller should retry on the next poll;
    //   Finish - input ended without a usable target (search yields empty result).
    NUdf::EFetchStatus FetchTarget() {
        auto guard = BindAllocator();
        NUdf::TUnboxedValue row;
        for (;;) {
            auto status = Input.Fetch(row);
            if (status == NUdf::EFetchStatus::Yield) {
                return NUdf::EFetchStatus::Yield;
            }
            if (status == NUdf::EFetchStatus::Finish) {
                break;
            }
            if (TargetVector.empty()) {
                // First row carries the target vector (same value in every row).
                auto value = row.GetElement(0);
                if (!value.IsString() && !value.IsEmbedded()) {
                    return NUdf::EFetchStatus::Finish;
                }
                TargetVector = TString(value.AsStringRef());
            }
            if (HasPrefix) {
                RootParents.push_back(row.GetElement(1).Get<ui64>());
            }
        }
        return TargetVector.empty() ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Ok;
    }

    // Insert `item` into a bounded max-heap kept at <= `cap` (cmp orders it worst
    // at the front for cheap eviction). The caller guarantees the item belongs:
    // either the heap is below cap, or the item is nearer than the current worst.
    template <typename T, typename TCmp>
    static void PushBoundedMaxHeap(TVector<T>& heap, size_t cap, T item, TCmp cmp) {
        if (heap.size() < cap) {
            heap.push_back(std::move(item));
            std::push_heap(heap.begin(), heap.end(), cmp);
        } else {
            std::pop_heap(heap.begin(), heap.end(), cmp);
            heap.back() = std::move(item);
            std::push_heap(heap.begin(), heap.end(), cmp);
        }
    }

    // Insert a child into the round's bounded max-heap of the LevelTop nearest
    // (keyed by distance). Once full, a child no nearer than the current worst is
    // dropped, so the ranking structure never grows past LevelTop regardless of how
    // many shards contribute.
    void PushLevelCandidate(TClusterId id, double distance) {
        auto cmp = [](const auto& a, const auto& b) { return a.second < b.second; };
        if (LevelCandidates.size() < LevelTop || distance < LevelCandidates.front().second) {
            PushBoundedMaxHeap(LevelCandidates, LevelTop, std::make_pair(id, distance), cmp);
        }
    }

    void StartLevelRound() {
        LevelCandidates.clear();
        Phase = EPhase::Level;

        // Read all parents of this round in a single inner read: it fans the
        // per-parent ranges out across the level table's shards in parallel
        // instead of doing one sequential round-trip per parent. Parents already
        // in the shared level cache are served from it and excluded from the read.
        ReadingParents.clear();
        CachingLevelBatches.clear();
        TVector<TClusterId> toRead;
        toRead.reserve(CurrentParents.size());
        for (TClusterId parent : CurrentParents) {
            if (UseLevelCache) {
                auto cached = LevelsCache->Get(LevelTablePathId, SerializeParentKey(parent));
                if (cached && !cached->BatchRows.empty()) {
                    for (TConstArrayRef<TCell> row : cached->BatchRows) {
                        // Cached row layout: [id (Uint64), centroid (String)].
                        auto centroid = row[1].AsBuf();
                        if (!RankClusters->IsExpectedFormat(centroid)) {
                            RuntimeError("Invalid centroids in level table", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                            return;
                        }
                        PushLevelCandidate(row[0].AsValue<ui64>(),
                            RankClusters->CalcDistance(centroid, TargetVector));
                    }
                    continue;
                }
                // Cache miss: accumulate this parent's read rows to populate it.
                CachingLevelBatches.emplace(parent, TOwnedCellVecBatch());
            }
            ReadingParents.insert(parent);
            toRead.push_back(parent);
        }

        if (toRead.empty()) {
            OnLevelRoundDone();
        } else {
            StartLevelRead(toRead);
        }
    }

    void OnLevelRoundDone() {
        // Children were ranked incrementally as rows arrived: LevelCandidates already
        // holds (at most) the LevelTop nearest as a bounded heap. Hand their ids to
        // the next round -- parent order does not affect correctness, so no sort.
        CurrentParents.clear();
        CurrentParents.reserve(LevelCandidates.size());
        for (auto& [id, _] : LevelCandidates) {
            CurrentParents.push_back(id);
        }
        LevelCandidates.clear();

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
        // Candidate leaf clusters are now in CurrentParents. For a prefixed index,
        // also scan the prefix groups' direct posting partitions (roots that had no
        // level-table subtree), which bypassed level traversal. They are all read in
        // a single inner read, fanned out across the posting table's shards.
        TVector<TClusterId> toRead;
        toRead.reserve(CurrentParents.size() + DirectPostingParents.size());
        toRead.insert(toRead.end(), CurrentParents.begin(), CurrentParents.end());
        toRead.insert(toRead.end(), DirectPostingParents.begin(), DirectPostingParents.end());
        PendingMainKeys.clear();
        Phase = EPhase::Posting;
        if (toRead.empty()) {
            // No leaf clusters to scan: empty result (covered and non-covered alike).
            FinalizeResults();
        } else {
            StartPostingRead(toRead);
        }
    }

    // Populate the shared level cache with the children read for each cache-miss
    // parent of the round (keyed by parent id; empty results are not cached).
    void FlushLevelCache() {
        for (auto& [parent, batch] : CachingLevelBatches) {
            if (batch.empty()) {
                continue;
            }
            auto data = MakeIntrusive<TCachedLevelTableData>();
            data->BatchRows = std::move(batch);
            LevelsCache->Put(LevelTablePathId, SerializeParentKey(parent), data);
        }
        CachingLevelBatches.clear();
    }

    // Candidates already holds (at most) the TopK nearest as a bounded heap; sort
    // them ascending by distance and hand them off in order.
    void FinalizeResults() {
        auto guard = BindAllocator();
        std::sort(Candidates.begin(), Candidates.end(),
            [](const TCandidate& a, const TCandidate& b) { return a.Distance < b.Distance; });
        for (auto& c : Candidates) {
            ResultRows.push_back(std::move(c.Row));
        }
        Candidates.clear();
        Phase = EPhase::Done;
        NotifyCA();
    }

    // ---- Inner reads -------------------------------------------------------

    static TSerializedTableRange SingleColumnPointRange(ui64 value) {
        // Range matching rows whose first key column equals `value`:
        // (value-1, value] open on the left (null is -inf), missing suffix is +inf.
        auto from = value > 0 ? TCell::Make(value - 1) : TCell();
        auto to = TCell::Make(value);
        return TSerializedTableRange({&from, 1}, false, {&to, 1}, true);
    }

    static TString SerializeParentKey(TClusterId parent) {
        return TSerializedCellVec::Serialize({TCell::Make(parent)});
    }

    // Emit one point range per parent into the read's range list. A single inner
    // read over all of them fans out across the table's shards in parallel. The
    // read actor matches ranges against partitions in lockstep, so the ranges must
    // be globally sorted ascending by parent id.
    void AddParentRanges(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src, TVector<TClusterId>& parents) {
        std::sort(parents.begin(), parents.end());
        auto* ranges = src->MutableRanges();
        for (TClusterId parent : parents) {
            SingleColumnPointRange(parent).Serialize(*ranges->AddKeyRanges());
        }
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
        // Follower reads are a stale-RO optimization for the immutable index impl
        // tables and carry no locks (the datashard read actor asserts that follower
        // reads take no locks). If the surrounding transaction holds a lock (e.g. a
        // prefixed multi-phase query whose main-table read locks), fall back to a
        // normal lock-based read of the index tables instead of going to followers.
        const bool followerRead = immutableFollowerRead && !Settings.HasLockTxId();
        if (followerRead) {
            // No snapshot: the read actor only routes to followers when no snapshot
            // is set; the table is immutable, so inconsistent reads are safe.
            src->SetUseFollowers(true);
            src->SetAllowInconsistentReads(true);
        } else if (Snapshot.IsValid()) {
            src->MutableSnapshot()->SetTxId(Snapshot.TxId);
            src->MutableSnapshot()->SetStep(Snapshot.Step);
        }
        if (!followerRead && Settings.HasLockTxId()) {
            src->SetLockTxId(Settings.GetLockTxId());
            if (Settings.HasLockMode()) {
                src->SetLockMode(Settings.GetLockMode());
            }
            if (Settings.HasLockNodeId()) {
                src->SetLockNodeId(Settings.GetLockNodeId());
            }
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

    // Append the main-table PK column types (shared by the posting and main reads,
    // whose keys both carry the main PK columns).
    void AddMainKeyColumnTypes(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src) {
        for (const auto& pk : Settings.GetMainTableKeyColumns()) {
            AddColumnMetaKeyColumnType(src, pk);
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

    // Push top-K-by-distance ranking down into the datashard read so it returns only
    // the K nearest rows of this read instead of the full scan. The actor still merges
    // the per-read top-K across shards/clusters into the global top-K (the global K
    // nearest are necessarily among each read's own top-K). `column` is the position of
    // the embedding column within the read's requested columns list (the datashard
    // interprets VectorTopK.Column as that index, not as a column id), and `limit` is
    // how many nearest rows the read should keep.
    NKikimrKqp::TReadVectorTopK* SetVectorTopK(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src,
        ui32 column, ui32 limit)
    {
        auto* topK = src->MutableVectorTopK();
        topK->SetColumn(column);
        *topK->MutableSettings() = Settings.GetIndexSettings();
        topK->SetTargetVector(TargetVector);
        topK->SetLimit(limit);
        return topK;
    }

    void StartLevelRead(TVector<TClusterId>& parents) {
        TIntrusivePtr<NActors::TProtoArenaHolder> arena;
        auto* src = MakeSourceSettings(arena, Settings.GetLevelTable(), Settings.GetUseFollowers());
        AddParentRanges(src, parents);

        // Level table key is (parent, id), both Uint64.
        AddUint64KeyColumnType(src);
        AddUint64KeyColumnType(src);

        // Columns are read in this order, so the centroid (the embedding to rank on)
        // sits at index 2 -- the position the datashard's VectorTopK interprets.
        AddColumn(src, Settings.GetLevelTableParentColumnId(), NTableIndex::NKMeans::ParentColumn, NScheme::NTypeIds::Uint64, nullptr, true);
        AddColumn(src, Settings.GetLevelTableClusterColumnId(), NTableIndex::NKMeans::IdColumn, NScheme::NTypeIds::Uint64, nullptr, true);
        AddColumn(src, Settings.GetLevelTableCentroidColumnId(), NTableIndex::NKMeans::CentroidColumn, NScheme::NTypeIds::String, nullptr, true);

        if (!UseLevelCache) {
            // Push the per-round top-K down into the datashard so the read returns only
            // the LevelTop nearest children instead of every child of every parent
            // (which can be hundreds of large centroids per parent). The actor merges
            // the per-shard top-K and keeps LevelTop -- identical to ranking them all,
            // but transferring and ranking ~LevelTop rows instead of the full fan-out.
            // Skipped when the level cache is on: it stores all children to serve other
            // target vectors, so it must read them all. (VectorColumnIndex = 2, the
            // centroid's position in the columns added above.)
            SetVectorTopK(src, /* column */ 2, LevelTop);
        }

        LaunchRead(src, arena, EReadKind::Level);
    }

    void StartPostingRead(TVector<TClusterId>& parents) {
        TIntrusivePtr<NActors::TProtoArenaHolder> arena;
        auto* src = MakeSourceSettings(arena, Settings.GetPostingTable(), Settings.GetUseFollowers());
        AddParentRanges(src, parents);

        // Posting key is (__ydb_parent, <main PK columns>).
        AddUint64KeyColumnType(src);
        AddMainKeyColumnTypes(src);

        YQL_ENSURE(Settings.PostingTableKeyColumnIdsSize() == Settings.MainTableKeyColumnsSize() + 1);
        if (PostingCovers) {
            // Covered index: read output columns (with their posting-table ids)
            // plus any PK columns not already among them for per-row dedup; see
            // BuildCoveredPostingColumns.
            for (size_t i = 0; i < Settings.OutputColumnsSize(); ++i) {
                const auto& col = Settings.GetOutputColumns(i);
                const NKikimrProto::TTypeInfo* ti = col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr;
                AddColumn(src, Settings.GetPostingOutputColumnIds(i), col.GetName(), col.GetType(), ti, col.GetNotNull());
            }
            for (ui32 j : CoveredExtraPkIndices) {
                const auto& pk = Settings.GetMainTableKeyColumns(j);
                const NKikimrProto::TTypeInfo* ti = pk.HasTypeInfo() ? &pk.GetTypeInfo() : nullptr;
                AddColumn(src, Settings.GetPostingTableKeyColumnIds(j + 1), pk.GetName(), pk.GetType(), ti, pk.GetNotNull());
            }
            // Covered index ranks on the posting table: push top-K down so the
            // datashard returns only the nearest rows. A single read batches several
            // leaf clusters, and with overlap the same row can appear under multiple
            // clusters; dedup by PK inside the pushed-down top-K so duplicates don't
            // crowd out distinct nearest rows (the actor still dedups across shards).
            auto* topK = SetVectorTopK(src, Settings.GetVectorColumnIndex(), TopK);
            for (ui32 pos : CoveredPkPositions) {
                topK->AddDistinctColumns(pos);
            }
        } else {
            // Read just the PK columns (using posting table column ids) to feed
            // the main table read.
            for (size_t i = 0; i < Settings.MainTableKeyColumnsSize(); ++i) {
                const auto& pk = Settings.GetMainTableKeyColumns(i);
                const NKikimrProto::TTypeInfo* ti = pk.HasTypeInfo() ? &pk.GetTypeInfo() : nullptr;
                AddColumn(src, Settings.GetPostingTableKeyColumnIds(i + 1), pk.GetName(), pk.GetType(), ti, pk.GetNotNull());
            }
        }

        LaunchRead(src, arena, EReadKind::Posting);
    }

    // Launch a main-table read for one batch of candidate PKs. Non-covered searches
    // pipeline posting -> main: instead of waiting for the whole posting scan, PKs are
    // flushed to a main read as soon as a batch accumulates (see HandleRead), so the
    // main reads overlap the remaining posting reads and cross-node latency is hidden
    // instead of serialized behind a barrier. Each batch pushes its own top-K down;
    // the global K nearest are necessarily among the per-batch top-Ks, which the actor
    // merges in FinalizeResults.
    void LaunchMainReadFor(TVector<TString> keys) {
        if (keys.empty()) {
            return;
        }
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
        // Parse each key once up front, then sort the parsed cell vecs, instead of
        // re-deserializing both operands on every comparison.
        TVector<TSerializedCellVec> sortedKeys;
        sortedKeys.reserve(keys.size());
        for (auto& key : keys) {
            sortedKeys.emplace_back(std::move(key));
        }
        std::sort(sortedKeys.begin(), sortedKeys.end(), [keyTypes, keyCount](const TSerializedCellVec& a, const TSerializedCellVec& b) {
            return CompareTypedCellVectors(a.GetCells().data(), b.GetCells().data(), keyTypes, keyCount) < 0;
        });

        // Point lookups by primary key.
        auto* ranges = src->MutableRanges();
        for (const auto& key : sortedKeys) {
            ranges->AddKeyPoints(key.GetBuffer());
        }

        AddMainKeyColumnTypes(src);
        for (const auto& col : Settings.GetOutputColumns()) {
            const NKikimrProto::TTypeInfo* ti = col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr;
            AddColumn(src, col.GetId(), col.GetName(), col.GetType(), ti, col.GetNotNull());
        }

        // Non-covered index ranks on the main table: push top-K down so the
        // datashard returns only the K nearest of the gathered candidate PKs.
        SetVectorTopK(src, Settings.GetVectorColumnIndex(), TopK);

        LaunchRead(src, arena, EReadKind::Main);
    }

    // Launch one inner read for a phase over all its ranges. The standard read actor
    // resolves the table's partitioning itself (a single scheme-cache round-trip that
    // returns every partition of the range) and fans the ranges out across the shards
    // internally, so the search actor does not pre-resolve or pin shards. The read's
    // ranges/points must already be globally sorted, which the read actor requires to
    // match them against partitions in lockstep (see AddParentRanges / LaunchMainReadFor).
    void LaunchRead(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src,
        TIntrusivePtr<NActors::TProtoArenaHolder>& arena,
        EReadKind kind)
    {
        auto [readActorInput, readActor] = CreateKqpReadActor(src, arena, this->SelfId(),
            0, IngressStats.Level, TxId, TaskId, TypeEnv, HolderFactory, Alloc, TraceId, Counters);
        ActiveReads.push_back({readActorInput, kind});
        RegisterWithSameMailbox(readActor);
        // Kick off the freshly launched read: the first GetAsyncInputData poll starts
        // the inner read actor. Always called after (never during) the ActiveReads
        // iteration loop, so re-polling here is safe -- the new read has no data yet,
        // so the nested poll finds nothing finished and returns early.
        PollActiveReads();
    }

    // The inner read actors are not async inputs of the compute actor, so the
    // framework never collects their stats. Drain them here, before the inner
    // read actor is dropped: each inner read actor reports rows/bytes read from
    // datashards against its own table path (level / posting / main).
    void AccumulateInnerReadStats(NYql::NDq::IDqComputeActorAsyncInput* read) {
        NDqProto::TDqTaskStats innerStats;
        read->FillExtraStats(&innerStats, /* last */ true, /* mstats */ nullptr);
        for (const auto& table : innerStats.GetTables()) {
            auto& acc = ReadStatsByTable[table.GetTablePath()];
            acc.Rows += table.GetReadRows();
            acc.Bytes += table.GetReadBytes();
        }
    }

    // Tear down a finished inner read synchronously, like the compute actor tears down
    // its own sources/transforms (AsyncInput->PassAway()). The inner actor's PassAway
    // binds the allocator via the TaskRunner's TypeEnv, which the compute actor frees
    // in the same Terminate turn — a queued poison would run too late, against a freed
    // TypeEnv. Must be called without holding our own allocator guard.
    void StopRead(NYql::NDq::IDqComputeActorAsyncInput* read) {
        AccumulateInnerReadStats(read);
        read->PassAway();
    }

    void StopAllReads() {
        for (auto& ar : ActiveReads) {
            StopRead(ar.Read);
        }
        ActiveReads.clear();
    }

    ui32 CountActiveReads(EReadKind kind) const {
        ui32 n = 0;
        for (const auto& ar : ActiveReads) {
            if (ar.Kind == kind) {
                ++n;
            }
        }
        return n;
    }

    // Inner reads notify via TEvNewAsyncInputDataArrived (all with the same input
    // index); every wake-up re-polls the whole active set.
    void HandleRead(TEvNewAsyncInputDataArrived::TPtr) {
        PollActiveReads();
    }

    // Drain every active inner read, collecting their rows by table kind. Level rounds
    // are barriered (the next round needs all children ranked). Posting and main reads
    // of a non-covered search overlap: as the posting read streams rows in, once enough
    // candidate PKs have buffered they are dispatched to a main read (LaunchMainReadFor)
    // while the posting read keeps running, so cross-node latency is hidden instead of
    // serialized behind a posting->main barrier. The search finishes once no posting and
    // no main reads remain. Also called to kick a freshly launched read (see LaunchRead).
    void PollActiveReads() {
        if (ActiveReads.empty()) {
            return;
        }
        TVector<TActiveRead> finishedReads;
        {
            auto guard = BindAllocator();
            for (auto& ar : ActiveReads) {
                TMaybe<TInstant> watermark;
                ui64 freeSpace = 32 * 1024 * 1024;
                bool finished = false;
                NKikimr::NMiniKQL::TUnboxedValueBatch rows;
                ar.Read->GetAsyncInputData(rows, watermark, finished, freeSpace);
                rows.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    ProcessReadRow(value, ar.Kind);
                });
                rows.clear();
                if (finished) {
                    CollectLocks(ar.Read);
                    finishedReads.push_back(ar);
                }
                if (Failed) {
                    break;
                }
            }
        }
        // Drop finished reads from the active set and tear them down outside our
        // allocator guard (their PassAway binds the allocator itself). Most wake-ups
        // deliver partial data with nothing finished, so skip this entirely then --
        // both the membership scan of ActiveReads and the reads' teardown. Any reads
        // left unpolled after a Failed break stay active and are torn down in PassAway.
        if (!finishedReads.empty()) {
            ActiveReads.erase(
                std::remove_if(ActiveReads.begin(), ActiveReads.end(), [&](const TActiveRead& ar) {
                    return std::any_of(finishedReads.begin(), finishedReads.end(),
                        [&](const TActiveRead& f) { return f.Read == ar.Read; });
                }),
                ActiveReads.end());
            for (const auto& ar : finishedReads) {
                StopRead(ar.Read);
            }
        }
        if (Failed) {
            return;
        }
        // Pipeline posting -> main like the legacy StreamLookup fetch-then-dispatch
        // loop: dispatch buffered candidate PKs to a main read that overlaps the still
        // -running posting read, so cross-node latency is hidden instead of serialized
        // behind a posting->main barrier. A dispatch fires as the posting stream yields
        // batches. See MaybeFlushPendingMainKeys.
        MaybeFlushPendingMainKeys();

        if (finishedReads.empty() || Phase == EPhase::Done) {
            return;
        }
        if (Phase == EPhase::Level) {
            if (CountActiveReads(EReadKind::Level) == 0) {
                FlushLevelCache();
                OnLevelRoundDone();
            }
            return;
        }
        // Posting phase (and the main reads it pipelines into): done once neither
        // remains. Covered -> candidates are already built; non-covered -> candidates
        // came from the main reads (or there were none -> empty result).
        if (CountActiveReads(EReadKind::Posting) == 0 && CountActiveReads(EReadKind::Main) == 0) {
            CA_LOG_D("Posting/main done: candidateRows=" << Candidates.size() << " topK=" << TopK);
            FinalizeResults();
        }
    }

    void ProcessReadRow(NUdf::TUnboxedValue& value, EReadKind kind) {
        switch (kind) {
            case EReadKind::Level: {
                TClusterId parent = value.GetElement(0).Get<ui64>();
                if (!ReadingParents.contains(parent)) {
                    RuntimeError("Returned clusters for invalid parent", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                    return;
                }

                TClusterId child = value.GetElement(1).Get<ui64>();

                auto centroid = value.GetElement(2);
                auto centroidRef = centroid.AsStringRef();
                if (!RankClusters->IsExpectedFormat(centroidRef)) {
                    RuntimeError("Invalid centroids in level table", NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                    return;
                }

                // Compute the centroid<->target distance now, overlapping the shard
                // reads still in flight; ranking is deferred to the round barrier.
                PushLevelCandidate(child, RankClusters->CalcDistance(centroidRef, TargetVector));
                if (auto it = CachingLevelBatches.find(parent); it != CachingLevelBatches.end()) {
                    // Store [id (Uint64), centroid (String)]; Append copies the cells.
                    TCell cells[2] = {
                        TCell::Make(child),
                        TCell(centroidRef.Data(), centroidRef.Size()),
                    };
                    it->second.Append(TConstArrayRef<TCell>(cells, 2));
                }
                break;
            }
            case EReadKind::Posting: {
                // Dedup rows that appear in overlapping clusters by their PK. In
                // the covered path the PK columns sit at CoveredPkPositions and the
                // output columns occupy the first positions (so AddCandidate reads
                // them directly); otherwise the read row is just the PK columns.
                TString serialized = SerializePostingPk(value);
                if (!SeenKeys.insert(serialized).second) {
                    break;
                }
                if (PostingCovers) {
                    AddCandidate(value);
                } else {
                    // Buffer the PK for a pipelined main read (dispatched as the
                    // posting scan streams rows in; see MaybeFlushPendingMainKeys).
                    PendingMainKeys.push_back(std::move(serialized));
                }
                break;
            }
            case EReadKind::Main: {
                AddCandidate(value);
                break;
            }
        }
    }

    // Serialize the PK cell vec of a posting row for dedup. The PK columns are
    // read at CoveredPkPositions in the covered path, else at positions 0..N-1.
    TString SerializePostingPk(NUdf::TUnboxedValue& value) {
        const ui32 n = MainKeyTypeInfos.size();
        PkCellsScratch.resize(n);
        for (ui32 i = 0; i < n; ++i) {
            const ui32 pos = PostingCovers ? CoveredPkPositions[i] : i;
            PkCellsScratch[i] = NMiniKQL::MakeCell(MainKeyTypeInfos[i], value.GetElement(pos), TypeEnv, /* copy */ true);
        }
        return TSerializedCellVec::Serialize(PkCellsScratch);
    }

    // Build a candidate (output row + distance to the target) from a read row
    // whose first OutputColumns elements are the output columns in order.
    void AddCandidate(NUdf::TUnboxedValue& value) {
        auto embedding = value.GetElement(Settings.GetVectorColumnIndex());
        double distance = std::numeric_limits<double>::max();
        if (embedding.IsString() || embedding.IsEmbedded()) {
            distance = RankClusters->CalcDistance(TargetVector, embedding.AsStringRef());
        }
        // Bounded max-heap of the TopK nearest (worst distance at the top). Once full,
        // a row no nearer than the current worst is dropped before its output holder
        // is even materialized -- bounding peak memory at ~TopK rows regardless of
        // shard count (matters most for non-covered reads with large main rows).
        auto cmp = [](const TCandidate& a, const TCandidate& b) { return a.Distance < b.Distance; };
        if (Candidates.size() >= TopK && (TopK == 0 || distance >= Candidates.front().Distance)) {
            return;
        }
        // Build a fresh output struct holder in OutputColumns order.
        NUdf::TUnboxedValue* items = nullptr;
        auto row = HolderFactory.CreateDirectArrayHolder(Settings.OutputColumnsSize(), items);
        for (ui32 i = 0; i < Settings.OutputColumnsSize(); ++i) {
            items[i] = value.GetElement(i);
        }
        // The early-out above guarantees this row belongs (heap below TopK, or nearer
        // than the current worst), so hand it to the bounded-heap insert.
        PushBoundedMaxHeap(Candidates, TopK, TCandidate{distance, std::move(row)}, cmp);
    }

    void CollectLocks(NYql::NDq::IDqComputeActorAsyncInput* read) {
        auto extra = read->ExtraData();
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
            i64 rowSize = 0;
            for (ui32 i = 0; i < Settings.OutputColumnsSize(); ++i) {
                rowSize += NMiniKQL::GetUnboxedValueSize(row.GetElement(i), OutputColumnTypeInfos[i]).AllocatedBytes;
            }
            batch.emplace_back(std::move(row));
            ResultRows.pop_front();
            totalSize += rowSize;
            freeSpace -= rowSize;
        }
        return totalSize;
    }

    void ClearResults() {
        NKikimr::NMiniKQL::TUnboxedValueDeque empty;
        empty.swap(ResultRows);
        Candidates.clear();
    }

    void FillExtraStats(NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats*) override {
        if (!last) {
            return;
        }
        // Report rows/bytes actually read from each index impl table (level,
        // posting) and the main table, as accumulated from the inner read actors.
        for (const auto& [path, readStats] : ReadStatsByTable) {
            NDqProto::TDqTableStats* tableStats = nullptr;
            for (size_t i = 0; i < stats->TablesSize(); ++i) {
                if (stats->GetTables(i).GetTablePath() == path) {
                    tableStats = stats->MutableTables(i);
                    break;
                }
            }
            if (!tableStats) {
                tableStats = stats->AddTables();
                tableStats->SetTablePath(path);
            }
            tableStats->SetReadRows(tableStats->GetReadRows() + readStats.Rows);
            tableStats->SetReadBytes(tableStats->GetReadBytes() + readStats.Bytes);
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

    struct TTableReadStats {
        ui64 Rows = 0;
        ui64 Bytes = 0;
    };

    struct TActiveRead {
        NYql::NDq::IDqComputeActorAsyncInput* Read;
        EReadKind Kind;
    };

    // Parameters
    const NKikimrTxDataShard::TKqpVectorSearchSettings Settings;
    const ui32 TopK;
    ui32 LevelTop;
    const ui32 OverlapClusters;
    const double OverlapRatio;
    const bool PostingCovers;
    const bool HasPrefix;
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
    // Output column types, in OutputColumns order, used to estimate real row
    // sizes (incl. allocated payloads) when filling the reply batch.
    TVector<NScheme::TTypeInfo> OutputColumnTypeInfos;
    std::unique_ptr<NKikimr::NKMeans::IClusters> RankClusters;

    // Reusable scratch for serializing a posting row's PK during dedup.
    TVector<TCell> PkCellsScratch;

    // Covered-index posting read: the read row holds the output columns at
    // positions 0..N-1, then any PK columns not already among them (the indices
    // of those extra PKs into MainTableKeyColumns are CoveredExtraPkIndices).
    // CoveredPkPositions gives the read-row position of each PK column, used to
    // dedup rows that appear in overlapping clusters.
    TVector<ui32> CoveredPkPositions;
    TVector<ui32> CoveredExtraPkIndices;

    ui32 LevelsRemaining = 0;
    TVector<TClusterId> CurrentParents;
    // Prefixed index: root cluster ids collected from the transform input. Roots
    // with the PostingParentFlag are direct posting partitions (skip level
    // traversal); the rest are level-traversal roots.
    TVector<TClusterId> RootParents;
    TVector<TClusterId> DirectPostingParents;
    // Parents of the current round being read (level phase); a read row's parent
    // must be one of these.
    THashSet<TClusterId> ReadingParents;
    // The current level round's LevelTop nearest children as a bounded max-heap of
    // (id, distance-to-target) pairs (worst at the front). Distance is computed
    // incrementally as rows arrive (overlapping the shard reads) and the heap is
    // kept at <= LevelTop on the fly, so the round barrier just reads out ids.
    // Ranking reuses RankClusters' metric (CalcDistance is SetClusters-independent),
    // so no centroid bytes are copied into this accumulator. See PushLevelCandidate.
    TVector<std::pair<TClusterId, double>> LevelCandidates;

    // Shared, process-wide cache of immutable level-table rows.
    TIntrusivePtr<TVectorIndexLevelsCache> LevelsCache;
    TPathId LevelTablePathId;
    bool UseLevelCache = false;
    // Per-parent accumulators for the round's cache-miss parents; their read rows
    // are gathered here and inserted into the shared cache when the read finishes.
    THashMap<TClusterId, TOwnedCellVecBatch> CachingLevelBatches;

    // Non-covered posting rows produce candidate PKs (serialized cell vecs) buffered
    // here and streamed into pipelined main reads as the posting scan delivers them
    // (see MaybeFlushPendingMainKeys). SeenKeys dedups PKs that recur across overlapping
    // clusters, across all posting shards of the query.
    TVector<TString> PendingMainKeys;
    THashSet<TString> SeenKeys;

    // Bounded max-heap (by distance, worst at the front) of the TopK nearest result
    // rows, kept at <= TopK as rows are added; see AddCandidate / FinalizeResults.
    TVector<TCandidate> Candidates;
    NKikimr::NMiniKQL::TUnboxedValueDeque ResultRows;

    // Active inner read actors, each tagged with the table it scans. Level rounds are
    // barriered; posting and (non-covered) main reads coexist (pipelined). Each read
    // resolves its own table partitioning and fans out across shards internally.
    TVector<TActiveRead> ActiveReads;

    TVector<NKikimrDataEvents::TLock> Locks;

    // Rows/bytes read from each scanned table (level / posting / main), keyed by
    // table path, accumulated from the inner read actors for query stats.
    TMap<TString, TTableReadStats> ReadStatsByTable;

    NWilson::TSpan MySpan;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpVectorSearchActor(
    NKikimrTxDataShard::TKqpVectorSearchSettings&& settings,
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
    auto actor = new TKqpVectorSearchActor(std::move(settings), inputIndex, input, statsLevel, txId,
        taskId, computeActorId, typeEnv, holderFactory, alloc, traceId, counters, std::move(levelsCache));
    return {actor, actor};
}

void RegisterKqpVectorSearchActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters,
    TIntrusivePtr<TVectorIndexLevelsCache> levelsCache) {
    factory.RegisterInputTransform<NKikimrTxDataShard::TKqpVectorSearchSettings>(
        "VectorSearchInputTransformer", [counters, levelsCache](NKikimrTxDataShard::TKqpVectorSearchSettings&& settings,
            NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) -> std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> {
            return CreateKqpVectorSearchActor(std::move(settings),
                args.InputIndex, args.TransformInput, args.StatsLevel, args.TxId, args.TaskId, args.ComputeActorId,
                args.TypeEnv, args.HolderFactory, args.Alloc, args.TraceId, counters, levelsCache);
        });
}

} // namespace NKqp
} // namespace NKikimr
