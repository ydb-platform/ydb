#include "kqp_vector_search_actor.h"
#include "kqp_read_actor.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

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
        Resolve,      // resolving shard partitionings of the index/main tables
        Level,        // traversing the KMeans tree level table
        Posting,      // scanning posting table for candidate PKs (and, for a
                      // non-covered index, the pipelined main reads they feed)
        Done,         // results ready to be drained
    };

    // Each inner read of a phase is tagged with which table it scans. The posting
    // and main reads of a non-covered search overlap (main reads are launched as
    // posting shards finish), so a single EPhase no longer identifies a read's
    // table -- the tag does. See HandleRead.
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
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolvePartitioning);
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

        LevelsRemaining = std::max<ui32>(1, Settings.GetIndexLevels());

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

        // Pre-resolve the shard partitioning of the level/posting/main tables once,
        // concurrently. The maps are data-independent, so resolving them up front lets
        // every per-phase read target shards directly (ShardIdHint) and skip its own
        // scheme-cache resolve round-trip -- which otherwise sits serially on the
        // critical path before each phase. Traversal begins once all maps arrive.
        StartResolve();
    }

    void BeginTraversal() {
        if (CurrentParents.empty()) {
            // All roots are direct posting partitions: skip level traversal.
            StartPosting();
        } else {
            StartLevelRound();
        }
    }

    // ---- Partitioning pre-resolve -----------------------------------------

    void StartResolve() {
        Phase = EPhase::Resolve;
        PendingResolves = 0;
        const NScheme::TTypeInfo u64(NScheme::NTypeIds::Uint64);
        // Level table key is (parent, id), both Uint64.
        SendPartitioningResolve(Settings.GetLevelTable(), {u64, u64});
        // Posting key is (__ydb_parent, <main PK columns>).
        TVector<NScheme::TTypeInfo> postingKeyTypes;
        postingKeyTypes.reserve(1 + MainKeyTypeInfos.size());
        postingKeyTypes.push_back(u64);
        postingKeyTypes.insert(postingKeyTypes.end(), MainKeyTypeInfos.begin(), MainKeyTypeInfos.end());
        SendPartitioningResolve(Settings.GetPostingTable(), postingKeyTypes);
        if (!PostingCovers) {
            // Covered index ranks on the posting table and never reads the main table.
            SendPartitioningResolve(Settings.GetMainTable(), MainKeyTypeInfos);
        }
    }

    void SendPartitioningResolve(const NKikimrTxDataShard::TKqpTransaction::TTableMeta& table,
        const TVector<NScheme::TTypeInfo>& keyTypes)
    {
        const auto& t = table.GetTableId();
        TTableId tableId(t.GetOwnerId(), t.GetTableId(), table.GetSysViewInfo(), t.GetSchemaVersion());

        TVector<TCell> minusInf(keyTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Settings.GetDatabase();
        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(tableId, range, TKeyDesc::ERowOperation::Read,
            keyTypes, TVector<TKeyDesc::TColumnOp>{}));

        ++PendingResolves;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

    static bool SameTable(const TTableId& id, const NKikimrTxDataShard::TKqpTransaction::TTableMeta& table) {
        const auto& t = table.GetTableId();
        return id.PathId.OwnerId == t.GetOwnerId() && id.PathId.LocalPathId == t.GetTableId();
    }

    void HandleResolvePartitioning(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();
        if (!request->ResultSet.empty() && request->ErrorCount == 0) {
            const auto& entry = request->ResultSet[0];
            auto partitioning = entry.KeyDescription->Partitioning;
            const auto& id = entry.KeyDescription->TableId;
            // Leave a partitioning null on any mismatch/failure: the per-phase read then
            // falls back to a normal resolving read, preserving correctness.
            if (SameTable(id, Settings.GetLevelTable())) {
                LevelPartitioning = partitioning;
            } else if (SameTable(id, Settings.GetPostingTable())) {
                PostingPartitioning = partitioning;
            } else if (SameTable(id, Settings.GetMainTable())) {
                MainPartitioning = partitioning;
            }
        }
        if (--PendingResolves == 0 && !Failed) {
            BeginTraversal();
        }
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

    void StartLevelRound() {
        LevelChildren.clear();
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
                        LevelChildren[row[0].AsValue<ui64>()] = TString(row[1].AsBuf());
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

    // Keep the TopK nearest candidate rows by distance and hand them off.
    void FinalizeResults() {
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

        LaunchPhaseReads(src, arena, LevelPartitioning, EReadKind::Level);
    }

    void StartPostingRead(TVector<TClusterId>& parents) {
        TIntrusivePtr<NActors::TProtoArenaHolder> arena;
        auto* src = MakeSourceSettings(arena, Settings.GetPostingTable(), Settings.GetUseFollowers());
        AddParentRanges(src, parents);

        // Posting key is (__ydb_parent, <main PK columns>).
        AddUint64KeyColumnType(src);
        for (const auto& pk : Settings.GetMainTableKeyColumns()) {
            AddColumnMetaKeyColumnType(src, pk);
        }

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

        LaunchPhaseReads(src, arena, PostingPartitioning, EReadKind::Posting);
    }

    // Launch a main-table read for one batch of candidate PKs. Non-covered searches
    // pipeline posting -> main: instead of waiting for the whole posting scan, each
    // posting shard's PKs are looked up as that shard finishes (see HandleRead), so
    // the main reads overlap the remaining posting reads and cross-node latency is
    // hidden instead of serialized behind a barrier. Each batch pushes its own
    // top-K down; the global K nearest are necessarily among the per-batch top-Ks,
    // which the actor merges in FinalizeResults.
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

        for (const auto& pk : Settings.GetMainTableKeyColumns()) {
            AddColumnMetaKeyColumnType(src, pk);
        }
        for (const auto& col : Settings.GetOutputColumns()) {
            const NKikimrProto::TTypeInfo* ti = col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr;
            AddColumn(src, col.GetId(), col.GetName(), col.GetType(), ti, col.GetNotNull());
        }

        // Non-covered index ranks on the main table: push top-K down so the
        // datashard returns only the K nearest of the gathered candidate PKs.
        SetVectorTopK(src, Settings.GetVectorColumnIndex(), TopK);

        LaunchPhaseReads(src, arena, MainPartitioning, EReadKind::Main);
    }

    // Dispatch buffered candidate PKs into main reads (steps #2/#4): batched to
    // MainBatchKeys and capped at MaxInFlightMainReads concurrent reads, decoupled from
    // posting-shard boundaries. Once posting is done, flush the remainder even if below
    // the batch size. Recall-neutral. The re-entrancy guard stops the nested HandleRead
    // (kicked by LaunchPhaseReads) from re-dispatching mid-loop.
    void MaybeDispatchMainReads(bool postingDone) {
        if (PostingCovers || DispatchingMain) {
            return;
        }
        DispatchingMain = true;
        while (CountActiveReads(EReadKind::Main) < MaxInFlightMainReads && !PendingMainKeys.empty()
            && (PendingMainKeys.size() >= MainBatchKeys || postingDone))
        {
            const size_t take = std::min(PendingMainKeys.size(), MainBatchKeys);
            TVector<TString> batch;
            batch.reserve(take);
            for (size_t i = PendingMainKeys.size() - take; i < PendingMainKeys.size(); ++i) {
                batch.push_back(std::move(PendingMainKeys[i]));
            }
            PendingMainKeys.resize(PendingMainKeys.size() - take);
            LaunchMainReadFor(std::move(batch));
        }
        DispatchingMain = false;
    }

    // Launch the reads for one phase. With a pre-resolved partitioning, each read is
    // pinned to a single shard via ShardIdHint, so the inner read actor skips its own
    // scheme-cache resolve (one less serial round-trip per phase). Without it (resolve
    // failed), a single read is launched that resolves itself -- the legacy behaviour.
    void LaunchPhaseReads(NKikimrTxDataShard::TKqpReadRangesSourceSettings* src,
        TIntrusivePtr<NActors::TProtoArenaHolder>& arena,
        const TPartitioning::TCPtr& partitioning,
        EReadKind kind)
    {
        TVector<std::pair<NKikimrTxDataShard::TKqpReadRangesSourceSettings*, TIntrusivePtr<NActors::TProtoArenaHolder>>> reads;
        if (partitioning && partitioning->Size() > 1) {
            // Multi-shard table: fan out one read per shard, each pinned via ShardIdHint.
            SplitByShards(src, partitioning, reads);
        } else {
            // Single-shard table: hint the shard directly to skip the resolve.
            // No partitioning (resolve failed): leave the read to resolve itself.
            if (partitioning) {
                src->SetShardIdHint(partitioning->GetTablePartitioning()[0].ShardId);
            }
            reads.emplace_back(src, arena);
        }

        for (auto& [shardSrc, shardArena] : reads) {
            auto [readActorInput, readActor] = CreateKqpReadActor(shardSrc, shardArena, this->SelfId(),
                0, IngressStats.Level, TxId, TaskId, TypeEnv, HolderFactory, Alloc, TraceId, Counters);
            ActiveReads.push_back({readActorInput, kind});
            RegisterWithSameMailbox(readActor);
        }
        // Kick off the freshly launched reads: the first GetAsyncInputData poll starts
        // the inner read actor. Always called after (never during) the ActiveReads
        // iteration loop, so re-entering HandleRead here is safe -- the new reads have
        // no data yet, so the nested call finds nothing finished and returns early.
        HandleRead(nullptr);
    }

    // Partition the phase read's ranges/points across shards by the pre-resolved map,
    // cloning the read settings per shard with that shard's subset and a ShardIdHint.
    void SplitByShards(const NKikimrTxDataShard::TKqpReadRangesSourceSettings* src,
        const TPartitioning::TCPtr& partitioning,
        TVector<std::pair<NKikimrTxDataShard::TKqpReadRangesSourceSettings*, TIntrusivePtr<NActors::TProtoArenaHolder>>>& out)
    {
        // Key types describe the read's own ranges; take them straight from the source
        // settings (single source of truth) rather than tracking a parallel copy.
        TVector<NScheme::TTypeInfo> keyTypes;
        keyTypes.reserve(src->KeyColumnTypesSize());
        for (size_t i = 0; i < src->KeyColumnTypesSize(); ++i) {
            keyTypes.push_back(NScheme::TypeInfoFromProto(src->GetKeyColumnTypes(i), src->GetKeyColumnTypeInfos(i)));
        }
        const bool hasPoints = src->GetRanges().KeyPointsSize() > 0;

        // shardId -> indices into the source's KeyRanges/KeyPoints list.
        TMap<ui64, TVector<ui32>> byShard;
        if (hasPoints) {
            for (ui32 i = 0; i < src->GetRanges().KeyPointsSize(); ++i) {
                TSerializedCellVec point(src->GetRanges().GetKeyPoints(i));
                TTableRange r(point.GetCells(), true, point.GetCells(), true, /* point */ true);
                for (const auto& it : partitioning->GetIntersectionWithRange(keyTypes, r)) {
                    byShard[it.ShardId].push_back(i);
                }
            }
        } else {
            for (ui32 i = 0; i < src->GetRanges().KeyRangesSize(); ++i) {
                TSerializedTableRange range(src->GetRanges().GetKeyRanges(i));
                for (const auto& it : partitioning->GetIntersectionWithRange(keyTypes, range.ToTableRange())) {
                    byShard[it.ShardId].push_back(i);
                }
            }
        }

        // Copy the source settings once without ranges; each shard read reuses this
        // ranges-free template and appends only its own subset, so the (potentially
        // large) range list isn't deep-copied once per shard.
        NKikimrTxDataShard::TKqpReadRangesSourceSettings base;
        base.CopyFrom(*src);
        base.ClearRanges();
        for (const auto& [shardId, indices] : byShard) {
            auto shardArena = MakeIntrusive<NActors::TProtoArenaHolder>();
            auto* shardSrc = shardArena->Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
            shardSrc->CopyFrom(base);
            shardSrc->SetShardIdHint(shardId);
            auto* ranges = shardSrc->MutableRanges();
            for (ui32 i : indices) {
                if (hasPoints) {
                    ranges->AddKeyPoints(src->GetRanges().GetKeyPoints(i));
                } else {
                    *ranges->AddKeyRanges() = src->GetRanges().GetKeyRanges(i);
                }
            }
            out.emplace_back(shardSrc, shardArena);
        }
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

    // Drain every active inner read, collecting their rows by table kind. Inner reads
    // notify via TEvNewAsyncInputDataArrived (all with the same input index), so each
    // wake-up polls all of them. Level rounds are barriered (the next round needs all
    // children ranked). Posting and main reads of a non-covered search overlap: as
    // each posting shard finishes, the PKs it produced are dispatched to a main read
    // (LaunchMainReadFor) while the remaining posting shards are still reading, so
    // cross-node latency is hidden instead of serialized behind a posting->main
    // barrier. The search finishes once no posting and no main reads remain.
    void HandleRead(TEvNewAsyncInputDataArrived::TPtr) {
        if (ActiveReads.empty()) {
            return;
        }
        TVector<TActiveRead> finishedReads;
        {
            auto guard = BindAllocator();
            for (auto& ar : ActiveReads) {
                // Backpressure (#2): stop pulling posting once the candidate buffer is
                // full; resume when main reads drain it below the quota. Not polling the
                // inner read propagates backpressure down to the posting shards.
                if (ar.Kind == EReadKind::Posting && PendingMainKeys.size() >= MaxPendingKeys) {
                    continue;
                }
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
        // Drop finished reads from the active set; any left unpolled after a Failed
        // break stay active and are torn down later in PassAway.
        THashSet<NYql::NDq::IDqComputeActorAsyncInput*> finishedSet;
        for (const auto& ar : finishedReads) {
            finishedSet.insert(ar.Read);
        }
        ActiveReads.erase(
            std::remove_if(ActiveReads.begin(), ActiveReads.end(), [&](const TActiveRead& ar) {
                return finishedSet.contains(ar.Read);
            }),
            ActiveReads.end());
        // Tear down finished reads outside our allocator guard (their PassAway binds
        // the allocator itself).
        for (const auto& ar : finishedReads) {
            StopRead(ar.Read);
        }
        if (Failed) {
            return;
        }
        // Pipeline posting -> main (#2/#4): dispatch buffered candidate PKs into
        // bounded, in-flight-capped main reads as the buffer fills, flushing the
        // remainder once posting is done -- decoupled from posting-shard boundaries.
        // Covered searches build candidates from posting rows and never read main.
        MaybeDispatchMainReads(/* postingDone */ CountActiveReads(EReadKind::Posting) == 0);

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
        if (CountActiveReads(EReadKind::Posting) == 0 && CountActiveReads(EReadKind::Main) == 0
            && PendingMainKeys.empty()) {
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
                LevelChildren[child] = TString(centroidRef);
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
                    // Buffer the PK for the next pipelined main read (dispatched when
                    // a posting shard finishes; see HandleRead).
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
        // Build a fresh output struct holder in OutputColumns order.
        NUdf::TUnboxedValue* items = nullptr;
        auto row = HolderFactory.CreateDirectArrayHolder(Settings.OutputColumnsSize(), items);
        for (ui32 i = 0; i < Settings.OutputColumnsSize(); ++i) {
            items[i] = value.GetElement(i);
        }
        Candidates.push_back(TCandidate{distance, std::move(row)});
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
    THashMap<TClusterId, TString> LevelChildren;

    // Shared, process-wide cache of immutable level-table rows.
    TIntrusivePtr<TVectorIndexLevelsCache> LevelsCache;
    TPathId LevelTablePathId;
    bool UseLevelCache = false;
    // Per-parent accumulators for the round's cache-miss parents; their read rows
    // are gathered here and inserted into the shared cache when the read finishes.
    THashMap<TClusterId, TOwnedCellVecBatch> CachingLevelBatches;

    // Non-covered posting rows produce candidate PKs (serialized cell vecs) that are
    // buffered here and flushed into a pipelined main read when a posting shard
    // finishes (see HandleRead). SeenKeys dedups PKs that recur across overlapping
    // clusters, across all posting shards of the query.
    TVector<TString> PendingMainKeys;
    THashSet<TString> SeenKeys;

    // Posting->main dispatch policy (steps #2/#4): batch candidate PKs into bounded,
    // in-flight-capped main reads instead of one read per finished posting shard, and
    // backpressure posting when the buffer is full. Recall-neutral -- only changes when
    // and in what sizes main lookups are issued. Tune via the VectorSearch summary
    // (mainReads count, peak buffer, postingMainMs).
    static constexpr size_t MainBatchKeys = 2048;      // dispatch a main read at this many buffered keys
    static constexpr ui32 MaxInFlightMainReads = 4;    // cap concurrent main reads
    static constexpr size_t MaxPendingKeys = 16384;    // backpressure posting above this many buffered
    bool DispatchingMain = false;                      // re-entrancy guard for MaybeDispatchMainReads

    TVector<TCandidate> Candidates;
    NKikimr::NMiniKQL::TUnboxedValueDeque ResultRows;

    // Active inner read actors, each tagged with the table it scans. Level rounds are
    // barriered; posting and (non-covered) main reads coexist (pipelined).
    TVector<TActiveRead> ActiveReads;

    // Pre-resolved shard partitionings of the index/main tables, used to pin each
    // per-phase read to its shards via ShardIdHint and skip per-read resolves. A null
    // map (resolve failed) falls back to a normal resolving read for that table.
    TPartitioning::TCPtr LevelPartitioning;
    TPartitioning::TCPtr PostingPartitioning;
    TPartitioning::TCPtr MainPartitioning;
    ui32 PendingResolves = 0;

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
