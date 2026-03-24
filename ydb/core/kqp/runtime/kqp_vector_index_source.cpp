/**
 * TVectorIndexSource -- async input source actor for KQP compute actors.
 *
 * Implements the vector index (KMeans tree) search pipeline:
 *  1. Bootstrap: resolve table partitioning via SchemeCache.
 *  2. Level traversal: from root, read centroids, pick top-K closest, recurse.
 *  3. Posting lookup: read posting table for leaf parent IDs, compute distances.
 *  4. Main table fetch: look up full rows by PKs of top-K candidates.
 *  5. Stream results to the compute actor.
 */

#include "kqp_vector_index_source.h"

#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/base/kmeans_clusters.h>

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;

namespace {

// Describes one table involved in the search
struct TTableDesc {
    TTableId TableId;
    TString TablePath;
    IKqpGateway::TKqpSnapshot Snapshot;

    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<ui32> KeyColumnIds;
    TVector<NScheme::TTypeInfo> ResultColumnTypes;
    TVector<ui32> ResultColumnIds;

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> PartitionInfo;

    void SetPartitionInfo(const THolder<TKeyDesc>& keyDesc) {
        PartitionInfo = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>(
            keyDesc->GetPartitions());
    }

    // Build a point-lookup TEvRead for the given keys
    std::unique_ptr<TEvDataShard::TEvRead> BuildPointReadRequest(
        ui64 readId, const TVector<TSerializedCellVec>& keys) const
    {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(readId);
        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (auto colId : ResultColumnIds) {
            record.AddColumns(colId);
        }

        for (const auto& key : keys) {
            request->Keys.emplace_back(key);
        }

        if (Snapshot.IsValid()) {
            record.MutableSnapshot()->SetStep(Snapshot.Step);
            record.MutableSnapshot()->SetTxId(Snapshot.TxId);
        }

        auto defaultSettings = GetDefaultReadSettings()->Record;
        record.SetMaxRows(defaultSettings.GetMaxRows());
        record.SetMaxBytes(defaultSettings.GetMaxBytes());
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        return request;
    }

    // Build a prefix-range TEvRead (scan all rows sharing a key prefix)
    std::unique_ptr<TEvDataShard::TEvRead> BuildPrefixRangeReadRequest(
        ui64 readId, const TVector<TCell>& prefix) const
    {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(readId);
        record.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(TableId.SchemaVersion);

        for (auto colId : ResultColumnIds) {
            record.AddColumns(colId);
        }

        // Range: from prefix (inclusive) to prefix (inclusive) - captures all rows with this prefix
        request->Ranges.emplace_back(TSerializedTableRange(prefix, true, prefix, true));

        if (Snapshot.IsValid()) {
            record.MutableSnapshot()->SetStep(Snapshot.Step);
            record.MutableSnapshot()->SetTxId(Snapshot.TxId);
        }

        auto defaultSettings = GetDefaultReadSettings()->Record;
        record.SetMaxRows(defaultSettings.GetMaxRows());
        record.SetMaxBytes(defaultSettings.GetMaxBytes());
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        return request;
    }

    void AddResolvePartitioningRequest(std::unique_ptr<NSchemeCache::TSchemeCacheRequest>& request) const {
        auto keyColumnTypes = KeyColumnTypes;
        TVector<TCell> minusInf(keyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(
            TableId, range, TKeyDesc::ERowOperation::Read,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{}));
    }

    // Find shard ID for a point key
    ui64 FindShardForKey(const TConstArrayRef<TCell>& key) const {
        Y_ABORT_UNLESS(PartitionInfo);
        for (size_t i = 0; i < PartitionInfo->size(); ++i) {
            const auto& part = (*PartitionInfo)[i];
            const auto& endKey = part.Range->EndKeyPrefix.GetCells();
            if (endKey.empty()) {
                return part.ShardId;
            }
            int cmp = CompareTypedCellVectors(
                key.data(), endKey.data(), KeyColumnTypes.data(),
                std::min(key.size(), endKey.size()));
            if (cmp <= 0) {
                return part.ShardId;
            }
        }
        return PartitionInfo->back().ShardId;
    }
};

} // anonymous namespace

class TVectorIndexSource : public TActorBootstrapped<TVectorIndexSource>, public IDqComputeActorAsyncInput {
    using TBase = TActorBootstrapped<TVectorIndexSource>;

    // Pipeline phases
    enum class EPhase {
        Resolving,       // waiting for SchemeCache
        ReadingLevels,   // traversing KMeans tree levels
        ReadingPosting,  // reading posting table
        ReadingMain,     // fetching main table rows
        Done
    };

public:
    TVectorIndexSource(
        const NKikimrKqp::TKqpVectorIndexSourceSettings* settings,
        TIntrusivePtr<NActors::TProtoArenaHolder> arena,
        const NActors::TActorId& computeActorId,
        ui64 inputIndex,
        NYql::NDq::TCollectStatsLevel statsLevel,
        NYql::NDq::TTxId txId,
        ui64 taskId,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory,
        std::shared_ptr<NMiniKQL::TScopedAlloc> alloc,
        const NWilson::TTraceId& traceId,
        TIntrusivePtr<TKqpCounters> counters)
        : Settings(settings)
        , Arena(arena)
        , ComputeActorId(computeActorId)
        , InputIndex(inputIndex)
        , HolderFactory(holderFactory)
        , Alloc(alloc)
        , Counters(counters)
        , PipeCacheId(NKikimr::MakePipePerNodeCacheID(false))
    {
        Y_UNUSED(traceId);
        Y_UNUSED(statsLevel);
        Y_UNUSED(txId);
        Y_UNUSED(taskId);
        Y_UNUSED(typeEnv);
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "VectorIndexSource [" << SelfId() << "] ";
        Become(&TVectorIndexSource::StateWork);

        ParseSettings();
        ResolvePartitions();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(TEvDataShard::TEvReadResult, HandleReadResult);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleDeliveryProblem);
            default:
                break;
        }
    }

    // IDqComputeActorAsyncInput
    ui64 GetInputIndex() const override { return InputIndex; }

    const TDqAsyncStats& GetIngressStats() const override { return IngressStats; }

    i64 GetAsyncInputData(
        NKikimr::NMiniKQL::TUnboxedValueBatch& batch,
        TMaybe<TInstant>& watermark,
        bool& finished,
        i64 freeSpace) override
    {
        Y_UNUSED(watermark);

        auto guard = BindAllocator();
        i64 totalSize = 0;

        while (!ResultQueue.empty() && totalSize < freeSpace) {
            batch.push_back(std::move(ResultQueue.front()));
            ResultQueue.pop_front();
            totalSize += 128; // approximate row size
        }

        finished = (Phase == EPhase::Done) && ResultQueue.empty() && PendingReads == 0;
        return totalSize;
    }

    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    void LoadState(const TSourceState&) override {}

    void PassAway() override {
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    TGuard<NMiniKQL::TScopedAlloc> BindAllocator() {
        return Guard(*Alloc);
    }

    void NotifyCA() {
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode) {
        NYql::TIssues issues;
        issues.AddIssue(NYql::TIssue(message));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

    // ---- Settings parsing ----

    void ParseSettings() {
        // Snapshot
        IKqpGateway::TKqpSnapshot snapshot;
        if (Settings->HasSnapshot()) {
            snapshot = IKqpGateway::TKqpSnapshot(
                Settings->GetSnapshot().GetStep(), Settings->GetSnapshot().GetTxId());
        }

        // Target vector
        TargetVector = Settings->GetTargetVector();
        TopK = Settings->GetTopK();
        LevelTopSize = Settings->GetLevelTopSize();
        if (LevelTopSize == 0) LevelTopSize = 1;
        WithOverlap = Settings->GetWithOverlap();
        EmbeddingColumn = Settings->GetEmbeddingColumn();

        // KMeans tree settings
        const auto& kmeansSettings = Settings->GetKMeansTreeSettings();
        Levels = std::max<ui32>(1, kmeansSettings.levels());

        CA_LOG_D("ParseSettings: TargetVector size=" << TargetVector.size()
            << " TopK=" << TopK << " LevelTopSize=" << LevelTopSize
            << " Levels=" << Levels << " EmbeddingColumn=" << EmbeddingColumn);

        // Clusters objects are created on-demand during pipeline phases

        // Parse main table
        MainTable.TableId = TTableId(
            Settings->GetTable().GetOwnerId(),
            Settings->GetTable().GetTableId(),
            Settings->GetTable().GetVersion());
        MainTable.TablePath = Settings->GetTable().GetPath();
        MainTable.Snapshot = snapshot;
        for (const auto& col : Settings->GetKeyColumns()) {
            MainTable.KeyColumnTypes.push_back(
                NScheme::TTypeInfo(static_cast<NScheme::TTypeId>(col.GetTypeId())));
            MainTable.KeyColumnIds.push_back(col.GetId());
        }
        for (const auto& col : Settings->GetColumns()) {
            MainTable.ResultColumnTypes.push_back(
                NScheme::TTypeInfo(static_cast<NScheme::TTypeId>(col.GetTypeId())));
            MainTable.ResultColumnIds.push_back(col.GetId());
            ResultColumnNames.push_back(col.GetName());
            ResultColumnTypes.push_back(
                NScheme::TTypeInfo(static_cast<NScheme::TTypeId>(col.GetTypeId())));
        }

        // Parse index tables (0=level, 1=posting)
        Y_ABORT_UNLESS(Settings->IndexTablesSize() >= 2);
        for (size_t i = 0; i < static_cast<size_t>(Settings->IndexTablesSize()); ++i) {
            const auto& itbl = Settings->GetIndexTables(i);
            auto& desc = (i == 0) ? LevelTable : PostingTable;
            desc.TableId = TTableId(
                itbl.GetTable().GetOwnerId(),
                itbl.GetTable().GetTableId(),
                itbl.GetTable().GetVersion());
            desc.TablePath = itbl.GetTable().GetPath();
            desc.Snapshot = snapshot;
            for (const auto& col : itbl.GetKeyColumns()) {
                desc.KeyColumnTypes.push_back(
                    NScheme::TTypeInfo(static_cast<NScheme::TTypeId>(col.GetTypeId())));
                desc.KeyColumnIds.push_back(col.GetId());
            }
            // Request ALL columns (key + data) in TEvRead
            THashSet<ui32> addedIds;
            for (const auto& col : itbl.GetKeyColumns()) {
                desc.ResultColumnTypes.push_back(
                    NScheme::TTypeInfo(static_cast<NScheme::TTypeId>(col.GetTypeId())));
                desc.ResultColumnIds.push_back(col.GetId());
                addedIds.insert(col.GetId());
            }
            for (const auto& col : itbl.GetColumns()) {
                if (addedIds.insert(col.GetId()).second) {
                    desc.ResultColumnTypes.push_back(
                        NScheme::TTypeInfo(static_cast<NScheme::TTypeId>(col.GetTypeId())));
                    desc.ResultColumnIds.push_back(col.GetId());
                }
            }
        }
    }

    // ---- Phase 1: Resolve partitioning ----

    void ResolvePartitions() {
        Phase = EPhase::Resolving;

        auto request = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        MainTable.AddResolvePartitioningRequest(request);
        LevelTable.AddResolvePartitioningRequest(request);
        PostingTable.AddResolvePartitioningRequest(request);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.release()));
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            return RuntimeError("Failed to resolve table partitioning",
                NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        Y_ABORT_UNLESS(resultSet.size() == 3);
        MainTable.SetPartitionInfo(resultSet[0].KeyDescription);
        LevelTable.SetPartitionInfo(resultSet[1].KeyDescription);
        PostingTable.SetPartitionInfo(resultSet[2].KeyDescription);

        // Navigate the posting table to discover its full column schema
        // (needed to detect covered indexes at runtime)
        NavigatePostingTable();
    }

    void NavigatePostingTable() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = PostingTable.TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& resultSet = ev->Get()->Request->ResultSet;
        if (resultSet.size() == 1 && resultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            // Check if the posting table has the embedding column
            THashSet<ui32> existingIds(PostingTable.ResultColumnIds.begin(), PostingTable.ResultColumnIds.end());
            for (const auto& [colId, colInfo] : resultSet[0].Columns) {
                if (colInfo.Name == EmbeddingColumn && !existingIds.contains(colId)) {
                    // Covered index: add embedding column to posting table read
                    PostingTable.ResultColumnIds.push_back(colId);
                    PostingTable.ResultColumnTypes.push_back(colInfo.PType);
                    PostingTableHasEmbedding = true;
                    CA_LOG_D("Posting table has embedding column '" << EmbeddingColumn
                        << "' id=" << colId << " (covered index)");
                    break;
                } else if (colInfo.Name == EmbeddingColumn) {
                    // Column already in the read set (proto had it)
                    PostingTableHasEmbedding = true;
                    CA_LOG_D("Posting table embedding column already in read set");
                    break;
                }
            }
        }

        StartLevelTraversal();
    }

    // ---- Phase 2: Level traversal ----

    void StartLevelTraversal() {
        Phase = EPhase::ReadingLevels;
        CurrentLevel = 0;

        // Start from root: parent_id = 0
        CurrentParentIds.clear();
        CurrentParentIds.push_back(0);

        ReadNextLevel();
    }

    void ReadNextLevel() {
        CA_LOG_D("ReadNextLevel: level=" << CurrentLevel << " parentIds=" << CurrentParentIds.size()
            << " levelTable columns=" << LevelTable.ResultColumnIds.size());
        LevelCentroids.clear();
        PendingReads = 0;

        for (auto parentId : CurrentParentIds) {
            TVector<TCell> prefix;
            prefix.push_back(TCell::Make(parentId));

            ui64 readId = NextReadId++;
            auto request = LevelTable.BuildPrefixRangeReadRequest(readId, prefix);
            ui64 shardId = LevelTable.FindShardForKey(prefix);

            ReadInfos[readId] = {EPhase::ReadingLevels, shardId};
            PendingReads++;

            SendEvRead(shardId, std::move(request));
        }
    }

    void HandleLevelReadResult(TEvDataShard::TEvReadResult& msg) {
        CA_LOG_D("HandleLevelReadResult: rows=" << msg.GetRowsCount()
            << " finished=" << msg.Record.GetFinished());
        // Collect centroids from this batch
        // Cells are ordered by ResultColumnIds — identify columns by name from proto
        if (LevelIdCellIdx < 0) {
            // Build cell index map: first key columns, then data columns (matching ResultColumnIds order)
            const auto& levelProto = Settings->GetIndexTables(0);
            int idx = 0;
            THashSet<ui32> addedIds;
            for (int c = 0; c < levelProto.GetKeyColumns().size(); ++c) {
                const auto& name = levelProto.GetKeyColumns(c).GetName();
                if (name == NTableIndex::NKMeans::IdColumn) LevelIdCellIdx = idx;
                else if (name == NTableIndex::NKMeans::CentroidColumn) LevelCentroidCellIdx = idx;
                addedIds.insert(levelProto.GetKeyColumns(c).GetId());
                idx++;
            }
            for (int c = 0; c < levelProto.GetColumns().size(); ++c) {
                if (addedIds.insert(levelProto.GetColumns(c).GetId()).second) {
                    const auto& name = levelProto.GetColumns(c).GetName();
                    if (name == NTableIndex::NKMeans::IdColumn) LevelIdCellIdx = idx;
                    else if (name == NTableIndex::NKMeans::CentroidColumn) LevelCentroidCellIdx = idx;
                    idx++;
                }
            }
        }

        for (size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& cells = msg.GetCells(i);
            if (LevelIdCellIdx < 0 || LevelCentroidCellIdx < 0) continue;
            if (static_cast<size_t>(LevelIdCellIdx) >= cells.size() || static_cast<size_t>(LevelCentroidCellIdx) >= cells.size()) continue;

            ui64 id = cells[LevelIdCellIdx].IsNull() ? 0 : cells[LevelIdCellIdx].AsValue<ui64>();
            TString centroid = cells[LevelCentroidCellIdx].IsNull()
                ? TString()
                : TString(cells[LevelCentroidCellIdx].Data(), cells[LevelCentroidCellIdx].Size());

            if (!centroid.empty()) {
                LevelCentroids.push_back({id, centroid});
            }
        }

        // TEvReadResult with Finished=true means this read is done
        if (!msg.Record.GetFinished()) {
            return; // more data coming for this read
        }

        PendingReads--;

        if (PendingReads > 0) {
            return; // still waiting for other reads
        }

        // All reads for this level complete — select top centroids
        SelectTopCentroids();
    }

    void SelectTopCentroids() {
        if (LevelCentroids.empty()) {
            // No centroids found — finish with empty results
            Phase = EPhase::Done;
            NotifyCA();
            return;
        }

        CA_LOG_E("SelectTopCentroids: centroids=" << LevelCentroids.size()
            << " targetSize=" << TargetVector.size());
        for (size_t i = 0; i < LevelCentroids.size(); ++i) {
            CA_LOG_E("  centroid[" << i << "] id=" << LevelCentroids[i].first
                << " size=" << LevelCentroids[i].second.size()
                << " lastByte=" << (int)(ui8)LevelCentroids[i].second.back());
        }

        // Create IClusters with auto-detected type from centroid data (centroids may be float even for uint8 index)
        TString error;
        auto centroidSettings = Settings->GetVectorIndexSettings();
        // Auto-detect centroid format from actual data
        auto clusters = NKMeans::CreateClustersAutoDetect(centroidSettings, LevelCentroids[0].second, 0, error);
        CA_LOG_E("CreateClustersAutoDetect: error='" << error << "' ok=" << (clusters ? "yes" : "no"));
        if (!clusters) {
            CA_LOG_E("CreateClusters failed: " << error);
            Phase = EPhase::Done;
            NotifyCA();
            return;
        }

        TVector<TString> centroidBlobs;
        TVector<ui64> centroidIds;
        for (auto& [id, blob] : LevelCentroids) {
            centroidIds.push_back(id);
            centroidBlobs.push_back(std::move(blob));
        }

        if (!clusters->SetClusters(std::move(centroidBlobs))) {
            CA_LOG_D("SetClusters failed");
            Phase = EPhase::Done;
            NotifyCA();
            return;
        }

        size_t topN = std::min<size_t>(LevelTopSize, centroidIds.size());
        std::vector<std::pair<ui32, double>> results;
        clusters->FindClusters(TargetVector, results, topN, 0.0);

        CA_LOG_E("FindClusters returned " << results.size() << " results from " << centroidIds.size() << " centroids");
        for (const auto& [idx, dist] : results) {
            CA_LOG_E("  cluster idx=" << idx << " dist=" << dist
                << " id=" << (idx < centroidIds.size() ? centroidIds[idx] : 0));
        }

        CurrentParentIds.clear();
        for (const auto& [idx, dist] : results) {
            if (idx < centroidIds.size()) {
                CurrentParentIds.push_back(centroidIds[idx]);
            }
        }

        CurrentLevel++;
        if (CurrentLevel < Levels) {
            ReadNextLevel();
        } else {
            StartPostingLookup();
        }
    }

    // ---- Phase 3: Posting table lookup ----

    void StartPostingLookup() {
        Phase = EPhase::ReadingPosting;
        PostingCandidates.clear();
        PendingReads = 0;

        for (auto parentId : CurrentParentIds) {
            // For leaf-level clusters, the ID already has PostingParentFlag set
            // For non-leaf clusters, we need to set it
            ui64 postingParentId = NTableIndex::NKMeans::HasPostingParentFlag(parentId)
                ? parentId
                : NTableIndex::NKMeans::SetPostingParentFlag(parentId);

            TVector<TCell> prefix;
            prefix.push_back(TCell::Make(postingParentId));

            ui64 readId = NextReadId++;
            auto request = PostingTable.BuildPrefixRangeReadRequest(readId, prefix);
            ui64 shardId = PostingTable.FindShardForKey(prefix);

            ReadInfos[readId] = {EPhase::ReadingPosting, shardId};
            PendingReads++;

            SendEvRead(shardId, std::move(request));
        }

        if (PendingReads == 0) {
            Phase = EPhase::Done;
            NotifyCA();
        }
    }

    void HandlePostingReadResult(TEvDataShard::TEvReadResult& msg) {
        CA_LOG_D("HandlePostingReadResult: rows=" << msg.GetRowsCount()
            << " finished=" << msg.Record.GetFinished());

        // Build column name → cell index map on first call
        // Cells are ordered by ResultColumnIds: first key columns, then data columns, then dynamically added
        if (PostingColumnMap.empty()) {
            const auto& postingProto = Settings->GetIndexTables(1);
            int idx = 0;
            THashSet<ui32> addedIds;
            for (int c = 0; c < postingProto.GetKeyColumns().size(); ++c) {
                PostingColumnMap[postingProto.GetKeyColumns(c).GetName()] = idx++;
                addedIds.insert(postingProto.GetKeyColumns(c).GetId());
            }
            for (int c = 0; c < postingProto.GetColumns().size(); ++c) {
                if (addedIds.insert(postingProto.GetColumns(c).GetId()).second) {
                    PostingColumnMap[postingProto.GetColumns(c).GetName()] = idx++;
                }
            }
            // Account for dynamically added embedding column (covered index case)
            if (PostingTableHasEmbedding && PostingColumnMap.find(EmbeddingColumn) == PostingColumnMap.end()) {
                PostingColumnMap[EmbeddingColumn] = idx++;
            }
        }

        // Find main table PK column cell indices in posting table result
        if (PostingPKCellIndices.empty()) {
            for (const auto& keyCol : Settings->GetKeyColumns()) {
                auto it = PostingColumnMap.find(keyCol.GetName());
                if (it != PostingColumnMap.end()) {
                    PostingPKCellIndices.push_back(it->second);
                }
            }
        }

        // Find embedding column cell index
        if (PostingEmbeddingCellIdx < 0) {
            auto it = PostingColumnMap.find(EmbeddingColumn);
            if (it != PostingColumnMap.end()) {
                PostingEmbeddingCellIdx = it->second;
            }
        }

        for (size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& cells = msg.GetCells(i);

            // Extract main table PK columns from posting table result
            TVector<TCell> mainPK;
            for (int idx : PostingPKCellIndices) {
                if (static_cast<size_t>(idx) < cells.size()) {
                    mainPK.push_back(cells[idx]);
                }
            }

            // Compute distance using embedding column
            double distance = std::numeric_limits<double>::max();
            if (PostingEmbeddingCellIdx >= 0 && static_cast<size_t>(PostingEmbeddingCellIdx) < cells.size()) {
                const auto& embCell = cells[PostingEmbeddingCellIdx];
                if (!embCell.IsNull()) {
                    TStringBuf embedding(embCell.Data(), embCell.Size());
                    try {
                        if (!PostingClusters) {
                            TString err;
                            PostingClusters = NKMeans::CreateClustersAutoDetect(
                                Settings->GetVectorIndexSettings(), TargetVector, 0, err);
                        }
                        if (PostingClusters) {
                            distance = PostingClusters->CalcDistance(TargetVector, embedding);
                        }
                    } catch (const std::exception& e) {
                        CA_LOG_D("Posting CalcDistance failed: " << e.what());
                        continue;
                    }
                }
            }

            PostingCandidates.push_back({TSerializedCellVec(mainPK), distance});
        }

        if (!msg.Record.GetFinished()) {
            return;
        }

        PendingReads--;
        if (PendingReads > 0) {
            return;
        }

        // All posting reads done — select top-K candidates
        SelectTopKCandidates();
    }

    void SelectTopKCandidates() {
        CA_LOG_D("SelectTopKCandidates: candidates=" << PostingCandidates.size() << " topK=" << TopK);
        if (PostingCandidates.empty()) {
            Phase = EPhase::Done;
            NotifyCA();
            return;
        }

        // Deduplicate by PK (needed for overlap clusters where same row appears in multiple clusters)
        if (WithOverlap) {
            // Sort by PK first, then by distance within each PK
            THashMap<TString, size_t> bestByPK; // serialized PK -> index in PostingCandidates
            for (size_t i = 0; i < PostingCandidates.size(); ++i) {
                TString key = PostingCandidates[i].first.GetBuffer();
                auto it = bestByPK.find(key);
                if (it == bestByPK.end()) {
                    bestByPK[key] = i;
                } else if (PostingCandidates[i].second < PostingCandidates[it->second].second) {
                    it->second = i;
                }
            }
            TVector<std::pair<TSerializedCellVec, double>> deduplicated;
            deduplicated.reserve(bestByPK.size());
            for (const auto& [key, idx] : bestByPK) {
                deduplicated.push_back(std::move(PostingCandidates[idx]));
            }
            PostingCandidates = std::move(deduplicated);
        }

        bool hasDistances = false;
        for (const auto& [pk, dist] : PostingCandidates) {
            if (dist < std::numeric_limits<double>::max()) {
                hasDistances = true;
                break;
            }
        }

        if (hasDistances) {
            // Covered case: distances available, select top-K before main table fetch
            size_t topN = std::min<size_t>(TopK, PostingCandidates.size());
            std::partial_sort(PostingCandidates.begin(), PostingCandidates.begin() + topN,
                PostingCandidates.end(),
                [](const auto& a, const auto& b) { return a.second < b.second; });
            PostingCandidates.resize(topN);
        }
        // For uncovered case: fetch all candidates from main table, compute distances there

        StartMainTableFetch();
    }

    // ---- Phase 4: Main table fetch ----

    void StartMainTableFetch() {
        Phase = EPhase::ReadingMain;
        PendingReads = 0;
        MainRowsReceived = 0;
        ExpectedMainRows = PostingCandidates.size();

        CA_LOG_D("StartMainTableFetch: candidates=" << PostingCandidates.size());

        // Group candidates by shard for batch lookups
        THashMap<ui64, TVector<TSerializedCellVec>> keysByShard;
        for (const auto& [pk, dist] : PostingCandidates) {
            auto cells = pk.GetCells();
            ui64 shardId = MainTable.FindShardForKey(cells);
            keysByShard[shardId].push_back(pk);
        }

        for (auto& [shardId, keys] : keysByShard) {
            ui64 readId = NextReadId++;
            auto request = MainTable.BuildPointReadRequest(readId, keys);

            ReadInfos[readId] = {EPhase::ReadingMain, shardId};
            PendingReads++;

            SendEvRead(shardId, std::move(request));
        }

        if (PendingReads == 0) {
            Phase = EPhase::Done;
            NotifyCA();
        }
    }

    void HandleMainReadResult(TEvDataShard::TEvReadResult& msg) {
        CA_LOG_D("HandleMainReadResult: rows=" << msg.GetRowsCount()
            << " finished=" << msg.Record.GetFinished()
            << " resultCols=" << MainTable.ResultColumnIds.size());

        // Find embedding column index in main table result columns (for uncovered case)
        if (MainEmbeddingCellIdx < 0 && !EmbeddingColumn.empty()) {
            for (size_t c = 0; c < ResultColumnNames.size(); ++c) {
                if (ResultColumnNames[c] == EmbeddingColumn) {
                    MainEmbeddingCellIdx = static_cast<int>(c);
                    break;
                }
            }
            CA_LOG_D("MainEmbeddingCellIdx=" << MainEmbeddingCellIdx);
        }

        for (size_t i = 0; i < msg.GetRowsCount(); ++i) {
            const auto& cells = msg.GetCells(i);

            // Compute distance if embedding is in main table (uncovered case)
            double distance = std::numeric_limits<double>::max();
            if (MainEmbeddingCellIdx >= 0 && static_cast<size_t>(MainEmbeddingCellIdx) < cells.size()) {
                const auto& embCell = cells[MainEmbeddingCellIdx];
                if (!embCell.IsNull()) {
                    TStringBuf embedding(embCell.Data(), embCell.Size());
                    try {
                        if (!PostingClusters) {
                            TString err;
                            PostingClusters = NKMeans::CreateClustersAutoDetect(
                                Settings->GetVectorIndexSettings(), TargetVector, 0, err);
                        }
                        if (PostingClusters) {
                            distance = PostingClusters->CalcDistance(TargetVector, embedding);
                        }
                    } catch (const std::exception& e) {
                        CA_LOG_D("Main CalcDistance failed: " << e.what());
                    }
                }
            }

            MainResultRows.push_back({TSerializedCellVec(TSerializedCellVec::Serialize(cells)), distance});
            MainRowsReceived++;
        }

        if (!msg.Record.GetFinished()) {
            return;
        }

        PendingReads--;

        if (PendingReads > 0) {
            return;
        }

        FinalizeResults();
    }

    void FinalizeResults() {
        auto guard = BindAllocator();

        // Deduplicate by serialized cell content (for overlap clusters)
        if (WithOverlap && MainResultRows.size() > 1) {
            THashMap<TString, size_t> bestByKey;
            for (size_t i = 0; i < MainResultRows.size(); ++i) {
                TString key = MainResultRows[i].first.GetBuffer();
                auto it = bestByKey.find(key);
                if (it == bestByKey.end()) {
                    bestByKey[key] = i;
                } else if (MainResultRows[i].second < MainResultRows[it->second].second) {
                    it->second = i;
                }
            }
            TVector<std::pair<TSerializedCellVec, double>> deduplicated;
            deduplicated.reserve(bestByKey.size());
            for (const auto& [key, idx] : bestByKey) {
                deduplicated.push_back(std::move(MainResultRows[idx]));
            }
            MainResultRows = std::move(deduplicated);
        }

        // Sort by distance, take top-K
        if (MainResultRows.size() > TopK) {
            std::partial_sort(MainResultRows.begin(), MainResultRows.begin() + TopK,
                MainResultRows.end(),
                [](const auto& a, const auto& b) { return a.second < b.second; });
            MainResultRows.resize(TopK);
        }

        CA_LOG_D("FinalizeResults: rows=" << MainResultRows.size());

        for (auto& [serializedCells, dist] : MainResultRows) {
            auto cells = serializedCells.GetCells();
            NUdf::TUnboxedValue* rowItems = nullptr;
            auto row = HolderFactory.CreateDirectArrayHolder(
                ResultColumnTypes.size(), rowItems);

            for (size_t c = 0; c < ResultColumnTypes.size() && c < cells.size(); ++c) {
                rowItems[c] = NMiniKQL::GetCellValue(cells[c], ResultColumnTypes[c]);
            }

            ResultQueue.push_back(std::move(row));
        }

        Phase = EPhase::Done;
        NotifyCA();
    }

    // ---- Read dispatch ----

    void HandleReadResult(TEvDataShard::TEvReadResult::TPtr& ev) {
        auto readId = ev->Get()->Record.GetReadId();
        auto it = ReadInfos.find(readId);
        if (it == ReadInfos.end()) {
            return;
        }

        auto& msg = *ev->Get();
        if (msg.Record.HasStatus() && msg.Record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            return RuntimeError(
                TStringBuilder() << "Read error from shard, status: " << msg.Record.GetStatus().GetCode(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        auto phase = it->second.Phase;
        if (msg.Record.GetFinished()) {
            ReadInfos.erase(it);
        }

        switch (phase) {
            case EPhase::ReadingLevels:
                HandleLevelReadResult(msg);
                break;
            case EPhase::ReadingPosting:
                HandlePostingReadResult(msg);
                break;
            case EPhase::ReadingMain:
                HandleMainReadResult(msg);
                break;
            default:
                break;
        }
    }

    void HandleDeliveryProblem(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        RuntimeError(
            TStringBuilder() << "Delivery problem to shard " << ev->Get()->TabletId,
            NYql::NDqProto::StatusIds::UNAVAILABLE);
    }

    void SendEvRead(ui64 shardId, std::unique_ptr<TEvDataShard::TEvRead> request) {
        bool newPipe = PipesCreated.insert(shardId).second;
        Send(PipeCacheId,
            new TEvPipeCache::TEvForward(
                request.release(), shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = newPipe,
                    .Subscribe = newPipe}),
            IEventHandle::FlagTrackDelivery);
    }

    // ---- State ----

    struct TReadState {
        EPhase Phase;
        ui64 ShardId;
    };

    const NKikimrKqp::TKqpVectorIndexSourceSettings* Settings;
    TIntrusivePtr<NActors::TProtoArenaHolder> Arena;
    NActors::TActorId ComputeActorId;
    ui64 InputIndex;
    const NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<TKqpCounters> Counters;
    NActors::TActorId PipeCacheId;

    TString LogPrefix;
    TDqAsyncStats IngressStats;

    // Search parameters
    TString TargetVector;
    ui64 TopK = 10;
    ui32 LevelTopSize = 1;
    ui32 Levels = 1;
    bool WithOverlap = false;
    TString EmbeddingColumn;
    std::unique_ptr<NKMeans::IClusters> Clusters;
    std::unique_ptr<NKMeans::IClusters> PostingClusters;

    // Table descriptors
    TTableDesc MainTable;
    TTableDesc LevelTable;
    TTableDesc PostingTable;

    // Result schema
    TVector<TString> ResultColumnNames;
    TVector<NScheme::TTypeInfo> ResultColumnTypes;

    // Pipeline state
    EPhase Phase = EPhase::Resolving;
    ui32 CurrentLevel = 0;
    ui64 NextReadId = 1;
    ui64 PendingReads = 0;
    THashMap<ui64, TReadState> ReadInfos;
    THashSet<ui64> PipesCreated;

    // Level traversal
    TVector<ui64> CurrentParentIds;
    TVector<std::pair<ui64, TString>> LevelCentroids; // (id, centroid_blob)
    int LevelIdCellIdx = -1;
    int LevelCentroidCellIdx = -1;

    // Posting table column mapping
    THashMap<TString, int> PostingColumnMap;
    TVector<int> PostingPKCellIndices;
    int PostingEmbeddingCellIdx = -1;
    bool PostingTableHasEmbedding = false;

    // Posting candidates: (serialized_pk, distance)
    TVector<std::pair<TSerializedCellVec, double>> PostingCandidates;

    // Main table fetch
    size_t MainRowsReceived = 0;
    size_t ExpectedMainRows = 0;
    int MainEmbeddingCellIdx = -1;
    TVector<std::pair<TSerializedCellVec, double>> MainResultRows;

    // Results
    TDeque<NUdf::TUnboxedValue> ResultQueue;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpVectorIndexSource(
    const NKikimrKqp::TKqpVectorIndexSourceSettings* settings,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena,
    const NActors::TActorId& computeActorId,
    ui64 inputIndex,
    NYql::NDq::TCollectStatsLevel statsLevel,
    NYql::NDq::TTxId txId,
    ui64 taskId,
    const NMiniKQL::TTypeEnvironment& typeEnv,
    const NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NMiniKQL::TScopedAlloc> alloc,
    const NWilson::TTraceId& traceId,
    TIntrusivePtr<TKqpCounters> counters)
{
    auto* actor = new TVectorIndexSource(settings, arena, computeActorId, inputIndex, statsLevel, txId, taskId, typeEnv, holderFactory, alloc, traceId, counters);
    return std::make_pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>(actor, actor);
}

void RegisterKqpVectorIndexSource(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSource<NKikimrKqp::TKqpVectorIndexSourceSettings>(
        TString(NYql::KqpVectorIndexSourceName),
        [counters] (const NKikimrKqp::TKqpVectorIndexSourceSettings* settings, NYql::NDq::TDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateKqpVectorIndexSource(settings, args.Arena, args.ComputeActorId, args.InputIndex, args.StatsLevel,
                args.TxId, args.TaskId, args.TypeEnv, args.HolderFactory, args.Alloc, args.TraceId, counters);
        });
}

} // namespace NKikimr::NKqp
