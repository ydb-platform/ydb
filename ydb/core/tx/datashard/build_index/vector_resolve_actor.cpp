#include "vector_resolve_actor.h"

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard::NBuildIndex {

using namespace NTableIndex::NKMeans;

namespace {

constexpr ui32 DefaultOverlapClusters = 1;

class TVectorResolveActor : public NActors::TActorBootstrapped<TVectorResolveActor> {
    using TBase = NActors::TActorBootstrapped<TVectorResolveActor>;
    using TClusterId = NTableIndex::NKMeans::TClusterId;
    using TClusterList = TVector<std::pair<TClusterId, double>>;

    struct TRequest {
        ui64 RequestId = 0;
        NActors::TActorId Sender;
        TVector<TString> Vectors;
        TVector<TSerializedCellVec> Prefixes;
        TVector<TVector<TClusterId>> Result;

        // Prefix lookup state.
        TVector<TClusterId> Roots;
        TVector<TSerializedCellVec> DistinctPrefixes;
        TVector<TVector<ui32>> RowsByPrefix;
        TVector<TMaybe<TClusterId>> PrefixRoots;
        ui32 PrefixReadsRemaining = 0;
        ui32 PrefixSeqRemaining = 0;
        ui64 SeqEpoch = 0;
        TVector<TNewPrefix> NewPrefixes;

        // Tree traversal state.
        TVector<TClusterList> PrevClusters;
        TVector<TClusterList> NextClusters;
        TMap<TClusterId, TVector<size_t>> LevelClusters;
        // Centroids of ReadingChildClustersOf as cells referencing either
        // CachedLevel or LevelBatches; both keep the referenced data alive.
        TVector<TCell> CurCentroids;
        TVector<TClusterId> CurClusterIds;
        NKqp::TEvKqp::TCachedLevelTableDataPtr CachedLevel;
        TVector<TEvDataShard::TEvReadResult::TPtr> LevelBatches;
        ui32 ResolvedLevel = 0;
        bool LevelsFinished = false;
        bool ReadingChildClusters = false;
        bool HaveClusters = false;
        TClusterId ReadingChildClustersOf = 0;
    };

public:
    explicit TVectorResolveActor(TVectorResolveSettings settings)
        : Settings(std::move(settings))
        , IsPrefixed(Settings.IsPrefixed())
        , OverlapClusters(Settings.OverlapClusters > 0 ? Settings.OverlapClusters : DefaultOverlapClusters)
        , OverlapRatio(Settings.OverlapRatio)
        , LogPrefix(TStringBuilder() << "VectorResolveActor, level table: " << Settings.LevelTablePath)
    {
        TString error;
        Clusters = NKMeans::CreateClusters(Settings.IndexSettings, 0, error);
        if (!Clusters) {
            InitError = error;
        }
    }

    void Bootstrap() {
        // The cache is shared with KQP and provisioned lazily, so it is used
        // opportunistically: requests served before the pointer arrives (or
        // when KQP is disabled) read the level table directly.
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()),
            new NKqp::TEvKqp::TEvGetVectorIndexLevelsCacheRequest(),
            IEventHandle::FlagTrackDelivery);
        ReaderId = RegisterWithSameMailbox(CreateTableRangeReader());
        Become(&TVectorResolveActor::StateFunc);
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvVectorResolveRequest, HandleRequest);
            hFunc(NKqp::TEvKqp::TEvGetVectorIndexLevelsCacheResponse, HandleLevelsCacheResponse);
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(TEvReadTableRangeResponse, HandleReadResponse);
            hFunc(NSequenceProxy::TEvSequenceProxy::TEvNextValResult, HandleNextValResult);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                YDB_LOG_WARN("VectorResolveActor: unexpected event",
                    {"logPrefix", LogPrefix},
                    {"eventType", ev->GetTypeRewrite()});
        }
    }

    void PassAway() override {
        if (ReaderId) {
            Send(ReaderId, new TEvents::TEvPoison());
            ReaderId = {};
        }
        Current.reset();
        Queue.clear();
        TBase::PassAway();
    }

    void HandleRequest(TEvVectorResolveRequest::TPtr& ev) {
        TEvVectorResolveRequest* request = ev->Get();

        if (InitError) {
            ReplyFailure(ev->Sender, request->RequestId, InitError);
            return;
        }
        if (IsPrefixed && request->Prefixes.size() != request->Vectors.size()) {
            ReplyFailure(ev->Sender, request->RequestId,
                TStringBuilder() << "Vector resolve request has " << request->Vectors.size()
                    << " vectors but " << request->Prefixes.size() << " prefixes");
            return;
        }
        if (IsPrefixed) {
            for (const auto& prefix : request->Prefixes) {
                if (prefix.GetCells().size() != Settings.PrefixColumns.size()) {
                    ReplyFailure(ev->Sender, request->RequestId,
                        TStringBuilder() << "Prefix has " << prefix.GetCells().size()
                            << " cells, expected " << Settings.PrefixColumns.size());
                    return;
                }
            }
        }

        TRequest state;
        state.RequestId = request->RequestId;
        state.Sender = ev->Sender;
        state.Vectors = std::move(request->Vectors);
        state.Prefixes = std::move(request->Prefixes);
        Queue.push_back(std::move(state));

        StartNextRequest();
    }

    void HandleLevelsCacheResponse(NKqp::TEvKqp::TEvGetVectorIndexLevelsCacheResponse::TPtr& ev) {
        LevelsCache = std::move(ev->Get()->CachePtr);
        YDB_LOG_DEBUG("VectorResolveActor: got vector index levels cache",
            {"logPrefix", LogPrefix},
            {"maxBytes", LevelsCache ? LevelsCache->MaxBytes() : 0});
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType != NKqp::TEvKqp::TEvGetVectorIndexLevelsCacheRequest::EventType) {
            return;
        }
        YDB_LOG_DEBUG("VectorResolveActor: vector index levels cache is unavailable",
            {"logPrefix", LogPrefix});
    }

    void StartNextRequest() {
        if (Current || Queue.empty()) {
            return;
        }
        Current = std::make_unique<TRequest>(std::move(Queue.front()));
        Queue.pop_front();

        auto& request = *Current;
        request.Result.assign(request.Vectors.size(), {});
        request.Roots.assign(request.Vectors.size(), 0);
        request.SeqEpoch = NextSeqEpoch++;

        YDB_LOG_DEBUG("VectorResolveActor: start request",
            {"logPrefix", LogPrefix},
            {"requestId", request.RequestId},
            {"rows", request.Vectors.size()});

        if (request.Vectors.empty()) {
            FinishCurrent();
            return;
        }
        if (IsPrefixed) {
            StartPrefixLookup();
        } else {
            StartDescent();
        }
    }

    void StartPrefixLookup() {
        auto& request = *Current;

        THashMap<TString, ui32> prefixIndex;
        for (size_t row = 0; row < request.Prefixes.size(); ++row) {
            const TString& key = request.Prefixes[row].GetBuffer();
            auto [it, inserted] = prefixIndex.try_emplace(key, request.DistinctPrefixes.size());
            if (inserted) {
                request.DistinctPrefixes.push_back(request.Prefixes[row]);
                request.RowsByPrefix.emplace_back();
            }
            request.RowsByPrefix[it->second].push_back(row);
        }

        request.PrefixRoots.resize(request.DistinctPrefixes.size());
        request.PrefixReadsRemaining = request.DistinctPrefixes.size();

        YDB_LOG_DEBUG("VectorResolveActor: read prefixes",
            {"logPrefix", LogPrefix},
            {"requestId", request.RequestId},
            {"rows", request.Vectors.size()},
            {"prefixes", request.DistinctPrefixes.size()});

        if (request.DistinctPrefixes.empty()) {
            StartDescent();
            return;
        }

        for (ui32 index = 0; index < request.DistinctPrefixes.size(); ++index) {
            SendPrefixRead(request.DistinctPrefixes[index], index);
        }
    }

    void SendPrefixRead(const TSerializedCellVec& prefix, ui32 index) {
        const ui64 readerRequestId = NextReaderRequestId++;

        auto request = MakeHolder<TEvReadTableRangeRequest>();
        request->RequestId = readerRequestId;
        request->DatabaseName = Settings.DatabaseName;
        request->TablePath = Settings.PrefixTablePath;
        request->Columns = {IdColumn};
        request->FromKey = prefix;
        request->FromInclusive = true;

        TVector<TCell> to(prefix.GetCells().begin(), prefix.GetCells().end());
        to.push_back(TCell::Make(Max<ui64>()));
        request->ToKey = TSerializedCellVec(to);
        request->ToInclusive = true;

        PrefixReadRoutes[readerRequestId] = index;
        Send(ReaderId, request.Release());
    }

    void HandleReadResponse(TEvReadTableRangeResponse::TPtr& ev) {
        if (!Current) {
            // A response from a failed or already finished request.
            return;
        }

        TEvReadTableRangeResponse* response = ev->Get();

        if (response->RequestId == LevelReadRequestId) {
            LevelReadRequestId = 0;
            HandleLevelReadResponse(*response);
            return;
        }

        auto route = PrefixReadRoutes.find(response->RequestId);
        if (route != PrefixReadRoutes.end()) {
            const ui32 index = route->second;
            PrefixReadRoutes.erase(route);
            HandlePrefixReadResponse(index, *response);
        }
    }

    void HandlePrefixReadResponse(ui32 index, const TEvReadTableRangeResponse& response) {
        auto& request = *Current;
        Y_ENSURE(request.PrefixReadsRemaining > 0);
        --request.PrefixReadsRemaining;

        if (!response.Success) {
            FailCurrent(response.Issues);
            return;
        }

        // A batch with rows always holds at least one row, and the prefix range
        // matches at most one of them.
        if (!response.Batches.empty()) {
            const auto row = response.Batches.front()->Get()->GetCells(0);
            if (row.empty()) {
                FailCurrent("Empty row returned for a prefix lookup");
                return;
            }
            request.PrefixRoots[index] = static_cast<TClusterId>(row[0].AsValue<ui64>());
        }

        if (request.PrefixReadsRemaining > 0) {
            return;
        }

        TVector<ui32> missing;
        for (ui32 i = 0; i < request.PrefixRoots.size(); ++i) {
            if (!request.PrefixRoots[i]) {
                missing.push_back(i);
            }
        }

        if (missing.empty()) {
            StartDescent();
            return;
        }

        YDB_LOG_DEBUG("VectorResolveActor: generate IDs for new prefixes",
            {"logPrefix", LogPrefix},
            {"requestId", request.RequestId},
            {"prefixes", missing.size()});

        const TString sequencePath = GetSequencePath();
        request.PrefixSeqRemaining = missing.size();
        for (ui32 prefixIndex : missing) {
            const ui64 cookie = (request.SeqEpoch << 32) | prefixIndex;
            Send(NSequenceProxy::MakeSequenceProxyServiceID(),
                new NSequenceProxy::TEvSequenceProxy::TEvNextVal(Settings.DatabaseName, sequencePath),
                0, cookie);
        }
    }

    void HandleNextValResult(NSequenceProxy::TEvSequenceProxy::TEvNextValResult::TPtr& ev) {
        if (!Current) {
            return;
        }
        auto& request = *Current;

        const ui64 cookie = ev->Cookie;
        if ((cookie >> 32) != request.SeqEpoch) {
            return;
        }
        const ui32 index = static_cast<ui32>(cookie);
        if (index >= request.PrefixRoots.size()) {
            return;
        }

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues = ev->Get()->Issues;
            if (issues.Empty()) {
                issues.AddIssue(TStringBuilder() << "Failed to get the next value of sequence " << GetSequencePath());
            }
            FailCurrent(std::move(issues));
            return;
        }

        // The prefix table sequence is configured to produce values above 2^63,
        // i.e. with the posting parent flag set (see NKMeans::IdColumnSequence).
        request.PrefixRoots[index] = static_cast<TClusterId>(static_cast<ui64>(ev->Get()->Value));
        request.NewPrefixes.push_back({request.DistinctPrefixes[index], *request.PrefixRoots[index]});

        Y_ENSURE(request.PrefixSeqRemaining > 0);
        --request.PrefixSeqRemaining;
        if (request.PrefixSeqRemaining == 0) {
            StartDescent();
        }
    }

    void StartDescent() {
        auto& request = *Current;

        if (IsPrefixed) {
            for (ui32 index = 0; index < request.DistinctPrefixes.size(); ++index) {
                if (!request.PrefixRoots[index]) {
                    FailCurrent("Prefix was not resolved to a cluster");
                    return;
                }
                for (ui32 row : request.RowsByPrefix[index]) {
                    request.Roots[row] = *request.PrefixRoots[index];
                }
            }
        }

        request.PrevClusters.clear();
        request.NextClusters.clear();
        request.LevelClusters.clear();
        request.ResolvedLevel = 0;
        request.LevelsFinished = false;
        request.ReadingChildClusters = false;
        request.HaveClusters = false;

        ContinueDescent();
    }

    void ContinueDescent() {
        if (!Current) {
            return;
        }
        auto& request = *Current;

        while (!request.LevelsFinished) {
            if (request.LevelClusters.empty()) {
                BuildLevelClusters();
                if (request.LevelsFinished) {
                    break;
                }
            }

            while (!request.LevelClusters.empty()) {
                auto cluster = request.LevelClusters.begin();
                const TClusterId parent = cluster->first;

                if (!request.HaveClusters) {
                    if (!LoadChildClustersFromCache(request, parent)) {
                        ReadChildClusters(parent);
                        return;
                    }
                }

                auto rows = std::move(cluster->second);
                request.LevelClusters.erase(cluster);

                for (size_t row : rows) {
                    const TStringBuf embedding(request.Vectors[row]);
                    TmpClusters.clear();
                    Clusters->FindClusters(request.CurCentroids, embedding, TmpClusters, OverlapClusters, OverlapRatio);
                    for (const auto& [pos, distance] : TmpClusters) {
                        request.NextClusters[row].push_back(std::make_pair(request.CurClusterIds[pos], distance));
                    }
                }

                ClearChildClusters(request);
            }

            if (OverlapClusters > 1) {
                for (auto& rowClusters : request.NextClusters) {
                    NKMeans::FilterOverlapRows(rowClusters, OverlapClusters, OverlapRatio);
                }
            }

            request.PrevClusters = std::move(request.NextClusters);
            ++request.ResolvedLevel;
        }

        FinishCurrent();
    }

    // Level tables are immutable, so the cache is keyed by the level table
    // PathId and the parent cluster id only; the snapshot does not matter.
    bool UseLevelCache() const {
        return LevelsCache && LevelTablePathId && LevelsCache->MaxBytes() > 0;
    }

    static TString SerializeParentKey(TClusterId parent) {
        return TSerializedCellVec::Serialize({TCell::Make(parent)});
    }

    // Drops the child cluster list together with the data it references.
    static void ClearChildClusters(TRequest& request) {
        request.HaveClusters = false;
        request.CurCentroids.clear();
        request.CurClusterIds.clear();
        request.CachedLevel.Reset();
        request.LevelBatches.clear();
    }

    // Fills the current cluster list from the shared level cache. Returns false
    // on a miss (empty results are not cached) so the caller reads the table.
    bool LoadChildClustersFromCache(TRequest& request, TClusterId parent) {
        if (!UseLevelCache()) {
            return false;
        }

        auto data = LevelsCache->Get(*LevelTablePathId, SerializeParentKey(parent));
        if (!data || data->BatchRows.empty()) {
            return false;
        }

        request.CurCentroids.clear();
        request.CurClusterIds.clear();
        request.CachedLevel = std::move(data);

        // Cells reference the cached batch rows, which CachedLevel keeps alive.
        const auto& rows = request.CachedLevel->BatchRows;
        request.CurCentroids.reserve(rows.Size());
        request.CurClusterIds.reserve(rows.Size());
        for (TConstArrayRef<TCell> row : rows) {
            // Cached row layout: [id (Uint64), centroid (String)], the same as
            // written by the KQP vector search actor.
            request.CurClusterIds.push_back(row[0].AsValue<ui64>());
            request.CurCentroids.push_back(row[1]);
        }
        request.HaveClusters = true;

        return true;
    }

    void BuildLevelClusters() {
        auto& request = *Current;

        request.NextClusters.assign(request.Vectors.size(), {});
        request.LevelClusters.clear();
        ClearChildClusters(request);

        if (request.ResolvedLevel == 0) {
            if (IsPrefixed) {
                for (size_t row = 0; row < request.Vectors.size(); ++row) {
                    const TClusterId root = request.Roots[row];
                    if (NTableIndex::NKMeans::HasPostingParentFlag(root)) {
                        // The prefix has a single cluster, it is the posting parent directly.
                        request.NextClusters[row].push_back(std::make_pair(root, 0.0));
                    } else {
                        request.LevelClusters[root].push_back(row);
                    }
                }
            } else {
                for (size_t row = 0; row < request.Vectors.size(); ++row) {
                    request.LevelClusters[0].push_back(row);
                }
            }
        } else {
            for (size_t row = 0; row < request.Vectors.size(); ++row) {
                for (const auto& cluster : request.PrevClusters[row]) {
                    if (NTableIndex::NKMeans::HasPostingParentFlag(cluster.first)) {
                        request.NextClusters[row].push_back(cluster);
                    } else {
                        request.LevelClusters[cluster.first].push_back(row);
                    }
                }
            }
        }

        if (request.LevelClusters.empty()) {
            // All clusters are leaf (posting) clusters.
            request.PrevClusters = std::move(request.NextClusters);
            request.LevelsFinished = true;
        }
    }

    void ReadChildClusters(TClusterId parent) {
        if (Current->ReadingChildClusters) {
            return;
        }
        Current->ReadingChildClusters = true;
        Current->ReadingChildClustersOf = parent;

        TCell from;
        if (parent > 0) {
            from = TCell::Make(parent - 1); // null is -infinity
        }
        const TCell to = TCell::Make(parent); // missing key suffix is +infinity

        const ui64 readerRequestId = NextReaderRequestId++;
        LevelReadRequestId = readerRequestId;

        auto request = MakeHolder<TEvReadTableRangeRequest>();
        request->RequestId = readerRequestId;
        request->DatabaseName = Settings.DatabaseName;
        request->TablePath = Settings.LevelTablePath;
        request->Columns = {ParentColumn, IdColumn, CentroidColumn};
        request->FromKey = TSerializedCellVec({&from, 1});
        request->FromInclusive = false;
        request->ToKey = TSerializedCellVec({&to, 1});
        request->ToInclusive = true;
        request->Snapshot = Settings.Snapshot;

        Send(ReaderId, request.Release());
    }

    void HandleLevelReadResponse(TEvReadTableRangeResponse& response) {
        auto& request = *Current;
        request.ReadingChildClusters = false;

        if (!response.Success) {
            FailCurrent(response.Issues);
            return;
        }

        if (response.PathId) {
            LevelTablePathId = response.PathId;
        }

        request.CurCentroids.clear();
        request.CurClusterIds.clear();

        size_t rowsCount = 0;
        for (const auto& batch : response.Batches) {
            rowsCount += batch->Get()->GetRowsCount();
        }
        request.CurCentroids.reserve(rowsCount);
        request.CurClusterIds.reserve(rowsCount);

        NKqp::TEvKqp::TCachedLevelTableDataPtr cacheData;
        if (UseLevelCache()) {
            cacheData = MakeIntrusive<NKqp::TEvKqp::TCachedLevelTableData>();
        }

        TString error;
        for (const auto& batch : response.Batches) {
            if (!error.empty()) {
                break;
            }
            const TEvDataShard::TEvReadResult* result = batch->Get();
            const size_t rows = result->GetRowsCount();
            for (size_t i = 0; i < rows; ++i) {
                const auto row = result->GetCells(i);
                if (row.size() < 3) {
                    error = "Level table row has less than 3 columns";
                    break;
                }
                const TClusterId parent = row[0].AsValue<ui64>();
                if (parent != request.ReadingChildClustersOf) {
                    error = TStringBuilder() << "Returned clusters for invalid parent " << parent
                        << ", expected " << request.ReadingChildClustersOf;
                    break;
                }
                request.CurClusterIds.push_back(row[1].AsValue<ui64>());
                // The cell references the read result rows, which LevelBatches
                // keeps alive below.
                request.CurCentroids.push_back(row[2]);
                if (cacheData) {
                    cacheData->BatchRows.Append(row.Slice(1, 2));
                }
            }
        }

        if (!error.empty()) {
            FailCurrent(error);
            return;
        }

        if (cacheData && !cacheData->BatchRows.empty()) {
            LevelsCache->Put(*LevelTablePathId, SerializeParentKey(request.ReadingChildClustersOf), cacheData);
        }

        request.LevelBatches = std::move(response.Batches);
        request.HaveClusters = true;

        ContinueDescent();
    }

    void FinishCurrent() {
        auto request = std::move(Current);
        Current.reset();

        if (request->PrevClusters.size() != request->Vectors.size()) {
            request->PrevClusters = std::move(request->NextClusters);
            request->PrevClusters.resize(request->Vectors.size());
        }

        auto response = MakeHolder<TEvVectorResolveResponse>(request->RequestId);
        response->Success = true;
        response->NewPrefixes = std::move(request->NewPrefixes);
        response->Clusters.resize(request->Vectors.size());
        for (size_t row = 0; row < request->Vectors.size(); ++row) {
            auto& clusters = response->Clusters[row];
            clusters.reserve(request->PrevClusters[row].size());
            for (const auto& cluster : request->PrevClusters[row]) {
                clusters.push_back(cluster.first);
            }
        }

        YDB_LOG_DEBUG("VectorResolveActor: request finished",
            {"logPrefix", LogPrefix},
            {"requestId", request->RequestId},
            {"rows", request->Vectors.size()});

        Send(request->Sender, response.Release());
        StartNextRequest();
    }

    void FailCurrent(const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        FailCurrent(std::move(issues));
    }

    void FailCurrent(NYql::TIssues issues) {
        auto request = std::move(Current);
        Current.reset();
        PrefixReadRoutes.clear();
        LevelReadRequestId = 0;

        YDB_LOG_WARN("VectorResolveActor: request failed",
            {"logPrefix", LogPrefix},
            {"requestId", request ? request->RequestId : 0},
            {"error", issues.ToOneLineString()});

        auto response = MakeHolder<TEvVectorResolveResponse>(request ? request->RequestId : 0);
        response->Success = false;
        response->Issues = std::move(issues);
        if (request) {
            Send(request->Sender, response.Release());
        }

        StartNextRequest();
    }

    void ReplyFailure(const NActors::TActorId& sender, ui64 requestId, const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        ReplyFailure(sender, requestId, std::move(issues));
    }

    void ReplyFailure(const NActors::TActorId& sender, ui64 requestId, NYql::TIssues issues) {
        auto response = MakeHolder<TEvVectorResolveResponse>(requestId);
        response->Success = false;
        response->Issues = std::move(issues);
        Send(sender, response.Release());
    }

    TString GetSequencePath() const {
        return TStringBuilder() << Settings.PrefixTablePath << "/" << IdColumnSequence;
    }

private:
    const TVectorResolveSettings Settings;
    const bool IsPrefixed;
    const ui32 OverlapClusters;
    const double OverlapRatio;
    const TString LogPrefix;

    std::unique_ptr<NKMeans::IClusters> Clusters;
    TString InitError;

    NKqp::TEvKqp::IVectorIndexLevelsCachePtr LevelsCache;
    // Learned from the first level table read; the shared cache key.
    TMaybe<TPathId> LevelTablePathId;

    NActors::TActorId ReaderId;
    TDeque<TRequest> Queue;
    std::unique_ptr<TRequest> Current;

    ui64 NextReaderRequestId = 1;
    ui64 NextSeqEpoch = 1;
    THashMap<ui64, ui32> PrefixReadRoutes;
    ui64 LevelReadRequestId = 0;
    std::vector<std::pair<ui32, double>> TmpClusters;
};

} // namespace

NActors::IActor* CreateVectorResolveActor(TVectorResolveSettings settings) {
    return new TVectorResolveActor(std::move(settings));
}

} // namespace NKikimr::NDataShard::NBuildIndex
