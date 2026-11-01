#include "table_range_reader.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/range_ops.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>

#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard::NBuildIndex {

ui64 gTableRangeReaderMaxRows = UINT64_MAX;

namespace {

// How many times a single shard read is retried from its last known good key
// before the whole request fails.
constexpr ui32 MaxShardRetries = 3;

bool IsRetriableStatus(Ydb::StatusIds::StatusCode status) {
    switch (status) {
        case Ydb::StatusIds::OVERLOADED:
        case Ydb::StatusIds::UNAVAILABLE:
        case Ydb::StatusIds::NOT_FOUND:
        case Ydb::StatusIds::TIMEOUT:
            return true;
        default:
            return false;
    }
}

// Reads ranges of arbitrary tables through TEvRead.
//
// One actor serves many requests. Per table it keeps a scheme cache navigation
// result (column names/tags and key types) and the partitioning resolved once
// with a full range, so repeated requests to the same table do not re-resolve.
// Every request is answered with a single TEvReadTableRangeResponse. In-flight reads
// are cancelled when a request fails or when the actor is stopped.
class TTableRangeReader : public NActors::TActorBootstrapped<TTableRangeReader> {
    using TBase = NActors::TActorBootstrapped<TTableRangeReader>;

    struct TColumnInfo {
        ui32 Id = 0;
        NScheme::TTypeInfo Type;
        i32 KeyOrder = -1;
        bool NotNull = false;
    };

    struct TTableScheme {
        enum class EState {
            Unknown,      // navigation required
            Navigating,
            Resolving,    // partitioning required
            Ready,
        };

        EState State = EState::Unknown;
        TString DatabaseName;
        TTableId TableId;
        TVector<NScheme::TTypeInfo> KeyTypes;
        THashMap<TString, TColumnInfo> Columns;
        std::shared_ptr<const TPartitioning> Partitioning;
    };

    struct TShardRead {
        ui64 TabletId = 0;
        ui64 ReadId = 0;
        TSerializedTableRange OriginalRange;
        // Resume point from the last continuation token; when set, a retry
        // continues after it instead of re-reading the whole range.
        TSerializedCellVec ResumeKey;
        bool HasResumeKey = false;
        bool Finished = false;
        ui64 LastSeqNo = 0;
        ui32 Retries = 0;
        TVector<TEvDataShard::TEvReadResult::TPtr> Batches;
    };

    struct TRequestState {
        ui64 InternalId = 0;
        ui64 RequestId = 0;
        TActorId Sender;
        TString TablePath;
        TMaybe<TRowVersion> Snapshot;
        TVector<ui32> ColumnTags;
        TSerializedTableRange Range;
        TVector<TShardRead> Shards;
        size_t ShardsRemaining = 0;
    };

    struct TReadIdRoute {
        ui64 InternalRequestId = 0;
        ui32 ShardIndex = 0;
    };

public:
    TTableRangeReader() = default;

    void Bootstrap() {
        PipeCacheId = MakePipePerNodeCacheID(false);
        Become(&TTableRangeReader::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvReadTableRangeRequest, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            hFunc(TEvDataShard::TEvReadResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            cFunc(TEvents::TEvPoison::EventType, HandleStop);
            default:
                YDB_LOG_WARN("TableRangeReader: unexpected event",
                    {"selfId", SelfId()},
                    {"eventType", ev->GetTypeRewrite()});
        }
    }

private:
    void Handle(TEvReadTableRangeRequest::TPtr& ev) {
        TEvReadTableRangeRequest* request = ev->Get();

        if (request->TablePath.empty()) {
            ReplyFailure(ev->Sender, request->RequestId, "Empty table path");
            return;
        }
        if (request->Columns.empty()) {
            ReplyFailure(ev->Sender, request->RequestId, "Empty column list");
            return;
        }

        const TString path = request->TablePath;
        TTableScheme& scheme = Tables[path];
        if (scheme.DatabaseName.empty()) {
            scheme.DatabaseName = request->DatabaseName;
        }

        if (scheme.State == TTableScheme::EState::Ready) {
            ProcessRequest(ev, scheme);
            return;
        }

        PendingRequests[path].push_back(std::move(ev));
        if (scheme.State == TTableScheme::EState::Unknown) {
            scheme.State = TTableScheme::EState::Navigating;
            NavigateQueue.push_back(path);
            ProcessNavigateQueue();
        }
    }

    void ProcessNavigateQueue() {
        if (NavigatingPath) {
            return;
        }
        while (!NavigateQueue.empty()) {
            TString path = NavigateQueue.front();
            NavigateQueue.pop_front();
            auto it = Tables.find(path);
            if (it == Tables.end() || it->second.State != TTableScheme::EState::Navigating) {
                continue;
            }
            NavigatingPath = path;
            SendNavigate(path, it->second.DatabaseName);
            return;
        }
    }

    void SendNavigate(const TString& path, const TString& databaseName) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = databaseName;

        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = false;

        YDB_LOG_DEBUG("TableRangeReader: sending navigate request",
            {"selfId", SelfId()},
            {"tablePath", path});

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (!NavigatingPath) {
            return;
        }
        const TString path = std::move(*NavigatingPath);
        NavigatingPath.Clear();

        auto it = Tables.find(path);
        if (it == Tables.end()) {
            ProcessNavigateQueue();
            return;
        }
        TTableScheme& scheme = it->second;

        auto* request = ev->Get()->Request.Get();
        if (request->ResultSet.empty()) {
            scheme.State = TTableScheme::EState::Unknown;
            DrainPending(path, "Empty navigate result");
            ProcessNavigateQueue();
            return;
        }

        const auto& entry = request->ResultSet.front();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            scheme.State = TTableScheme::EState::Unknown;
            DrainPending(path, TStringBuilder() << "Failed to resolve table '" << path << "': status " << (int)entry.Status);
            ProcessNavigateQueue();
            return;
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            scheme.State = TTableScheme::EState::Unknown;
            DrainPending(path, TStringBuilder() << "'" << path << "' is not a user table");
            ProcessNavigateQueue();
            return;
        }
        if (!entry.Self) {
            scheme.State = TTableScheme::EState::Unknown;
            DrainPending(path, TStringBuilder() << "No table entry for '" << path << "'");
            ProcessNavigateQueue();
            return;
        }

        // entry.TableId carries the table schema version, while Info.GetPathVersion()
        // is the general path version and is not accepted by TEvRead.
        scheme.TableId = entry.TableId;
        scheme.KeyTypes.clear();
        scheme.Columns.clear();
        for (const auto& [tag, column] : entry.Columns) {
            TColumnInfo columnInfo;
            columnInfo.Id = column.Id;
            columnInfo.Type = column.PType;
            columnInfo.KeyOrder = column.KeyOrder;
            columnInfo.NotNull = entry.NotNullColumns.contains(column.Name);
            scheme.Columns.emplace(column.Name, std::move(columnInfo));
            if (column.KeyOrder >= 0) {
                const size_t keyOrder = static_cast<size_t>(column.KeyOrder);
                if (scheme.KeyTypes.size() <= keyOrder) {
                    scheme.KeyTypes.resize(keyOrder + 1);
                }
                scheme.KeyTypes[keyOrder] = column.PType;
            }
        }

        scheme.State = TTableScheme::EState::Resolving;
        ResolveQueue.push_back(path);
        ProcessResolveQueue();
        ProcessNavigateQueue();
    }

    void ProcessResolveQueue() {
        if (ResolvingPath) {
            return;
        }
        while (!ResolveQueue.empty()) {
            TString path = ResolveQueue.front();
            ResolveQueue.pop_front();
            auto it = Tables.find(path);
            if (it == Tables.end() || it->second.State != TTableScheme::EState::Resolving) {
                continue;
            }
            ResolvingPath = path;
            SendResolve(path, it->second);
            return;
        }
    }

    void SendResolve(const TString& path, const TTableScheme& scheme) {
        TVector<TCell> minusInf(scheme.KeyTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);

        auto keyDesc = MakeHolder<TKeyDesc>(scheme.TableId, range, TKeyDesc::ERowOperation::Read,
            scheme.KeyTypes, TVector<TKeyDesc::TColumnOp>{});

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = scheme.DatabaseName;

        const ui64 cookie = NextResolveCookie++;
        ResolveCookies[cookie] = path;
        request->ResultSet.emplace_back(std::move(keyDesc));
        request->ResultSet.front().UserData = cookie;

        YDB_LOG_DEBUG("TableRangeReader: sending resolve request",
            {"selfId", SelfId()},
            {"tablePath", path});

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();
        if (request->ResultSet.empty()) {
            return;
        }

        const ui64 cookie = request->ResultSet.front().UserData;
        auto cookieIt = ResolveCookies.find(cookie);
        if (cookieIt == ResolveCookies.end()) {
            return;
        }
        TString path = cookieIt->second;
        ResolveCookies.erase(cookieIt);

        if (ResolvingPath && *ResolvingPath == path) {
            ResolvingPath.Clear();
        }

        auto it = Tables.find(path);
        if (it == Tables.end()) {
            ProcessResolveQueue();
            return;
        }
        TTableScheme& scheme = it->second;

        const auto& entry = request->ResultSet.front();
        // Read requests are resolved into OkData, scheme-only requests into OkScheme.
        if (request->ErrorCount > 0 || !entry.KeyDescription
            || (ui32)entry.Status < (ui32)NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme)
        {
            YDB_LOG_WARN("TableRangeReader: failed to resolve partitioning",
                {"tablePath", path},
                {"errorCount", request->ErrorCount},
                {"status", static_cast<int>(entry.Status)},
                {"hasKeyDescription", bool(entry.KeyDescription)});
            scheme.State = TTableScheme::EState::Unknown;
            DrainPending(path, TStringBuilder() << "Failed to resolve table '" << path << "' partitioning");
            ProcessResolveQueue();
            return;
        }

        scheme.Partitioning = entry.KeyDescription->Partitioning;
        scheme.State = TTableScheme::EState::Ready;

        YDB_LOG_DEBUG("TableRangeReader: table resolved",
            {"selfId", SelfId()},
            {"tablePath", path},
            {"keyColumns", scheme.KeyTypes.size()},
            {"columns", scheme.Columns.size()});

        ProcessResolveQueue();
        DrainPending(path);
    }

    void DrainPending(const TString& path, const TString& error = "") {
        auto it = PendingRequests.find(path);
        if (it == PendingRequests.end()) {
            return;
        }
        auto requests = std::move(it->second);
        PendingRequests.erase(it);

        if (error.empty()) {
            TTableScheme& scheme = Tables.at(path);
            for (auto& handle : requests) {
                ProcessRequest(handle, scheme);
            }
        } else {
            for (const auto& handle : requests) {
                ReplyFailure(handle->Sender, handle->Get()->RequestId, error);
            }
        }
    }

    void ProcessRequest(TEvReadTableRangeRequest::TPtr& ev, TTableScheme& scheme) {
        TEvReadTableRangeRequest* request = ev->Get();

        TString error;
        TVector<ui32> columnTags;
        columnTags.reserve(request->Columns.size());
        for (const auto& name : request->Columns) {
            auto it = scheme.Columns.find(name);
            if (it == scheme.Columns.end()) {
                error = TStringBuilder() << "Unknown column '" << name << "' in table '" << request->TablePath << "'";
                break;
            }
            columnTags.push_back(it->second.Id);
        }
        if (error) {
            ReplyFailure(ev->Sender, request->RequestId, error);
            return;
        }

        auto state = MakeHolder<TRequestState>();
        state->InternalId = NextInternalId++;
        state->RequestId = request->RequestId;
        state->Sender = ev->Sender;
        state->TablePath = request->TablePath;
        state->Snapshot = request->Snapshot;
        state->ColumnTags = std::move(columnTags);
        state->Range = TSerializedTableRange(request->FromKey.GetCells(), request->FromInclusive,
            request->ToKey.GetCells(), request->ToInclusive);

        TTableRange range(state->Range.From.GetCells(), state->Range.FromInclusive,
            state->Range.To.GetCells(), state->Range.ToInclusive);
        const auto intersections = scheme.Partitioning->GetIntersectionWithRange(scheme.KeyTypes, range);

        for (const auto& intersection : intersections) {
            TShardRead shard;
            shard.TabletId = intersection.ShardId;
            shard.OriginalRange = TSerializedTableRange(static_cast<const TTableRange&>(intersection.TableRange));
            state->Shards.push_back(std::move(shard));
        }
        state->ShardsRemaining = state->Shards.size();

        const ui64 internalId = state->InternalId;
        Requests.emplace(internalId, std::move(state));

        YDB_LOG_DEBUG("TableRangeReader: processing read request",
            {"selfId", SelfId()},
            {"tablePath", request->TablePath},
            {"requestId", request->RequestId},
            {"shards", Requests[internalId]->Shards.size()});

        if (Requests[internalId]->Shards.empty()) {
            RespondOk(internalId);
            return;
        }

        const size_t shardCount = Requests[internalId]->Shards.size();
        for (size_t i = 0; i < shardCount; ++i) {
            SendShardRead(internalId, static_cast<ui32>(i));
        }
    }

    void SendShardRead(ui64 internalId, ui32 shardIndex) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        TRequestState& state = *it->second;
        TShardRead& shard = state.Shards[shardIndex];
        TTableScheme& scheme = Tables.at(state.TablePath);

        shard.ReadId = NextReadId++;
        ReadIdRoutes[shard.ReadId] = TReadIdRoute{internalId, shardIndex};
        shard.LastSeqNo = 0;
        shard.Finished = false;

        auto request = MakeHolder<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        record.SetReadId(shard.ReadId);
        record.MutableTableId()->SetOwnerId(scheme.TableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(scheme.TableId.PathId.LocalPathId);
        if (scheme.TableId.SchemaVersion) {
            record.MutableTableId()->SetSchemaVersion(scheme.TableId.SchemaVersion);
        }
        for (ui32 tag : state.ColumnTags) {
            record.AddColumns(tag);
        }
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        if (state.Snapshot) {
            record.MutableSnapshot()->SetStep(state.Snapshot->Step);
            record.MutableSnapshot()->SetTxId(state.Snapshot->TxId);
        }
        //record.SetMaxRows(static_cast<ui32>(Min<ui64>(state.MaxRows, Max<ui32>())));
        //record.SetMaxBytes(static_cast<ui32>(Min<ui64>(state.MaxBytes, Max<ui32>())));

        if (shard.HasResumeKey) {
            request->Ranges.emplace_back(shard.ResumeKey.GetCells(), false,
                shard.OriginalRange.To.GetCells(), shard.OriginalRange.ToInclusive);
        } else {
            request->Ranges.push_back(shard.OriginalRange);
        }

        YDB_LOG_DEBUG("TableRangeReader: sending read to shard",
            {"selfId", SelfId()},
            {"tablePath", state.TablePath},
            {"shardId", shard.TabletId},
            {"readId", shard.ReadId},
            {"resumed", shard.HasResumeKey},
            {"retries", shard.Retries});

        SubscribedTablets.insert(shard.TabletId);
        Send(PipeCacheId, new TEvPipeCache::TEvForward(request.Release(), shard.TabletId,
            TEvPipeCache::TEvForwardOptions{.AutoConnect = true, .Subscribe = true}),
            IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto routeIt = ReadIdRoutes.find(record.GetReadId());
        if (routeIt == ReadIdRoutes.end()) {
            // Cancelled or completed read.
            return;
        }
        const TReadIdRoute route = routeIt->second;

        auto requestIt = Requests.find(route.InternalRequestId);
        if (requestIt == Requests.end()) {
            return;
        }
        TRequestState& state = *requestIt->second;
        TShardRead& shard = state.Shards[route.ShardIndex];

        shard.LastSeqNo = record.GetSeqNo();

        const auto status = record.HasStatus() ? record.GetStatus().GetCode() : Ydb::StatusIds::INTERNAL_ERROR;
        if (status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            if (record.HasStatus()) {
                NYql::IssuesFromMessage(record.GetStatus().GetIssues(), issues);
            }
            HandleShardFailure(state, shard, route.ShardIndex, status, issues);
            return;
        }

        if (record.GetRowCount() > 0) {
            shard.Batches.push_back(std::move(ev));
        }

        shard.ResumeKey = {};
        shard.HasResumeKey = false;
        if (record.HasContinuationToken()) {
            NKikimrTxDataShard::TReadContinuationToken token;
            if (token.ParseFromString(record.GetContinuationToken()) && token.HasLastProcessedKey() && !token.GetLastProcessedKey().empty()) {
                shard.ResumeKey = TSerializedCellVec(token.GetLastProcessedKey());
                shard.HasResumeKey = true;
            }
        }

        if (record.GetFinished()) {
            shard.Finished = true;
            if (state.ShardsRemaining > 0) {
                --state.ShardsRemaining;
            }
            if (state.ShardsRemaining == 0) {
                RespondOk(state.InternalId);
            }
            return;
        }

        SendAck(state, shard);
    }

    void SendAck(TRequestState&, TShardRead& shard) {
        auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
        ack->Record.SetReadId(shard.ReadId);
        ack->Record.SetSeqNo(shard.LastSeqNo);
        ack->Record.SetMaxRows(gTableRangeReaderMaxRows);
        ack->Record.SetMaxBytes(UINT32_MAX);
        Send(PipeCacheId, new TEvPipeCache::TEvForward(ack.Release(), shard.TabletId,
            TEvPipeCache::TEvForwardOptions{.AutoConnect = true, .Subscribe = true}),
            IEventHandle::FlagTrackDelivery);
    }

    void HandleShardFailure(TRequestState& state, TShardRead& shard, ui32 shardIndex,
            Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
    {
        if (IsRetriableStatus(status) && shard.Retries < MaxShardRetries) {
            ++shard.Retries;
            RetryShard(state.InternalId, shardIndex);
            return;
        }

        const ui64 internalId = state.InternalId;
        const ui64 tabletId = shard.TabletId;
        FailRequest(internalId, TStringBuilder()
            << "Failed to read table '" << state.TablePath << "' from shard " << tabletId
            << ": " << Ydb::StatusIds::StatusCode_Name(status), issues);
    }

    void RetryShard(ui64 internalId, ui32 shardIndex) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        TRequestState& state = *it->second;
        TShardRead& shard = state.Shards[shardIndex];

        if (!state.Shards[shardIndex].HasResumeKey) {
            // No continuation point, so the whole range must be re-read.
            shard.Batches.clear();
        }
        ReadIdRoutes.erase(shard.ReadId);
        // Best effort: the original read may still be alive on the shard.
        SendReadCancel(shard.TabletId, shard.ReadId);

        YDB_LOG_DEBUG("TableRangeReader: retrying shard read",
            {"selfId", SelfId()},
            {"tablePath", state.TablePath},
            {"shardId", shard.TabletId},
            {"resumed", shard.HasResumeKey},
            {"retries", shard.Retries});

        SendShardRead(internalId, shardIndex);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const ui64 tabletId = ev->Get()->TabletId;

        TVector<std::pair<ui64, ui32>> toRetry;
        for (const auto& [internalId, state] : Requests) {
            for (ui32 i = 0; i < state->Shards.size(); ++i) {
                const TShardRead& shard = state->Shards[i];
                if (!shard.Finished && shard.TabletId == tabletId) {
                    toRetry.emplace_back(internalId, i);
                }
            }
        }

        YDB_LOG_WARN("TableRangeReader: delivery problem",
            {"selfId", SelfId()},
            {"tabletId", tabletId},
            {"shards", toRetry.size()});

        for (const auto& [internalId, shardIndex] : toRetry) {
            auto it = Requests.find(internalId);
            if (it == Requests.end()) {
                continue;
            }
            TRequestState& state = *it->second;
            TShardRead& shard = state.Shards[shardIndex];
            if (shard.Retries >= MaxShardRetries) {
                const TString path = state.TablePath;
                FailRequest(internalId, TStringBuilder()
                    << "Failed to deliver read to table '" << path << "' shard " << tabletId);
                continue;
            }
            ++shard.Retries;
            RetryShard(internalId, shardIndex);
        }
    }

    void SendReadCancel(ui64 tabletId, ui64 readId) {
        auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
        cancel->Record.SetReadId(readId);
        Send(PipeCacheId, new TEvPipeCache::TEvForward(cancel.Release(), tabletId, false /* subscribe */));
    }

    void CancelShardRead(const TShardRead& shard) {
        ReadIdRoutes.erase(shard.ReadId);
        SendReadCancel(shard.TabletId, shard.ReadId);
    }

    void RemoveRoutes(TRequestState& state) {
        for (const auto& shard : state.Shards) {
            ReadIdRoutes.erase(shard.ReadId);
        }
    }

    void RespondOk(ui64 internalId) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        THolder<TRequestState> state = std::move(it->second);
        Requests.erase(it);

        auto response = MakeHolder<TEvReadTableRangeResponse>(state->RequestId);
        response->Success = true;
        response->PathId = Tables.at(state->TablePath).TableId.PathId;
        size_t rows = 0;
        for (auto& shard : state->Shards) {
            for (auto& batch : shard.Batches) {
                rows += batch->Get()->GetRowsCount();
                response->Batches.push_back(std::move(batch));
            }
        }
        RemoveRoutes(*state);

        YDB_LOG_DEBUG("TableRangeReader: responding with rows",
            {"selfId", SelfId()},
            {"tablePath", state->TablePath},
            {"requestId", state->RequestId},
            {"rows", rows});

        Send(state->Sender, response.Release());
    }

    void FailRequest(ui64 internalId, const TString& message, const NYql::TIssues& issues = {}) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        THolder<TRequestState> state = std::move(it->second);
        Requests.erase(it);

        for (const auto& shard : state->Shards) {
            if (!shard.Finished) {
                CancelShardRead(shard);
            }
        }
        RemoveRoutes(*state);

        auto response = MakeHolder<TEvReadTableRangeResponse>(state->RequestId);
        response->Success = false;
        response->Issues = issues;
        response->Issues.AddIssue(message);

        YDB_LOG_WARN("TableRangeReader: request failed",
            {"selfId", SelfId()},
            {"tablePath", state->TablePath},
            {"requestId", state->RequestId},
            {"message", message});

        Send(state->Sender, response.Release());
    }

    static void ReplyFailure(const TActorId& sender, ui64 requestId, const TString& message) {
        auto response = MakeHolder<TEvReadTableRangeResponse>(requestId);
        response->Success = false;
        response->Issues.AddIssue(message);
        TActivationContext::Send(new IEventHandle(sender, TActorId(), response.Release()));
    }

    void HandleStop() {
        for (const auto& [internalId, state] : Requests) {
            for (const auto& shard : state->Shards) {
                if (!shard.Finished) {
                    CancelShardRead(shard);
                }
            }
        }
        Requests.clear();
        ReadIdRoutes.clear();

        for (ui64 tabletId : SubscribedTablets) {
            Send(PipeCacheId, new TEvPipeCache::TEvUnlink(tabletId));
        }
        SubscribedTablets.clear();

        PassAway();
    }

private:
    TActorId PipeCacheId;
    ui64 NextInternalId = 1;
    ui64 NextReadId = 1;
    ui64 NextResolveCookie = 1;

    THashMap<TString, TTableScheme> Tables;
    THashMap<TString, TVector<TEvReadTableRangeRequest::TPtr>> PendingRequests;

    TDeque<TString> NavigateQueue;
    TMaybe<TString> NavigatingPath;
    TDeque<TString> ResolveQueue;
    TMaybe<TString> ResolvingPath;
    THashMap<ui64, TString> ResolveCookies;

    THashMap<ui64, THolder<TRequestState>> Requests;
    THashMap<ui64, TReadIdRoute> ReadIdRoutes;
    THashSet<ui64> SubscribedTablets;
};

} // anonymous namespace

NActors::IActor* CreateTableRangeReader() {
    return new TTableRangeReader();
}

} // namespace NKikimr::NDataShard::NBuildIndex
