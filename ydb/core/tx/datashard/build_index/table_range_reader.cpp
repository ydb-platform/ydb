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
#include <util/datetime/base.h>
#include <util/string/builder.h>

#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard::NBuildIndex {

ui64 gTableRangeReaderMaxRows = UINT64_MAX;

namespace {

// Exponential retry delay with 5 second cap.
TDuration RetryDelay(ui32 attempt) {
    constexpr ui32 maxShift = 6;
    constexpr ui32 maxDelayMs = 5000;
    return TDuration::MilliSeconds(Min<ui32>(100u << Min<ui32>(attempt, maxShift), maxDelayMs));
}

// NOT_FOUND and SCHEME_ERROR mean the table was repartitioned.
// We have to re-resolve and retry the whole read from the beginning.
bool IsSchemeOutdatedStatus(Ydb::StatusIds::StatusCode status) {
    return status == Ydb::StatusIds::NOT_FOUND || status == Ydb::StatusIds::SCHEME_ERROR;
}

// The shard still owns the range, it is just temporarily unavailable: it may be
// overloaded, or moving to another node.
bool IsRetriableStatus(Ydb::StatusIds::StatusCode status) {
    switch (status) {
        case Ydb::StatusIds::OVERLOADED:
        case Ydb::StatusIds::UNAVAILABLE:
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
// A temporarily unavailable shard is retried on its own, a shard that has split
// or merged (or a table that has been altered) drops the cached table scheme and
// restarts the whole request after a backoff, so a response never mixes data
// from different schemes. Every request is answered with a single
// TEvReadTableRangeResponse. In-flight reads are cancelled when a request fails
// or when the actor is stopped.
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
        // The scheme is being re-resolved after a retriable read error.
        bool Refresh = false;
        TString DatabaseName;
        TTableId TableId;
        TVector<NScheme::TTypeInfo> KeyTypes;
        THashMap<TString, TColumnInfo> Columns;
        std::shared_ptr<const TPartitioning> Partitioning;
        // Grows on every successful resolve, so a request can tell whether the
        // scheme it failed with has already been re-resolved.
        ui64 Generation = 0;
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
        // A retry of this shard read is already scheduled.
        bool RetryPending = false;
        ui64 LastSeqNo = 0;
        ui32 Retries = 0;
        TVector<TEvDataShard::TEvReadResult::TPtr> Batches;
    };

    struct TEvPrivate {
        enum EEv {
            EvRetryShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 100,
            EvRestartRequest,
        };

        struct TEvRetryShard : public TEventLocal<TEvRetryShard, EvRetryShard> {
            ui64 InternalRequestId = 0;
            ui32 ShardIndex = 0;
            // The failed read, so a retry scheduled before a request restart is
            // not applied to the new shards.
            ui64 ReadId = 0;

            TEvRetryShard(ui64 internalRequestId, ui32 shardIndex, ui64 readId)
                : InternalRequestId(internalRequestId)
                , ShardIndex(shardIndex)
                , ReadId(readId)
            {}
        };

        struct TEvRestartRequest : public TEventLocal<TEvRestartRequest, EvRestartRequest> {
            ui64 InternalRequestId = 0;

            explicit TEvRestartRequest(ui64 internalRequestId)
                : InternalRequestId(internalRequestId)
            {}
        };
    };

    struct TRequestState {
        ui64 InternalId = 0;
        ui64 RequestId = 0;
        TActorId Sender;
        TString TablePath;
        TMaybe<TRowVersion> Snapshot;
        TVector<TString> ColumnNames;
        TVector<ui32> ColumnTags;
        TSerializedTableRange Range;
        TVector<TShardRead> Shards;
        size_t ShardsRemaining = 0;
        // The table scheme generation the reads were started with.
        ui64 SchemeGeneration = 0;
        // How many times the whole request has been restarted.
        ui32 Retries = 0;
        // No reads are in flight, the request waits to be restarted.
        bool Waiting = false;
        // The backoff has elapsed, the request can be restarted as soon as the
        // table scheme is resolved.
        bool ReadyToRestart = false;
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
            hFunc(TEvPrivate::TEvRetryShard, Handle);
            hFunc(TEvPrivate::TEvRestartRequest, Handle);
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
            ProcessRequest(ev);
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
            SendNavigate(path, it->second.DatabaseName, it->second.Refresh);
            return;
        }
    }

    void SendNavigate(const TString& path, const TString& databaseName, bool syncVersion) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = databaseName;

        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        // A retry must not be answered from a stale cache entry: ask the scheme
        // cache to wait for the current scheme board version.
        entry.SyncVersion = syncVersion;

        YDB_LOG_DEBUG("TableRangeReader: sending navigate request",
            {"selfId", SelfId()},
            {"tablePath", path},
            {"syncVersion", syncVersion});

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
            FailNavigation(path, scheme, "Empty navigate result");
            return;
        }

        const auto& entry = request->ResultSet.front();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            FailNavigation(path, scheme, TStringBuilder() << "Failed to resolve table '" << path << "': status " << (int)entry.Status);
            return;
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            FailNavigation(path, scheme, TStringBuilder() << "'" << path << "' is not a user table");
            return;
        }
        if (!entry.Self) {
            FailNavigation(path, scheme, TStringBuilder() << "No table entry for '" << path << "'");
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

    void FailNavigation(const TString& path, TTableScheme& scheme, const TString& error) {
        scheme.Refresh = false;
        scheme.State = TTableScheme::EState::Unknown;
        FailWaitingRequests(path, error);
        DrainPending(path, error);
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
        // A retry must not be answered from a stale cache entry: ask the scheme
        // cache to wait for the current scheme board version.
        request->ResultSet.front().SyncVersion = scheme.Refresh;

        YDB_LOG_DEBUG("TableRangeReader: sending resolve request",
            {"selfId", SelfId()},
            {"tablePath", path},
            {"syncVersion", scheme.Refresh});

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
            scheme.Refresh = false;
            scheme.State = TTableScheme::EState::Unknown;
            const TString error = TStringBuilder() << "Failed to resolve table '" << path << "' partitioning";
            FailWaitingRequests(path, error);
            DrainPending(path, error);
            ProcessResolveQueue();
            return;
        }

        scheme.Partitioning = entry.KeyDescription->Partitioning;
        const bool wasRefresh = scheme.Refresh;
        scheme.Refresh = false;
        ++scheme.Generation;
        scheme.State = TTableScheme::EState::Ready;

        YDB_LOG_DEBUG("TableRangeReader: table resolved",
            {"selfId", SelfId()},
            {"tablePath", path},
            {"keyColumns", scheme.KeyTypes.size()},
            {"columns", scheme.Columns.size()},
            {"shards", scheme.Partitioning->Size()},
            {"refresh", wasRefresh},
            {"generation", scheme.Generation});

        StartWaitingRequests(path);
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
            for (auto& handle : requests) {
                ProcessRequest(handle);
            }
        } else {
            for (const auto& handle : requests) {
                ReplyFailure(handle->Sender, handle->Get()->RequestId, error);
            }
        }
    }

    void ProcessRequest(TEvReadTableRangeRequest::TPtr& ev) {
        TEvReadTableRangeRequest* request = ev->Get();

        auto state = MakeHolder<TRequestState>();
        state->InternalId = NextInternalId++;
        state->RequestId = request->RequestId;
        state->Sender = ev->Sender;
        state->TablePath = request->TablePath;
        state->Snapshot = request->Snapshot;
        state->ColumnNames = request->Columns;
        state->Range = TSerializedTableRange(request->FromKey.GetCells(), request->FromInclusive,
            request->ToKey.GetCells(), request->ToInclusive);

        const ui64 internalId = state->InternalId;
        Requests.emplace(internalId, std::move(state));
        StartRequest(internalId);
    }

    // Builds the shards of a request from the current table scheme and starts
    // their reads. Also used to restart a request from scratch.
    void StartRequest(ui64 internalId) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        TRequestState& state = *it->second;
        TTableScheme& scheme = Tables.at(state.TablePath);
        Y_ABORT_UNLESS(scheme.State == TTableScheme::EState::Ready);

        TVector<ui32> columnTags;
        columnTags.reserve(state.ColumnNames.size());
        for (const auto& name : state.ColumnNames) {
            auto column = scheme.Columns.find(name);
            if (column == scheme.Columns.end()) {
                FailRequest(internalId, TStringBuilder()
                    << "Unknown column '" << name << "' in table '" << state.TablePath << "'");
                return;
            }
            columnTags.push_back(column->second.Id);
        }

        state.ColumnTags = std::move(columnTags);
        state.SchemeGeneration = scheme.Generation;
        state.Waiting = false;
        state.ReadyToRestart = false;

        const auto intersections = scheme.Partitioning->GetIntersectionWithRange(scheme.KeyTypes, state.Range.ToTableRange());

        state.Shards.clear();
        state.ShardsRemaining = 0;
        for (const auto& intersection : intersections) {
            TShardRead shard;
            shard.TabletId = intersection.ShardId;
            shard.OriginalRange = TSerializedTableRange(intersection.TableRange);
            if (shard.OriginalRange.To.GetCells().empty()) {
                // make +inf stay +inf, otherwise it's transformed into -inf by datashard__read_iterator.cpp
                // with keyTo = ToRawTypeValue(keyToCells.GetCells(), TableInfo, !toInclusive) :-(((
                shard.OriginalRange.ToInclusive = true;
            }
            state.Shards.push_back(std::move(shard));
            ++state.ShardsRemaining;
        }

        YDB_LOG_DEBUG("TableRangeReader: starting read request",
            {"selfId", SelfId()},
            {"tablePath", state.TablePath},
            {"requestId", state.RequestId},
            {"shards", state.Shards.size()},
            {"retries", state.Retries});

        if (state.Shards.empty()) {
            RespondOk(internalId);
            return;
        }

        const size_t shardCount = state.Shards.size();
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

        YDB_LOG_DEBUG("TableRangeReader: received read result",
            {"selfId", SelfId()},
            {"shardId", shard.TabletId},
            {"rowCount", record.GetRowCount()});
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
        if (IsSchemeOutdatedStatus(status)) {
            // The shard has split or merged, or the cached scheme is outdated:
            // the whole request has to be restarted against a fresh scheme.
            RestartRequest(state.InternalId, TStringBuilder()
                << "shard " << shard.TabletId
                << " answered " << Ydb::StatusIds::StatusCode_Name(status));
            return;
        }

        if (IsRetriableStatus(status)) {
            // The shard still owns the range, it is just unavailable for a
            // while (overloaded, moving to another node, ...).
            RetryShardLater(state.InternalId, shardIndex, shard,
                TStringBuilder() << "status " << Ydb::StatusIds::StatusCode_Name(status));
            return;
        }

        FailRequest(state.InternalId, TStringBuilder()
            << "Failed to read table '" << state.TablePath << "' from shard " << shard.TabletId
            << ": " << Ydb::StatusIds::StatusCode_Name(status), issues);
    }

    void RetryShardLater(ui64 internalId, ui32 shardIndex, TShardRead& shard, const TString& reason) {
        if (shard.RetryPending) {
            // Several failures of one read schedule a single retry.
            return;
        }
        shard.RetryPending = true;
        ++shard.Retries;
        const TDuration delay = RetryDelay(shard.Retries);

        YDB_LOG_DEBUG("TableRangeReader: scheduling shard read retry",
            {"selfId", SelfId()},
            {"shardId", shard.TabletId},
            {"reason", reason},
            {"retries", shard.Retries},
            {"delay", delay});

        Schedule(delay, new TEvPrivate::TEvRetryShard(internalId, shardIndex, shard.ReadId));
    }

    void Handle(TEvPrivate::TEvRetryShard::TPtr& ev) {
        RetryShard(ev->Get()->InternalRequestId, ev->Get()->ShardIndex, ev->Get()->ReadId);
    }

    void RetryShard(ui64 internalId, ui32 shardIndex, ui64 readId) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        TRequestState& state = *it->second;
        if (shardIndex >= state.Shards.size()) {
            return;
        }
        TShardRead& shard = state.Shards[shardIndex];
        if (shard.Finished || shard.ReadId != readId) {
            // The request has been restarted or the shard has been re-armed
            // meanwhile, so this retry is stale.
            return;
        }
        shard.RetryPending = false;

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

    // Drops everything read so far and restarts the whole request after a
    // backoff, so a retriable error never leaves a partially read range behind.
    void RestartRequest(ui64 internalId, const TString& reason) {
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        TRequestState& state = *it->second;
        if (state.Waiting) {
            // Already scheduled by another failed shard of this request.
            return;
        }

        ++state.Retries;
        for (const auto& shard : state.Shards) {
            if (!shard.Finished) {
                CancelShardRead(shard);
            }
        }
        RemoveRoutes(state);
        state.Shards.clear();
        state.ShardsRemaining = 0;
        state.Waiting = true;
        state.ReadyToRestart = false;

        const TDuration delay = RetryDelay(state.Retries);
        YDB_LOG_WARN("TableRangeReader: restarting read request",
            {"selfId", SelfId()},
            {"tablePath", state.TablePath},
            {"requestId", state.RequestId},
            {"reason", reason},
            {"retries", state.Retries},
            {"delay", delay});

        Schedule(delay, new TEvPrivate::TEvRestartRequest(internalId));
    }

    void Handle(TEvPrivate::TEvRestartRequest::TPtr& ev) {
        const ui64 internalId = ev->Get()->InternalRequestId;
        auto it = Requests.find(internalId);
        if (it == Requests.end()) {
            return;
        }
        TRequestState& state = *it->second;
        if (!state.Waiting) {
            return;
        }
        state.ReadyToRestart = true;

        TTableScheme& scheme = Tables.at(state.TablePath);
        switch (scheme.State) {
            case TTableScheme::EState::Ready:
                if (state.SchemeGeneration != scheme.Generation) {
                    // The scheme has been re-resolved since the failed read, so
                    // the request can be restarted right away.
                    StartRequest(internalId);
                } else {
                    ResetTableScheme(state.TablePath);
                }
                return;
            case TTableScheme::EState::Unknown:
                // Nothing is in flight for an unknown table, so the navigation
                // has to be started here.
                ResetTableScheme(state.TablePath);
                return;
            default:
                // Navigation or resolve is already in progress, the request is
                // started as soon as the scheme is ready.
                return;
        }
    }

    // Drops the cached table scheme, so the next resolve sees the current shards
    // and schema version.
    void ResetTableScheme(const TString& path) {
        auto it = Tables.find(path);
        if (it == Tables.end()) {
            return;
        }
        TTableScheme& scheme = it->second;
        if (scheme.State != TTableScheme::EState::Ready
            && scheme.State != TTableScheme::EState::Unknown)
        {
            // Already re-resolving.
            return;
        }

        YDB_LOG_DEBUG("TableRangeReader: dropping cached table scheme",
            {"selfId", SelfId()},
            {"tablePath", path},
            {"generation", scheme.Generation});

        scheme.Refresh = true;
        scheme.State = TTableScheme::EState::Navigating;
        NavigateQueue.push_back(path);
        ProcessNavigateQueue();
    }

    void StartWaitingRequests(const TString& path) {
        TVector<ui64> toStart;
        for (const auto& [internalId, state] : Requests) {
            if (state->Waiting && state->ReadyToRestart && state->TablePath == path) {
                toStart.push_back(internalId);
            }
        }
        for (ui64 internalId : toStart) {
            if (Requests.find(internalId) != Requests.end()) {
                StartRequest(internalId);
            }
        }
    }

    void FailWaitingRequests(const TString& path, const TString& message) {
        TVector<ui64> toFail;
        for (const auto& [internalId, state] : Requests) {
            if (state->Waiting && state->TablePath == path) {
                toFail.push_back(internalId);
            }
        }
        for (ui64 internalId : toFail) {
            FailRequest(internalId, message);
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const ui64 tabletId = ev->Get()->TabletId;
        const bool isDeleted = ev->Get()->IsDeleted;

        TVector<std::pair<ui64, ui32>> affected;
        for (const auto& [internalId, state] : Requests) {
            if (state->Waiting) {
                continue;
            }
            for (ui32 i = 0; i < state->Shards.size(); ++i) {
                const TShardRead& shard = state->Shards[i];
                if (!shard.Finished && shard.TabletId == tabletId) {
                    affected.emplace_back(internalId, i);
                }
            }
        }

        YDB_LOG_WARN("TableRangeReader: delivery problem",
            {"selfId", SelfId()},
            {"tabletId", tabletId},
            {"deleted", isDeleted},
            {"shards", affected.size()});

        for (const auto& [internalId, shardIndex] : affected) {
            auto it = Requests.find(internalId);
            if (it == Requests.end()) {
                continue;
            }
            TRequestState& state = *it->second;
            if (shardIndex >= state.Shards.size()) {
                continue;
            }

            if (isDeleted) {
                // The tablet is merged and deleted, restart the request
                RestartRequest(internalId, TStringBuilder()
                    << "shard " << tabletId << " is deleted");
                continue;
            }

            RetryShardLater(internalId, shardIndex, state.Shards[shardIndex], "delivery problem");
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
