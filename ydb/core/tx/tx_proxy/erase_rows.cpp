#include "erase_rows.h"
#include "proxy.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/ydb_issue/issue_helpers.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/xrange.h>
#include <util/string/builder.h>

#include <memory>

namespace NKikimr {
namespace NTxProxy {

using namespace NActors;

namespace {

    constexpr ui64 TimeoutWakeupTag = 0;
    constexpr ui64 RetryWakeupTag = 1;

    bool IsRetriableStatus(NKikimrTxDataShard::TEvEraseRowsResponse::EStatus status) {
        switch (status) {
            case NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_OVERLOADED:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_TRY_LATER:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_NOT_AVAILABLE:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_ABORTED:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_CANCELLED:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_RESULT_UNAVAILABLE:
                return true;
            default:
                return false;
        }
    }

    Ydb::StatusIds::StatusCode YdbStatusForEraseStatus(NKikimrTxDataShard::TEvEraseRowsResponse::EStatus status) {
        switch (status) {
            case NKikimrTxDataShard::TEvEraseRowsResponse::OK:
                return Ydb::StatusIds::SUCCESS;
            case NKikimrTxDataShard::TEvEraseRowsResponse::BAD_REQUEST:
                return Ydb::StatusIds::BAD_REQUEST;
            case NKikimrTxDataShard::TEvEraseRowsResponse::SCHEME_ERROR:
                return Ydb::StatusIds::SCHEME_ERROR;
            case NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_OVERLOADED:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_TRY_LATER:
                return Ydb::StatusIds::OVERLOADED;
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_NOT_AVAILABLE:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_ABORTED:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_CANCELLED:
            case NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_RESULT_UNAVAILABLE:
                return Ydb::StatusIds::UNAVAILABLE;
            default:
                return Ydb::StatusIds::GENERIC_ERROR;
        }
    }

    Ydb::StatusIds::StatusCode YdbStatusForNavigateStatus(NSchemeCache::TSchemeCacheNavigate::EStatus status) {
        switch (status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                return Ydb::StatusIds::SUCCESS;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                return Ydb::StatusIds::UNAVAILABLE;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                return Ydb::StatusIds::UNAUTHORIZED;
            default:
                return Ydb::StatusIds::SCHEME_ERROR;
        }
    }

    class TEraseRowsInternal: public TActorBootstrapped<TEraseRowsInternal> {
        using TBase = TActorBootstrapped<TEraseRowsInternal>;
        using TTabletId = ui64;

        static constexpr TDuration DEFAULT_TIMEOUT = TDuration::Seconds(5 * 60);

        struct TShardEraseRetryState {
            // Basic request settings (table id, key column ids, flags)
            NKikimrTxDataShard::TEvEraseRowsRequest Headers;
            TVector<TSerializedCellVec> Keys;
            ui64 LastOverloadSeqNo = 0;
            ui64 SentOverloadSeqNo = 0;
        };

    public:
        TEraseRowsInternal(
                TActorId sender,
                const TString& database,
                const TString& table,
                std::shared_ptr<const TEraseKeys> keys,
                bool writeToTableShadow,
                bool writeToPrivateTable,
                bool writeToIndexImplTable,
                ui64 cookie,
                TBackoff backoff)
            : Sender(sender)
            , SchemeCache(MakeSchemeCacheID())
            , LeaderPipeCache(MakePipePerNodeCacheID(false))
            , Database(database)
            , Table(table)
            , Keys(std::move(keys))
            , WriteToTableShadow(writeToTableShadow)
            , AllowWriteToPrivateTable(writeToPrivateTable)
            , AllowWriteToIndexImplTable(writeToIndexImplTable)
            , Cookie(cookie)
            , Backoff(backoff)
        {}

        void Bootstrap(const TActorContext& ctx) {
            StartTime = TAppData::TimeProvider->Now();
            ResolveTable(ctx);
        }

        void Die(const TActorContext& ctx) override {
            for (auto& pr : ShardEraseRetryStates) {
                if (pr.second.SentOverloadSeqNo) {
                    auto* msg = new TEvDataShard::TEvOverloadUnsubscribe(pr.second.SentOverloadSeqNo);
                    ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(msg, pr.first, false));
                }
            }
            ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
            if (TimeoutTimerActorId) {
                ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
            }
            TBase::Die(ctx);
        }

    private:
        const TKeyDesc* GetKeyRange() const {
            Y_ABORT_UNLESS(ResolvePartitionsResult->ResultSet.size() == 1);
            return ResolvePartitionsResult->ResultSet[0].KeyDescription.Get();
        }

        void ResolveTable(const TActorContext& ctx) {
            TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
            request->DatabaseName = Database;

            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = ::NKikimr::SplitPath(Table);
            if (entry.Path.empty()) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Invalid table path specified: '" << Table << "'", ctx);
            }
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            entry.SyncVersion = true;
            entry.ShowPrivatePath = AllowWriteToPrivateTable;
            request->ResultSet.emplace_back(entry);

            ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

            TimeoutTimerActorId = CreateLongTimer(ctx, Timeout,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(TimeoutWakeupTag)));

            Become(&TThis::StateWaitResolveTable);
        }

        void HandleTimeout(const TActorContext& ctx) {
            ShardRepliesLeft.clear();
            return ReplyWithError(Ydb::StatusIds::TIMEOUT,
                TStringBuilder() << "timed out, duration: " << (TAppData::TimeProvider->Now() - StartTime).Seconds() << " sec",
                ctx);
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
            Y_ABORT_UNLESS(request.ResultSet.size() == 1);
            const auto& entry = request.ResultSet.front();

            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                return ReplyWithError(YdbStatusForNavigateStatus(entry.Status),
                    TStringBuilder() << "Failed to resolve table: " << entry.Status, ctx);
            }

            if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Only the OLTP table is supported", ctx);
            }
            if (entry.TableId.IsSystemView() || entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindSysView) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Table is a system view, not supported", ctx);
            }

            IsIndexImplTable = (entry.TableKind != NSchemeCache::ETableKind::KindRegularTable);
            ResolveNamesResult.reset(ev->Get()->Request.Release());

            for (const auto& [_, colInfo] : entry.Columns) {
                const i32 keyOrder = colInfo.KeyOrder;
                if (keyOrder >= 0) {
                    KeyColumnIds.resize(Max<size_t>(KeyColumnIds.size(), keyOrder + 1));
                    KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), keyOrder + 1));
                    KeyColumnIds[keyOrder] = colInfo.Id;
                    KeyColumnTypes[keyOrder] = colInfo.PType;
                }
            }

            if (KeyColumnIds.empty()) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Table has no key columns", ctx);
            }

            ResolveShards(ctx);
        }

        void FindMinMaxKeys() {
            MinKey = {};
            MaxKey = {};
            for (const auto& key : *Keys) {
                if (MinKey.GetCells().empty()) {
                    MinKey = key;
                    MaxKey = key;
                } else if (CompareTypedCellVectors(key.GetCells().data(), MinKey.GetCells().data(),
                            KeyColumnTypes.data(), key.GetCells().size(), MinKey.GetCells().size()) < 0)
                {
                    MinKey = key;
                } else if (CompareTypedCellVectors(key.GetCells().data(), MaxKey.GetCells().data(),
                            KeyColumnTypes.data(), key.GetCells().size(), MaxKey.GetCells().size()) > 0)
                {
                    MaxKey = key;
                }
            }
        }

        void ResolveShards(const TActorContext& ctx) {
            if (Keys->empty()) {
                return ReplyIfDone(ctx);
            }

            for (const auto& key : *Keys) {
                if (key.GetCells().size() != KeyColumnIds.size()) {
                    return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                        TStringBuilder() << "Key cell count doesn't match row scheme: got "
                            << key.GetCells().size() << ", expected " << KeyColumnIds.size(), ctx);
                }
            }

            Y_ABORT_UNLESS(ResolveNamesResult);

            auto& entry = ResolveNamesResult->ResultSet.front();

            TVector<TKeyDesc::TColumnOp> columns;
            for (const auto& [_, colInfo] : entry.Columns) {
                columns.push_back(TKeyDesc::TColumnOp{ colInfo.Id, TKeyDesc::EColumnOperation::Set, colInfo.PType, 0, 0 });
            }

            FindMinMaxKeys();
            TTableRange range(MinKey.GetCells(), true, MaxKey.GetCells(), true, false);
            auto keyRange = MakeHolder<TKeyDesc>(entry.TableId, range, TKeyDesc::ERowOperation::Update, KeyColumnTypes, columns);

            TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
            request->DatabaseName = Database;
            request->ResultSet.emplace_back(std::move(keyRange));

            ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvResolveKeySet(request));

            Become(&TThis::StateWaitResolveShards);
        }

        void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev, const TActorContext& ctx) {
            ResolvePartitionsResult = ev->Get()->Request;

            if (!ResolvePartitionsResult->ResultSet.empty()
                && ResolvePartitionsResult->ResultSet.front().Status == NSchemeCache::TSchemeCacheRequest::EStatus::LookupError)
            {
                return ReplyWithError(Ydb::StatusIds::UNAVAILABLE, "table lookup failed, try again later", ctx);
            }
            if (ResolvePartitionsResult->ErrorCount > 0) {
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "unknown table", ctx);
            }

            if (IsIndexImplTable && !AllowWriteToIndexImplTable) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                    "Erasing from index implementation tables is not allowed.", ctx);
            }

            MakeShardRequests(ctx);
        }

        void RetryShardRequest(ui64 shardId, TShardEraseRetryState* state, const TActorContext& ctx) {
            Y_ABORT_UNLESS(ShardRepliesLeft.contains(shardId));

            auto ev = std::make_unique<TEvDataShard::TEvEraseRowsRequest>();
            ev->Record = state->Headers;
            for (const auto& key : state->Keys) {
                ev->Record.AddKeyColumns(key.GetBuffer());
            }

            ui64 seqNo = ++state->LastOverloadSeqNo;
            ev->Record.SetOverloadSubscribe(seqNo);
            state->SentOverloadSeqNo = seqNo;

            ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.release(), shardId, true), IEventHandle::FlagTrackDelivery);
        }

        void MakeShardRequests(const TActorContext& ctx) {
            const auto* keyRange = GetKeyRange();
            Y_ABORT_UNLESS(!keyRange->GetPartitions().empty());

            TVector<TShardEraseRetryState*> eraseRetryStates(keyRange->GetPartitions().size());
            TVector<std::unique_ptr<TEvDataShard::TEvEraseRowsRequest>> shardRequests(keyRange->GetPartitions().size());

            for (const auto& key : *Keys) {
                auto it = std::lower_bound(keyRange->GetPartitions().begin(), keyRange->GetPartitions().end(), key.GetCells(),
                    [this](const auto& partition, const auto& key) {
                        const auto& range = *partition.Range;
                        const int cmp = CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                            range.IsInclusive || range.IsPoint, true, KeyColumnTypes);

                        return (cmp < 0);
                    });

                const size_t shardIdx = it - keyRange->GetPartitions().begin();

                auto* retryState = eraseRetryStates[shardIdx];
                if (!retryState) {
                    const TTabletId shardId = it->ShardId;
                    retryState = eraseRetryStates[shardIdx] = &ShardEraseRetryStates[shardId];
                }

                auto& ev = shardRequests[shardIdx];
                if (!ev) {
                    ev = std::make_unique<TEvDataShard::TEvEraseRowsRequest>();
                    ev->Record.SetTableId(keyRange->TableId.PathId.LocalPathId);
                    if (keyRange->TableId.SchemaVersion) {
                        ev->Record.SetSchemaVersion(keyRange->TableId.SchemaVersion);
                    }
                    for (const ui32 colId : KeyColumnIds) {
                        ev->Record.AddKeyColumnIds(colId);
                    }
                    if (WriteToTableShadow) {
                        ev->Record.SetWriteToTableShadow(true);
                    }
                    // Copy protobuf settings without keys
                    retryState->Headers = ev->Record;
                }

                ev->Record.AddKeyColumns(key.GetBuffer());
                retryState->Keys.push_back(key);
            }

            size_t shardRequestCount = 0;
            for (size_t idx = 0; idx < shardRequests.size(); ++idx) {
                auto& ev = shardRequests[idx];
                if (!ev) {
                    continue;
                }
                ++shardRequestCount;

                const TTabletId shardId = keyRange->GetPartitions()[idx].ShardId;

                ui64 seqNo = ++eraseRetryStates[idx]->LastOverloadSeqNo;
                ev->Record.SetOverloadSubscribe(seqNo);
                eraseRetryStates[idx]->SentOverloadSeqNo = seqNo;

                ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.release(), shardId, true), IEventHandle::FlagTrackDelivery);

                ShardRepliesLeft.insert(shardId);
            }

            YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::RPC_REQUEST, "Erase rows: sending requests to shards",
                {"selfId", ctx.SelfID},
                {"keys", Keys->size()},
                {"shards", shardRequestCount});

            Become(&TThis::StateWaitResults);

            return ReplyIfDone(ctx);
        }

        void Handle(TEvDataShard::TEvEraseRowsResponse::TPtr& ev, const TActorContext& ctx) {
            const auto& shardResponse = ev->Get()->Record;
            const ui64 shardId = shardResponse.GetTabletID();
            const auto status = shardResponse.GetStatus();

            YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::RPC_REQUEST, "Erase rows: got shard response",
                {"status", NKikimrTxDataShard::TEvEraseRowsResponse_EStatus_Name(status)},
                {"tabletId", shardId},
                {"error", shardResponse.GetErrorDescription()});

            if (status == NKikimrTxDataShard::TEvEraseRowsResponse::OK) {
                ShardEraseRetryStates.erase(shardId);
            } else {
                if (auto* state = ShardEraseRetryStates.FindPtr(shardId)) {
                    if (!shardResponse.HasOverloadSubscribed()) {
                        // Shard doesn't support overload subscriptions for this request
                        state->SentOverloadSeqNo = 0;
                    } else if (shardResponse.GetOverloadSubscribed() == state->SentOverloadSeqNo) {
                        // Wait until shard notifies us it is possible to write again
                        return;
                    }
                }

                if (!IsRetriableStatus(status) || !Backoff.HasMore()) {
                    return ReplyWithError(YdbStatusForEraseStatus(status), shardResponse.GetErrorDescription(), ctx);
                }

                ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(GetKeyRange()->TableId, TActorId()));
            }

            // Notify the cache that we are done with the pipe
            ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(shardId));

            ShardRepliesLeft.erase(shardId);

            return ReplyIfDone(ctx);
        }

        void Handle(TEvDataShard::TEvOverloadReady::TPtr& ev, const TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            const ui64 shardId = record.GetTabletID();
            const ui64 seqNo = record.GetSeqNo();

            if (auto* state = ShardEraseRetryStates.FindPtr(shardId)) {
                if (state->SentOverloadSeqNo && state->SentOverloadSeqNo == seqNo && ShardRepliesLeft.contains(shardId)) {
                    RetryShardRequest(shardId, state, ctx);
                }
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
            ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR,
                "Internal error: pipe cache is not available, the cluster might not be configured properly", ctx);
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
            ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(GetKeyRange()->TableId, TActorId()));

            if (!Backoff.HasMore()) {
                return ReplyWithError(Ydb::StatusIds::UNAVAILABLE,
                    TStringBuilder() << "Failed to connect to shard " << ev->Get()->TabletId, ctx);
            }

            ShardRepliesLeft.erase(ev->Get()->TabletId);

            return ReplyIfDone(ctx);
        }

        void DoRetry(const TActorContext& ctx) {
            ctx.Schedule(Backoff.Next(), new TEvents::TEvWakeup(RetryWakeupTag));
            Become(&TThis::StateDoRetry);
        }

        void HandleOnRetry(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
            if (ev->Get()->Tag != RetryWakeupTag) {
                return HandleTimeout(ctx);
            }

            size_t count = 0;
            for (auto& [_, v] : ShardEraseRetryStates) {
                count += v.Keys.size();
            }

            auto keys = std::make_shared<TEraseKeys>();
            keys->reserve(count);

            for (auto& [_, v] : ShardEraseRetryStates) {
                for (auto& key : v.Keys) {
                    keys->push_back(std::move(key));
                }
            }

            ShardEraseRetryStates.clear();
            Keys = std::move(keys);

            ResolveShards(ctx);
        }

        void ReplyIfDone(const TActorContext& ctx) {
            if (!ShardRepliesLeft.empty()) {
                return;
            }
            if (!ShardEraseRetryStates.empty()) {
                return DoRetry(ctx);
            }

            return ReplyWithResult(Status, ctx);
        }

        void ReplyWithError(Ydb::StatusIds::StatusCode code, const TString& errorMessage, const TActorContext& ctx) {
            if (code == Ydb::StatusIds::SUCCESS) {
                return;
            }
            Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Erase rows from table '" << Table << "': " << errorMessage));
            ReplyWithResult(code, ctx);
        }

        void ReplyWithResult(Ydb::StatusIds::StatusCode code, const TActorContext& ctx) {
            ctx.Send(Sender, new TEvTxUserProxy::TEvEraseRowsResponse(code, Issues), 0, Cookie);
            Die(ctx);
        }

        STFUNC(StateWaitResolveTable) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
                HFunc(TEvents::TEvPoison, Handle);

                default:
                    break;
            }
        }

        STFUNC(StateWaitResolveShards) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
                HFunc(TEvents::TEvPoison, Handle);

                default:
                    break;
            }
        }

        STFUNC(StateWaitResults) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvDataShard::TEvEraseRowsResponse, Handle);
                HFunc(TEvDataShard::TEvOverloadReady, Handle);
                HFunc(TEvents::TEvUndelivered, Handle);
                HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
                HFunc(TEvents::TEvPoison, Handle);

                default:
                    break;
            }
        }

        STFUNC(StateDoRetry) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvWakeup, HandleOnRetry);
                HFunc(TEvents::TEvPoison, Handle);

                default:
                    break;
            }
        }

        void Handle(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) {
            Die(ctx);
        }

    private:
        const TActorId Sender;
        const TActorId SchemeCache;
        const TActorId LeaderPipeCache;
        const TString Database;
        const TString Table;
        std::shared_ptr<const TEraseKeys> Keys;
        const bool WriteToTableShadow;
        const bool AllowWriteToPrivateTable;
        const bool AllowWriteToIndexImplTable;
        const ui64 Cookie;

        TDuration Timeout = DEFAULT_TIMEOUT;
        TInstant StartTime;
        TActorId TimeoutTimerActorId;
        TBackoff Backoff;

        std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> ResolveNamesResult;
        TAutoPtr<NSchemeCache::TSchemeCacheRequest> ResolvePartitionsResult;

        TVector<ui32> KeyColumnIds;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
        bool IsIndexImplTable = false;

        TSerializedCellVec MinKey;
        TSerializedCellVec MaxKey;

        THashSet<TTabletId> ShardRepliesLeft;
        THashMap<TTabletId, TShardEraseRetryState> ShardEraseRetryStates;

        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
    };

} // anonymous

IActor* CreateEraseRowsInternal(
    const TActorId& sender,
    const TString& database,
    const TString& table,
    std::shared_ptr<const TEraseKeys> keys,
    bool writeToTableShadow,
    bool writeToPrivateTable,
    bool writeToIndexImplTable,
    ui64 cookie,
    TBackoff backoff)
{
    return new TEraseRowsInternal(sender, database, table, std::move(keys),
        writeToTableShadow, writeToPrivateTable, writeToIndexImplTable, cookie, backoff);
}

} // namespace NTxProxy
} // namespace NKikimr
