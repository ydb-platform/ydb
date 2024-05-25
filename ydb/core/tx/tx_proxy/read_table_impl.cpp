#include "read_table_impl.h"
#include "proxy.h"

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/engine/mkql_proto.h>

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/trace.h>

namespace NKikimr {
namespace NTxProxy {


#define TXLOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_LOG(priority, stream) LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::TX_PROXY, LogPrefix << stream);


namespace {

    static constexpr TDuration SNAPSHOT_TIMEOUT = TDuration::Seconds(30);

    static constexpr TDuration MIN_RETRY_DELAY = TDuration::MilliSeconds(250);
    static constexpr TDuration MAX_RETRY_DELAY = TDuration::Seconds(2);

    static constexpr TDuration MIN_REFRESH_DELAY = TDuration::MilliSeconds(250);
    static constexpr TDuration MAX_REFRESH_DELAY = TDuration::Seconds(10);

    static constexpr ui64 MAX_SHARD_RETRIES = 20;
    static constexpr ui64 MAX_RETRIES_TO_RESOLVE = 5;
    static constexpr TDuration MAX_SHARD_RETRY_TIME = TDuration::Seconds(15);

    /**
     * Returns true if rangeBegin <= rangeEnd, i.e. there's at least one
     * potential key that is part of the intersection.
     *
     * Note that range begin/end are treated as key prefixes if they are incomplete.
     */
    bool RangeEndBeginHasIntersection(
        TConstArrayRef<NScheme::TTypeInfo> keyTypes,
        TConstArrayRef<TCell> rangeEnd, bool rangeEndInclusive,
        TConstArrayRef<TCell> rangeBegin, bool rangeBeginInclusive)
    {
        int cmp = ComparePrefixBorders(
            keyTypes,
            rangeEnd,
            rangeEndInclusive || rangeEnd.empty() ? PrefixModeRightBorderInclusive : PrefixModeRightBorderNonInclusive,
            rangeBegin,
            rangeBeginInclusive || rangeBegin.empty() ? PrefixModeLeftBorderInclusive : PrefixModeLeftBorderNonInclusive);
        Y_DBGTRACE(VERBOSE, "RangeEndBeginHasIntersection(" << keyTypes.size() << " keys, "
            << rangeEnd.size() << " end, inclusive=" << rangeEndInclusive << ", "
            << rangeBegin.size() << " begin, inclusive=" << rangeBeginInclusive << ") => " << cmp);
        return cmp >= 0;
    }

    int CompareRangeEnds(
        TConstArrayRef<NScheme::TTypeInfo> keyTypes,
        TConstArrayRef<TCell> left, bool leftInclusive,
        TConstArrayRef<TCell> right, bool rightInclusive)
    {
        return ComparePrefixBorders(
            keyTypes,
            left,
            leftInclusive || left.empty() ? PrefixModeRightBorderInclusive : PrefixModeRightBorderNonInclusive,
            right,
            rightInclusive || right.empty() ? PrefixModeRightBorderInclusive : PrefixModeRightBorderNonInclusive);
    }

    enum class EParseRangeKeyExp {
        NONE,
        TO_NULL
    };

    bool ParseRangeKey(
            const NKikimrMiniKQL::TParams& proto,
            TConstArrayRef<NScheme::TTypeInfo> keyTypes,
            const TVector<bool>& notNullTypes,
            TSerializedCellVec& buf,
            EParseRangeKeyExp exp,
            TVector<TString>& unresolvedKeys)
    {
        TVector<TCell> key;
        TVector<TString> memoryOwner;
        if (proto.HasValue()) {
            if (!proto.HasType()) {
                unresolvedKeys.push_back("No type was specified in the range key tuple");
                return false;
            }

            auto& value = proto.GetValue();
            auto& type = proto.GetType();
            TString errStr;
            bool res = NMiniKQL::CellsFromTuple(&type, value, keyTypes, notNullTypes, true, key, errStr, memoryOwner);
            if (!res) {
                unresolvedKeys.push_back("Failed to parse range key tuple: " + errStr);
                return false;
            }
        }

        switch (exp) {
            case EParseRangeKeyExp::TO_NULL:
                key.resize(keyTypes.size());
                break;
            case EParseRangeKeyExp::NONE:
                break;
        }

        buf = TSerializedCellVec(key);
        return true;
    }

    bool CheckDomainLocality(NSchemeCache::TSchemeCacheRequest& request) {
        NSchemeCache::TDomainInfo::TPtr domainInfo;

        for (const auto& entry : request.ResultSet) {
            if (TSysTables::IsSystemTable(entry.KeyDescription->TableId)) {
                continue;
            }

            Y_ABORT_UNLESS(entry.DomainInfo);

            if (!domainInfo) {
                domainInfo = entry.DomainInfo;
                continue;
            }

            if (domainInfo->DomainKey != entry.DomainInfo->DomainKey) {
                return false;
            }
        }

        return true;
    }

    ui64 SelectCoordinator(NSchemeCache::TSchemeCacheRequest& request, ui64 txId) {
        NSchemeCache::TDomainInfo::TPtr domainInfo;

        for (const auto& entry : request.ResultSet) {
            if (entry.DomainInfo) {
                domainInfo = entry.DomainInfo;
                break;
            }
        }

        if (domainInfo) {
            return domainInfo->Coordinators.Select(txId);
        }

        return 0;
    }

}


class TReadTableWorker : public TActorBootstrapped<TReadTableWorker> {
private:
    using TBase = TActorBootstrapped<TReadTableWorker>;

    struct TEvPrivate {
        enum EEv {
            EvBegin = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvRetryShard,
            EvRefreshShard,
            EvResolveShards,
        };

        struct TEvRetryShard : public TEventLocal<TEvRetryShard, EvRetryShard> {
            const ui64 ShardId;
            const ui64 SeqNo;

            TEvRetryShard(ui64 shardId, ui64 seqNo)
                : ShardId(shardId)
                , SeqNo(seqNo)
            { }
        };

        struct TEvRefreshShard : public TEventLocal<TEvRefreshShard, EvRefreshShard> {
            const ui64 ShardId;
            const ui64 SeqNo;

            TEvRefreshShard(ui64 shardId, ui64 seqNo)
                : ShardId(shardId)
                , SeqNo(seqNo)
            { }
        };

        struct TEvResolveShards : public TEventLocal<TEvResolveShards, EvResolveShards> {
            // empty
        };
    };

    enum {
        AffectedRead = 1 << 0,
        AffectedWrite = 1 << 1,
    };

    using EShardState = EReadTableWorkerShardState;
    using ESnapshotState = EReadTableWorkerSnapshotState;

    struct TShardState;
    using TShardList = TList<TShardState*>;

    struct TShardState {
        EShardState State = EShardState::Unknown;
        const ui64 ShardId;

        TShardList::iterator ShardPosition;
        TShardState* Parent = nullptr;

        // Ranges we want to read from this shard (usually one)
        TList<TSerializedTableRange> Ranges;

        ui64 MinStep = 0;
        ui64 MaxStep = 0;
        ui32 AffectedFlags = 0;
        bool Retriable = true;

        ui64 ReadTxId = 0;
        ui64 ExpectedSeqNo = 1;

        TActorId ClearanceActor;
        ui64 ClearanceCookie = 0;

        TActorId QuotaActor;
        size_t QuotaRequests = 0;
        ui64 QuotaReserved = 0;

        ui64 Retries = 0;
        ui64 RetrySeqNo = 0;
        TActorId RetryTimer;
        TInstant RetryingSince;
        TDuration LastRetryDelay;
        bool AllowInstantRetry = true;

        ESnapshotState SnapshotState = ESnapshotState::Unknown;
        ui64 RefreshSeqNo = 0;
        TActorId RefreshTimer;
        TDuration LastRefreshDelay;

        explicit TShardState(ui64 shardId)
            : ShardId(shardId)
        { }

        TDuration SelectNextRetryDelay() {
            if (LastRetryDelay) {
                LastRetryDelay = Min(LastRetryDelay * 2, MAX_RETRY_DELAY);
            } else {
                LastRetryDelay = MIN_RETRY_DELAY;
            }
            return LastRetryDelay;
        }

        TDuration SelectNextRefreshDelay() {
            if (LastRefreshDelay) {
                LastRefreshDelay = Min(LastRefreshDelay * 2, MAX_REFRESH_DELAY);
            } else {
                LastRefreshDelay = MIN_REFRESH_DELAY;
            }
            return LastRefreshDelay;
        }
    };

    using TShardMap = TMap<ui64, TShardState>;

    struct TQuotaState {
        ui64 Reserved = 0;
        ui64 Allocated = 0;
        ui64 MessageSize = 0;
        ui64 MessageRows = 0;
    };

public:
    TReadTableWorker(const TReadTableSettings& settings)
        : Settings(settings)
    { }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_REQ_PROXY;
    }

    void Bootstrap(const TActorContext& ctx) {
        LogPrefix = TStringBuilder() << "[ReadTable " << SelfId() << "] ";

        Columns.resize(Settings.Columns.size());
        for (size_t i = 0; i < Settings.Columns.size(); ++i) {
            Columns[i].Name = Settings.Columns[i];
        }

        WallClockAccepted = Now();

        if (!Settings.UserToken.empty()) {
            UserToken = MakeHolder<NACLib::TUserToken>(Settings.UserToken);
        }

        if (Settings.MaxRows > 0) {
            RemainingRows = Settings.MaxRows;
        }

        SendAllocateInitialTxId(ctx);
    }

private:
    void SendCancelSnapshotProposal(TShardState& state, const TActorContext& ctx) {
        ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
            new TEvDataShard::TEvCancelTransactionProposal(TxId),
            state.ShardId, false));
    }

    void SendInterruptReadTable(TShardState& state, const TActorContext& ctx) {
        // We send TEvCancelTransactionProposal for cases where datashard
        // decided to prepare our immediate read table transaction.
        ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
            new TEvDataShard::TEvCancelTransactionProposal(state.ReadTxId),
            state.ShardId, false));
        ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
            new TEvTxProcessing::TEvInterruptTransaction(state.ReadTxId),
            state.ShardId, false));
    }

    void CancelActiveShard(TShardState& state, const TActorContext& ctx) {
        if (state.State == EShardState::SnapshotProposeSent ||
            state.State == EShardState::SnapshotPrepared)
        {
            // Proposal is not planned yet, attempt to cancel
            // Usually all shards will be in this state
            SendCancelSnapshotProposal(state, ctx);
            return;
        }

        if (state.State == EShardState::ReadTableNeedTxId) {
            state.State = EShardState::Finished;
            return;
        }

        if (state.State == EShardState::ReadTableProposeSent ||
            state.State == EShardState::ReadTableClearancePending ||
            state.State == EShardState::ReadTableStreaming ||
            state.State == EShardState::ReadTableNeedRetry)
        {
            // This shard is no longer active, remove it from any active sets
            ClearancePendingShards.erase(state.ShardId);
            StreamingShards.erase(state.ShardId);
            DiscardShardQuota(state, ctx);

            // Interrupt any active ReadTable transactions (best effort, no retries)
            SendInterruptReadTable(state, ctx);

            if (state.RetryTimer) {
                Send(state.RetryTimer, new TEvents::TEvPoison);
                state.RetryTimer = { };
            }

            state.State = EShardState::Finished;
        }
    }

    void Die(const TActorContext& ctx) override {
        if (TxId == 0) {
            // We are not fully initialized yet
            Y_ABORT_UNLESS(!ResolveInProgress);
            Y_ABORT_UNLESS(!ShardMap);
            return TBase::Die(ctx);
        }

        --*TxProxyMon->DataReqInFly;

        for (auto& [shardId, state] : ShardMap) {
            CancelActiveShard(state, ctx);

            if (state.RefreshTimer) {
                Send(state.RefreshTimer, new TEvents::TEvPoison);
                state.RefreshTimer = { };
            }

            if (PlanStep != 0) {
                // If we have a known PlanStep we know snapshot Step/TxId
                // Attempt to discard it (best effort, no retries or waiting for replies)
                auto req = MakeHolder<TEvDataShard::TEvDiscardVolatileSnapshotRequest>();
                req->Record.SetOwnerId(TableId.PathId.OwnerId);
                req->Record.SetPathId(TableId.PathId.LocalPathId);
                req->Record.SetStep(PlanStep);
                req->Record.SetTxId(TxId);
                Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                    req.Release(), shardId, false));
            }
        }

        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        Send(Services.FollowerPipeCache, new TEvPipeCache::TEvUnlink(0));

        if (ResolveInProgress) {
            // Actor cannot die until it receives a reply
            Become(&TThis::StateZombie);
            return;
        }

        TBase::Die(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        Die(ctx);
    }

private:
    STFUNC(StateInitial) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleInitial);
        }
    }

    void SendAllocateInitialTxId(const TActorContext& ctx) {
        TXLOG_D("Allocating TxId");
        ctx.Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);

        Become(&TThis::StateInitial);
    }

    void HandleInitial(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();

        TxId = msg->TxId;
        Services = msg->Services;
        TxProxyMon = msg->TxProxyMon;
        LogPrefix = TStringBuilder() << "[ReadTable " << SelfId() << " TxId# " << TxId << "] ";

        WallClockAllocated = Now();

        TXLOG_D("Allocated initial TxId# " << TxId);

        // We increment the MakeRequest counter for compatibility with existing transaction dashboards
        ++*TxProxyMon->DataReqInFly;
        TxProxyMon->MakeRequest->Inc();
        TxProxyMon->MakeRequestProxyAccepted->Inc();

        SendNavigateKeySet(ctx);
    }

    void HandleExecTimeout(const TActorContext& ctx) {
        TXLOG_T("HandleExecTimeout");
        TxProxyMon->ExecTimeout->Inc();
        return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout, NKikimrIssues::TStatusIds::TIMEOUT, ctx);
    }

    STFUNC(StateWaitNavigate) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
        }
    }

    void SendNavigateKeySet(const TActorContext& ctx) {
        // Send the navigate request to find out table schema
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Settings.DatabaseName;
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(Settings.TablePath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.ShowPrivatePath = true;
        entry.SyncVersion = true;

        TXLOG_D("Sending TEvNagivateKeySet for table '" << Settings.TablePath << "'");
        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        Become(&TThis::StateWaitNavigate);
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

        TXLOG_D("Received TEvNavigateKeySetResult for table '" << Settings.TablePath << "'");

        if (resp->ErrorCount > 0) {
            TXLOG_E("Navigate request failed for table '" << Settings.TablePath << "'");
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
            TString error = TStringBuilder() << "Failed to resolve table " << Settings.TablePath;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
            UnresolvedKeys.emplace_back(error);
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
        }

        THashMap<TString, size_t> colNameToPos;
        for (size_t i = 0; i < Columns.size(); ++i) {
            auto& col = Columns[i];
            if (colNameToPos.contains(col.Name)) {
                TxProxyMon->ResolveKeySetWrongRequest->Inc();
                TString error = TStringBuilder() << "Duplicate column requested: '" << col.Name << "'";
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
                UnresolvedKeys.emplace_back(error);
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
            }
            colNameToPos[col.Name] = i;
        }

        // When true we don't want to select all columns
        const bool projection = bool(colNameToPos);

        auto& res = resp->ResultSet[0];
        TableId = res.TableId;
        DomainInfo = res.DomainInfo;
        Y_ABORT_UNLESS(DomainInfo, "Missing DomainInfo in TEvNavigateKeySetResult");

        if (TableId.IsSystemView() ||
            TSysTables::IsSystemTable(TableId))
        {
            TString error = TStringBuilder()
                << "Cannot read system table '" << Settings.TablePath << "', tableId# " << TableId;
            TXLOG_E(error);
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
            UnresolvedKeys.emplace_back(error);
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
        }

        TVector<NScheme::TTypeInfo> keyTypes(res.Columns.size());
        TVector<bool> notNullKeys(res.Columns.size());
        TVector<TKeyDesc::TColumnOp> columns(res.Columns.size());
        {
            size_t no = 0;
            size_t keys = 0;

            for (auto &entry : res.Columns) {
                auto& col = entry.second;

                if (col.KeyOrder != -1) {
                    keyTypes[col.KeyOrder] = col.PType;
                    notNullKeys[col.KeyOrder] = col.IsNotNullColumn;
                    ++keys;
                }

                columns[no].Column = col.Id;
                columns[no].Operation = TKeyDesc::EColumnOperation::Read;
                columns[no].ExpectedType = col.PType;
                ++no;

                if (projection) {
                    auto it = colNameToPos.find(col.Name);
                    if (it != colNameToPos.end()) {
                        Columns[it->second] = col;
                        colNameToPos.erase(col.Name);
                    }
                } else {
                    Columns.emplace_back(col);
                }
            }

            keyTypes.resize(keys);
            notNullKeys.resize(keys);
        }

        if (!colNameToPos.empty()) {
            TxProxyMon->ResolveKeySetWrongRequest->Inc();

            TVector<TString> missingColumns(Reserve(colNameToPos.size()));
            for (auto& kv : colNameToPos) {
                missingColumns.emplace_back(kv.first);
            }
            std::sort(missingColumns.begin(), missingColumns.end());

            for (auto& name : missingColumns) {
                TString error = TStringBuilder() << "Unresolved column: '" << name << "'";
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
                UnresolvedKeys.emplace_back(error);
            }

            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
        }

        bool fromInclusive = Settings.KeyRange.GetFromInclusive();
        bool toInclusive = Settings.KeyRange.GetToInclusive();
        const EParseRangeKeyExp fromExpand = (
            Settings.KeyRange.HasFrom()
                ? (fromInclusive ? EParseRangeKeyExp::TO_NULL : EParseRangeKeyExp::NONE)
                : EParseRangeKeyExp::TO_NULL);
        const EParseRangeKeyExp toExpand = (
            Settings.KeyRange.HasTo()
                ? (toInclusive ? EParseRangeKeyExp::NONE : EParseRangeKeyExp::TO_NULL)
                : EParseRangeKeyExp::NONE);

        if (!ParseRangeKey(Settings.KeyRange.GetFrom(), keyTypes, notNullKeys,
                        KeyFromValues, fromExpand, UnresolvedKeys) ||
            !ParseRangeKey(Settings.KeyRange.GetTo(), keyTypes, notNullKeys,
                        KeyToValues, toExpand, UnresolvedKeys))
        {
            TxProxyMon->ResolveKeySetWrongRequest->Inc();

            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR, "Failed to parse key ranges"));

            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::QUERY_ERROR, ctx);
        }

        if (KeyFromValues.GetCells().size() < keyTypes.size() && !Settings.KeyRange.HasFromInclusive()) {
            // Default: non-inclusive for incomplete From, except when From is empty
            fromInclusive = KeyFromValues.GetCells().size() == 0;
        }

        if (KeyToValues.GetCells().size() < keyTypes.size() && !Settings.KeyRange.HasToInclusive()) {
            // Default: inclusive for incomplete To
            toInclusive = true;
        }

        TTableRange range(
                KeyFromValues.GetCells(), fromInclusive,
                KeyToValues.GetCells(), toInclusive);

        if (range.IsEmptyRange({keyTypes.begin(), keyTypes.end()})) {
            TxProxyMon->ResolveKeySetWrongRequest->Inc();

            TString error = "Empty range requested";
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::EMPTY_OP_RANGE, error));
            UnresolvedKeys.push_back(error);

            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::QUERY_ERROR, ctx);
        }

        KeyDesc = MakeHolder<TKeyDesc>(res.TableId, range, TKeyDesc::ERowOperation::Read, keyTypes, columns);

        SendResolveKeySet(ctx);
    }

    STFUNC(StateWaitResolve) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
        }
    }

    void SendResolveKeySet(const TActorContext& ctx) {
        Y_ABORT_UNLESS(!ResolveInProgress, "Only one resolve request may be active at a time");

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DomainOwnerId = DomainInfo->ExtractSchemeShard();
        request->ResultSet.emplace_back(std::move(KeyDesc));

        TXLOG_D("Sending TEvResolveKeySet for table '" << Settings.TablePath << "'");
        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        ResolveInProgress = true;

        WallClockResolveStarted = Now();

        Become(&TThis::StateWaitResolve);
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveInProgress, "Received TEvResolveKeySetResult without an active request");
        ResolveInProgress = false;

        WallClockResolved = Now();

        TXLOG_D("Received TEvResolveKeySetResult for table '" << Settings.TablePath << "'");

        TxProxyMon->CacheRequestLatency->Collect((WallClockResolved - WallClockAllocated).MilliSeconds());

        auto* request = ev->Get()->Request.Get();
        if (request->ErrorCount > 0) {
            TXLOG_E("Resolve request failed for table '" << Settings.TablePath << "', ErrorCount# " << request->ErrorCount);

            bool gotHardResolveError = false;
            for (const auto& x : request->ResultSet) {
                if ((ui32)x.Status < (ui32) NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                    TryToInvalidateTable(TableId, ctx);

                    TString error;
                    switch (x.Status) {
                        case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                            gotHardResolveError = true;
                            error = TStringBuilder() << "table not exists: " << x.KeyDescription->TableId;
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                            gotHardResolveError = true;
                            error = TStringBuilder() << "type check error: " << x.KeyDescription->TableId;
                            break;
                        default:
                            error = TStringBuilder() << "unresolved table: " << x.KeyDescription->TableId << ". Status: " << x.Status;
                            break;
                    }

                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
                    UnresolvedKeys.push_back(error);
                }
            }

            if (gotHardResolveError) {
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
            } else {
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, ctx);
            }
        }

        if (Settings.ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportResolved) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
        }

        TxProxyMon->TxPrepareResolveHgram->Collect((WallClockResolved - WallClockResolveStarted).MicroSeconds());

        for (const auto& entry : request->ResultSet) {
            ui32 access = 0;

            // N.B. it's always a Read
            switch (entry.KeyDescription->RowOperation) {
            case TKeyDesc::ERowOperation::Update:
                access |= NACLib::EAccessRights::UpdateRow;
                break;
            case TKeyDesc::ERowOperation::Read:
                access |= NACLib::EAccessRights::SelectRow;
                break;
            case TKeyDesc::ERowOperation::Erase:
                access |= NACLib::EAccessRights::EraseRow;
                break;
            default:
                break;
            }

            if (access != 0
                    && UserToken != nullptr
                    && entry.KeyDescription->Status == TKeyDesc::EStatus::Ok
                    && entry.KeyDescription->SecurityObject != nullptr
                    && !entry.KeyDescription->SecurityObject->CheckAccess(access, *UserToken))
            {
                TString error = TStringBuilder()
                    << "Access denied for " << UserToken->GetUserSID()
                    << " with access " << NACLib::AccessRightsToString(access)
                    << " to tableId# " << entry.KeyDescription->TableId;

                TXLOG_E(error);
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error));
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, NKikimrIssues::TStatusIds::ACCESS_DENIED, ctx);
            }
        }

        if (!CheckDomainLocality(*request)) {
            TxProxyMon->ResolveKeySetDomainLocalityFail->Inc();
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DOMAIN_LOCALITY_ERROR));
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError, NKikimrIssues::TStatusIds::BAD_REQUEST, ctx);
        }

        KeyDesc = std::move(request->ResultSet[0].KeyDescription);

        if (KeyDesc->GetPartitions().empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << Settings.TablePath << "'";
            TXLOG_E(error);
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, NKikimrIssues::TStatusIds::BAD_REQUEST, ctx);
        }

        SelectedCoordinator = SelectCoordinator(*request, TxId);

        if (!SelectedCoordinator) {
            TXLOG_D("SelectCoordinator was unable to choose coordinator from resolved keys, will use propose results");
        }

        // Do we need to create a new snapshot?
        const bool needSnapshot = Settings.ReadVersion.IsMax();

        for (size_t idx = 0; idx < KeyDesc->GetPartitions().size(); ++idx) {
            const auto& partition = KeyDesc->GetPartitions()[idx];
            const ui64 shardId = partition.ShardId;

            auto [it, inserted] = ShardMap.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(shardId),
                std::forward_as_tuple(shardId));
            Y_DEBUG_ABORT_UNLESS(inserted, "Duplicate shard %" PRIu64 " after keys resolve", shardId);

            auto& state = it->second;
            auto& range = state.Ranges.emplace_back();

            if (idx == 0) {
                // First shard in range
                range.From = KeyFromValues;
                range.FromInclusive = KeyDesc->Range.InclusiveFrom;
            } else {
                const auto& prevRange = *KeyDesc->GetPartitions()[idx - 1].Range;
                range.From = prevRange.EndKeyPrefix;
                range.FromInclusive = !prevRange.IsInclusive; // N.B. always true for now
            }

            if (idx == KeyDesc->GetPartitions().size() - 1) {
                // Last shard in range
                range.To = KeyToValues;
                range.ToInclusive = KeyDesc->Range.InclusiveTo;
            } else {
                const auto& partRange = *partition.Range;
                range.To = partRange.EndKeyPrefix;
                range.ToInclusive = partRange.IsInclusive; // N.B. always false for now
            }

            // This is an ordered list of shards
            state.ShardPosition = ShardList.insert(ShardList.end(), &state);

            if (needSnapshot) {
                SendSnapshotPrepare(state, ctx);
            } else {
                SendReadTablePropose(state, ctx);
            }
        }

        if (needSnapshot) {
            TXLOG_T("Waiting for snapshot to prepare");
            Become(&TThis::StateWaitPrepare);
        } else {
            Become(&TThis::StateReadTable);
        }
    }

    STFUNC(StateWaitPrepare) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvDataShard::TEvGetReadTableSinkStateRequest, Handle);
            HFunc(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
            HFunc(TEvDataShard::TEvProposeTransactionResult, HandlePrepare);
            HFunc(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
        }
    }

    void SendSnapshotPrepare(TShardState& state, const TActorContext& ctx) {
        NKikimrTxDataShard::TSnapshotTransaction tx;
        auto* op = tx.MutableCreateVolatileSnapshot();
        op->SetOwnerId(TableId.PathId.OwnerId);
        op->SetPathId(TableId.PathId.LocalPathId);
        op->SetTimeoutMs(SNAPSHOT_TIMEOUT.MilliSeconds());

        const TString txBody = tx.SerializeAsString();
        const ui64 txFlags = 0;

        TXLOG_D("Sending CreateVolatileSnapshot tx to shard " << state.ShardId);
        ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_SNAPSHOT,
                    ctx.SelfID, TxId, txBody, txFlags),
                state.ShardId, true));

        state.AffectedFlags |= AffectedRead;
        state.State = EShardState::SnapshotProposeSent;
        ++TabletsToPrepare;
    }

    void RaiseShardOverloaded(const NKikimrTxDataShard::TEvProposeTransactionResult& record, ui64 shardId) {
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, TStringBuilder()
            << "Shard " << shardId << " is overloaded");
        for (const auto& err : record.GetError()) {
            issue.AddSubIssue(new NYql::TIssue(TStringBuilder()
                << "[" << err.GetKind() << "] " << err.GetReason()));
        }
        IssueManager.RaiseIssue(std::move(issue));
    }

    void HandlePrepare(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;
        const ui64 shardId = msg->GetOrigin();

        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
                "Received TEvProposeTransactionResult from unexpected shardId# " << shardId);

        auto& state = it->second;
        if (state.State != EShardState::SnapshotProposeSent) {
            // Ignore unexpected messages
            TXLOG_W("Unexpected TEvProposeTransactionResult from shardId# " << shardId
                    << " state# " << state.State);
            return;
        }

        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status;
        NKikimrIssues::TStatusIds::EStatusCode code;

        const auto shardStatus = msg->GetStatus();
        TXLOG_D("Received " << NKikimrTxDataShard::TEvProposeTransactionResult::EStatus_Name(shardStatus)
                << " for CreateVolatileSnapshot from shard " << shardId);

        switch (shardStatus) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED: {
                state.State = EShardState::SnapshotPrepared;
                state.MinStep = record.GetMinStep();
                state.MaxStep = record.GetMaxStep();

                AggrMinStep = Max(AggrMinStep, state.MinStep);
                AggrMaxStep = Min(AggrMaxStep, state.MaxStep);

                if (record.HasExecLatency())
                    ElapsedPrepareExec = Max<TDuration>(ElapsedPrepareExec, TDuration::MilliSeconds(record.GetExecLatency()));
                if (record.HasProposeLatency())
                    ElapsedPrepareComplete = Max<TDuration>(ElapsedPrepareComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

                TxProxyMon->TxResultPrepared->Inc();

                const TVector<ui64> privateCoordinators(
                        record.GetDomainCoordinators().begin(),
                        record.GetDomainCoordinators().end());
                const ui64 privateCoordinator = TCoordinators(privateCoordinators)
                        .Select(TxId);

                if (!SelectedCoordinator) {
                    SelectedCoordinator = privateCoordinator;
                }

                if (!SelectedCoordinator || SelectedCoordinator != privateCoordinator) {
                    CancelProposal();

                    TxProxyMon->TxResultAborted->Inc();

                    const TString error = TStringBuilder()
                        << "Unable to choose coordinator"
                        << " from shard " << shardId
                        << " txId# " << TxId;

                    TXLOG_E("HANDLE SnapshotPrepare: "
                        << error
                        << ", coordinator selected at resolve keys state: " << SelectedCoordinator
                        << ", coordinator selected at propose result state: " << privateCoordinator);

                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR, error));
                    auto errorCode = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError;
                    if (SelectedCoordinator == 0) {
                        errorCode = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown;
                    }

                    return ReplyAndDie(errorCode, NKikimrIssues::TStatusIds::INTERNAL_ERROR, ctx);
                }

                if (--TabletsToPrepare) {
                    return;
                }

                return RegisterPlan(ctx);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE: {
                state.State = EShardState::Finished;

                CancelProposal(shardId);

                TxProxyMon->TxResultComplete->Inc();

                const TString error = TStringBuilder()
                    << "Unexpected COMPLETE result from shard " << shardId << " txId# " << TxId;
                TXLOG_E(error);
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, error));
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError,
                        NKikimrIssues::TStatusIds::INTERNAL_ERROR, ctx);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR: {
                state.State = EShardState::Error;
                TxProxyMon->TxResultError->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable;
                code = NKikimrIssues::TStatusIds::REJECTED;
                ExtractDatashardErrors(record);
                CancelProposal(shardId);
                ReportStatus(status, code, true, ctx);
                // Try to gather some more errors (logging and counters)
                Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
                return HandlePrepareErrors(ev, ctx);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
                TxProxyMon->TxResultAborted->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted;
                code = NKikimrIssues::TStatusIds::SUCCESS; // FIXME: ???
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER:
                TxProxyMon->TxResultShardTryLater->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater;
                code = NKikimrIssues::TStatusIds::REJECTED;
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
                TxProxyMon->TxResultShardOverloaded->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded;
                code = NKikimrIssues::TStatusIds::OVERLOADED;
                RaiseShardOverloaded(record, shardId);
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR:
                TxProxyMon->TxResultExecError->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
                TxProxyMon->TxResultResultUnavailable->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
                TxProxyMon->TxResultCancelled->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST:
                TxProxyMon->TxResultCancelled->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest;
                code = NKikimrIssues::TStatusIds::BAD_REQUEST;
                break;
            default:
                // everything other is hard error
                TxProxyMon->TxResultFatal->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
        }

        state.State = EShardState::Error;
        ExtractDatashardErrors(record);
        CancelProposal(shardId);
        return ReplyAndDie(status, code, ctx);
    }

    void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->TabletId;
        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
            "Received TEvDeliveryProblem from unexpected shard " << shardId);
        auto& state = it->second;

        if (state.State != EShardState::SnapshotProposeSent) {
            return;
        }

        TXLOG_D("Delivery problem during CreateVolatileSnapshot at shard " << shardId);

        TxProxyMon->ClientConnectedError->Inc();
        ComplainingDatashards.push_back(shardId);
        CancelProposal(shardId);

        if (msg->NotDelivered) {
            const TString explanation = TStringBuilder()
                << "could not deliver program to shard " << shardId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, explanation));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        } else {
            const TString explanation = TStringBuilder()
                << "tx state unknown for shard " << shardId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
        }

        Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
        return HandlePrepareErrors(ev, ctx);
    }

    STFUNC(StatePrepareErrors) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvDataShard::TEvProposeTransactionResult, HandlePrepareErrors);
            HFunc(TEvPipeCache::TEvDeliveryProblem, HandlePrepareErrors);
            CFunc(TEvents::TSystem::Wakeup, HandlePrepareErrorsTimeout);
        }
    }

    void CancelProposal(ui64 exceptShard = 0) {
        for (const auto& kv : ShardMap) {
            ui64 shardId = kv.first;
            const auto& state = kv.second;
            if (shardId != exceptShard && (
                    state.State == EShardState::SnapshotProposeSent ||
                    state.State == EShardState::SnapshotPrepared))
            {
                TXLOG_T("Sending TEvCancelTransactionProposal to shard " << shardId);
                Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvCancelTransactionProposal(TxId),
                    shardId, false));
            }
        }
    }

    void ExtractDatashardErrors(const NKikimrTxDataShard::TEvProposeTransactionResult& record) {
        for (const auto &er : record.GetError()) {
            DatashardErrors << "[" << er.GetKind() << "] " << er.GetReason() << Endl;
        }

        ComplainingDatashards.push_back(record.GetOrigin());
    }

    void HandlePrepareErrors(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->GetOrigin();

        const bool isExpected = msg->GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::ERROR;
        TXLOG_LOG(isExpected ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN,
                "Received TEvProposeTransactionResult (waiting for prepare errors)"
                << " ShardId# " << shardId
                << " Status# " << msg->GetStatus());

        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
            "Received TEvProposeTransactionResult from unexpected shard " << shardId);

        auto& state = it->second;
        if (state.State != EShardState::SnapshotProposeSent) {
            return;
        }

        MarkShardPrepareError(shardId, state, NeedInvalidateDistCache(msg), ctx);
    }

    bool NeedInvalidateDistCache(const TEvDataShard::TEvProposeTransactionResult* msg) {
        switch (msg->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
                for (const auto& er : msg->Record.GetError()) {
                    const NKikimrTxDataShard::TError::EKind errorKind = er.GetKind();
                    switch (errorKind) {
                        case NKikimrTxDataShard::TError::SCHEME_ERROR:
                        case NKikimrTxDataShard::TError::WRONG_PAYLOAD_TYPE:
                        case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
                        case NKikimrTxDataShard::TError::SCHEME_CHANGED:
                            return true;
                        default:
                            break;
                    }
                }
                break;
            default:
                break;
        }

        return false;
    }

    void HandlePrepareErrors(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->TabletId;
        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
            "Received TEvDeliveryProblem from unexpected shard " << shardId);

        TXLOG_E("PrepareErrors delivery problem ShardId# " << shardId);

        auto& state = it->second;
        if (state.State != EShardState::SnapshotProposeSent) {
            return;
        }

        MarkShardPrepareError(shardId, state, true, ctx);
    }

    void MarkShardPrepareError(ui64 shardId, TShardState& state, bool invalidateDistCache, const TActorContext& ctx) {
        if (state.State != EShardState::SnapshotProposeSent) {
            return;
        }

        state.State = EShardState::Error;

        if (invalidateDistCache) {
            TryToInvalidateTable(TableId, ctx);
        }

        Y_UNUSED(shardId);

        ++TabletPrepareErrors;
        Y_DEBUG_ABORT_UNLESS(TabletsToPrepare > 0);
        if (!--TabletsToPrepare) {
            TxProxyMon->MarkShardError->Inc();
            TXLOG_E("Gathered all snapshot propose results, TabletPrepareErrors# " << TabletPrepareErrors);
            return Die(ctx);
        }
    }

    void HandlePrepareErrorsTimeout(const TActorContext& ctx) {
        TxProxyMon->PrepareErrorTimeout->Inc();
        // Unlike ExecTimeout we don't send any replies here
        return Die(ctx);
    }

    STFUNC(StateReadTable) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvDataShard::TEvGetReadTableSinkStateRequest, Handle);
            HFunc(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
            HFunc(TEvTxProxy::TEvProposeTransactionStatus, HandleSnapshotPlan);
            HFunc(TEvDataShard::TEvProposeTransactionResult, HandleReadTable);
            HFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleReadTable);
            HFunc(TEvTxProcessing::TEvStreamClearanceRequest, HandleReadTable);
            HFunc(TEvTxProcessing::TEvStreamQuotaRequest, HandleReadTable);
            HFunc(TEvTxProcessing::TEvStreamQuotaResponse, HandleReadTable);
            HFunc(TEvTxProcessing::TEvStreamQuotaRelease, HandleReadTable);
            HFunc(TEvPipeCache::TEvDeliveryProblem, HandleReadTable);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
            HFunc(TEvPrivate::TEvRetryShard, HandleRetryShard);
            HFunc(TEvPrivate::TEvRefreshShard, HandleRefreshShard);
            HFunc(TEvDataShard::TEvRefreshVolatileSnapshotResponse, HandleReadTable);
            CFunc(TEvPrivate::EvResolveShards, HandleResolveShards);
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleReadTable);
        }
    }

    void RegisterPlan(const TActorContext& ctx) {
        WallClockPrepared = Now();

        if (Settings.ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportPrepared) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyPrepared, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
        }

        Y_ABORT_UNLESS(SelectedCoordinator, "Unexpected null SelectedCoordinator");

        auto req = MakeHolder<TEvTxProxy::TEvProposeTransaction>(
            SelectedCoordinator, TxId, 0, AggrMinStep, AggrMaxStep);

        auto* reqAffectedSet = req->Record.MutableTransaction()->MutableAffectedSet();
        reqAffectedSet->Reserve(ShardMap.size());

        for (auto& kv : ShardMap) {
            ui64 shardId = kv.first;
            auto& state = kv.second;

            Y_VERIFY_S(state.State == EShardState::SnapshotPrepared,
                "Unexpected ShardId# " << shardId << " State# " << state.State);
            state.State = EShardState::SnapshotPlanned;

            auto* x = reqAffectedSet->Add();
            x->SetTabletId(shardId);
            x->SetFlags(state.AffectedFlags);
        }

        TXLOG_D("Sending EvProposeTransaction to SelectedCoordinator# " << SelectedCoordinator);

        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(req.Release(), SelectedCoordinator, true));
        Become(&TThis::StateReadTable);
    }

    // Received from coordinator
    void HandleSnapshotPlan(TEvTxProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        switch (msg->GetStatus()) {
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
                // no-op
                TXLOG_D("Received TEvProposeTransactionStatus Status# StatusAccepted");
                TxProxyMon->ClientTxStatusAccepted->Inc();
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
                // no-op
                TXLOG_D("Received TEvProposeTransactionStatus Status# StatusProcessed");
                TxProxyMon->ClientTxStatusProcessed->Inc();
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
                // no-op
                TXLOG_D("Received TEvProposeTransactionStatus Status# StatusConfirmed");
                TxProxyMon->ClientTxStatusConfirmed->Inc();
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                // ok
                TXLOG_D("Received TEvProposeTransactionStatus Status# StatusPlanned");
                if (!PlanStep) {
                    PlanStep = record.GetStepId();
                } else {
                    Y_VERIFY_S(PlanStep == record.GetStepId(),
                        "PlanStep diverged, current = " << PlanStep << " coordinator = " << record.GetStepId());
                }
                TxProxyMon->ClientTxStatusPlanned->Inc();
                WallClockPlanned = Now();
                if (Settings.ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportPlanned)
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorPlanned, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting: // TODO: retry
                // cancel proposal only for defined cases
                CancelProposal(0);

                // fall through to generic error handling
                [[fallthrough]];

            default:
                // something went wrong
                TXLOG_E("Received TEvProposeTransactionStatus Status# " << msg->GetStatus());
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::REJECTED, ctx);
        }
    }

    // Received from each shard
    void HandleSnapshotResult(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->GetOrigin();

        const bool isExpected = (
            msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE ||
            msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED);
        TXLOG_LOG(isExpected ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_WARN,
                "Received TEvProposeTransactionResult (snapshot tx)"
                << " ShardId# " << shardId
                << " Status# " << msg->GetStatus());

        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
                "Received TEvProposeTransactionResult from unexpected shardId# " << shardId);
        auto& state = it->second;

        Y_DEBUG_ABORT_UNLESS(state.State == EShardState::SnapshotPlanned);

        if (msg->GetTxId() != TxId) {
            TXLOG_E("Unexpected TEvProposeTransactionResult (snapshot tx) TxId# " << msg->GetTxId() << " expected " << TxId);
            return;
        }

        const auto& record = msg->Record;

        if (!PlanStep) {
            PlanStep = record.GetStep();
        }

        if (record.HasExecLatency())
            ElapsedExecExec = Max<TDuration>(ElapsedExecExec, TDuration::MilliSeconds(record.GetExecLatency()));
        if (record.HasProposeLatency())
            ElapsedExecComplete = Max<TDuration>(ElapsedExecComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status;
        NKikimrIssues::TStatusIds::EStatusCode code;

        switch (msg->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE: {
                TxProxyMon->PlanClientTxResultComplete->Inc();

                state.SnapshotState = ESnapshotState::Confirmed;
                state.RefreshTimer = CreateLongTimer(
                    ctx,
                    SNAPSHOT_TIMEOUT / 2,
                    new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvPrivate::TEvRefreshShard(shardId, ++state.RefreshSeqNo)));

                if (state.ShardPosition == ShardList.end()) {
                    // We don't want to read from this shard at this time
                    state.State = EShardState::Finished;
                    return;
                }

                state.State = EShardState::ReadTableNeedTxId;
                return SendReadTablePropose(state, ctx);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
                TxProxyMon->PlanClientTxResultAborted->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted;
                code = NKikimrIssues::TStatusIds::SUCCESS; // FIXME: ???
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
                TxProxyMon->PlanClientTxResultResultUnavailable->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
                TxProxyMon->PlanClientTxResultCancelled->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
            default:
                TxProxyMon->PlanClientTxResultExecError->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
        }

        ExtractDatashardErrors(record);
        return ReplyAndDie(status, code, ctx);
    }

    void HandleReadTable(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev, const TActorContext& ctx) {
        TXLOG_D("Allocated a new ReadTxId# " << ev->Get()->TxId);

        Y_ABORT_UNLESS(AllocatingReadTxId);
        CurrentReadTxId = ev->Get()->TxId;
        AllocatingReadTxId = false;

        // Check for shards that have been waiting for this tx id
        for (auto& kv : ShardMap) {
            auto& state = kv.second;
            if (state.State == EShardState::ReadTableNeedTxId) {
                SendReadTablePropose(state, ctx);
                // Sanity check, this new tx id must have never been used by this shard before
                Y_ABORT_UNLESS(state.State == EShardState::ReadTableProposeSent);
            }
        }
    }

    void SendReadTablePropose(TShardState& state, const TActorContext& ctx) {
        const ui64 shardId = state.ShardId;

        Y_VERIFY_S(state.State == EShardState::ReadTableNeedTxId,
            "Unexpected ShardId# " << shardId << " State# " << state.State);
        Y_VERIFY_S(state.ShardPosition != ShardList.end(),
            "Unexpected ShardId# " << shardId << " State# " << state.State << " not in current shard list");

        if (state.ReadTxId && state.ReadTxId == CurrentReadTxId) {
            // CurrentReadTxId cannot be reused, must allocate a new one
            Y_ABORT_UNLESS(!AllocatingReadTxId);
            CurrentReadTxId = 0;
        }

        if (!CurrentReadTxId) {
            // Allocate some new ReadTxId
            if (!AllocatingReadTxId) {
                TXLOG_D("Allocating a new ReadTxId for ShardId# " << shardId);
                ctx.Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
                AllocatingReadTxId = true;
            } else {
                TXLOG_D("Waiting for a new ReadTxId for ShardId# " << shardId);
            }
            return;
        }

        // Use the last allocated ReadTxId
        state.ReadTxId = CurrentReadTxId;
        state.ExpectedSeqNo = 1;

        NKikimrTxDataShard::TDataTransaction dataTransaction;
        dataTransaction.SetStreamResponse(true);
        dataTransaction.SetImmediate(true);
        dataTransaction.SetReadOnly(true);
        ActorIdToProto(SelfId(), dataTransaction.MutableSink());

        auto &tx = *dataTransaction.MutableReadTableTransaction();
        tx.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
        tx.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);

        switch (Settings.DataFormat) {
            case EReadTableFormat::OldResultSet:
                tx.SetApiVersion(NKikimrTxUserProxy::TReadTableTransaction::UNSPECIFIED);
                break;
            case EReadTableFormat::YdbResultSet:
                tx.SetApiVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
                break;
        }

        for (auto &col : Columns) {
            auto &c = *tx.AddColumns();
            c.SetId(col.Id);
            c.SetName(col.Name);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(col.PType, col.PTypeMod);
            c.SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *c.MutableTypeInfo() = *columnType.TypeInfo;
            }
        }

        auto& txRange = *tx.MutableRange();
        state.Ranges.front().Serialize(txRange);

        // Normalize range's From/ToInclusive
        if (txRange.GetFrom().empty() && !txRange.GetFromInclusive()) {
            txRange.SetFromInclusive(true);
        }
        if (txRange.GetTo().empty() && !txRange.GetToInclusive()) {
            txRange.SetToInclusive(true);
        }

        if (Settings.ReadVersion.IsMax()) {
            // Use snapshot that we have created
            tx.SetSnapshotStep(PlanStep);
            tx.SetSnapshotTxId(TxId);
        } else {
            // Use snapshot provided externally
            tx.SetSnapshotStep(Settings.ReadVersion.Step);
            tx.SetSnapshotTxId(Settings.ReadVersion.TxId);
        }

        const TString txBody = dataTransaction.SerializeAsString();
        const ui64 txFlags = NTxDataShard::TTxFlags::Immediate;

        TxProxyMon->ReadTableResolveSentToShard->Inc();
        TXLOG_D("Sending TEvProposeTransaction (scan) to shard " << shardId << " ReadTxId# " << state.ReadTxId);

        // TODO: support followers?
        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_SCAN,
                    ctx.SelfID, state.ReadTxId, txBody, txFlags),
                shardId, true));

        state.State = EShardState::ReadTableProposeSent;
    }

    void HandleReadTable(TEvTxProcessing::TEvStreamClearanceRequest::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->Record.GetShardId();
        const ui64 txId = msg->Record.GetTxId();

        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
                "Received TEvStreamClearanceRequest from unexpected ShardId# " << shardId);

        auto& state = it->second;
        if (state.State != EShardState::ReadTableProposeSent || state.ReadTxId != txId) {
            TXLOG_D("Ignoring stream clearance request from ShardId# " << shardId << " ReadTxId# " << txId);
            // We're no longer interested in this shard/txid combination, and
            // need to reply for cases when there was interruption/retry on our
            // end, but no interruptions since stream clearance request was
            // sent by the shard. Otherwise outdated tx would be stuck waiting
            // forever, even if we already retried with a newer tx.
            auto response = MakeHolder<TEvTxProcessing::TEvStreamClearanceResponse>();
            response->Record.SetTxId(txId);
            response->Record.SetCleared(false);
            ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
            return;
        }

        // This shard is now ready for streaming
        TXLOG_D("Received TEvStreamClearanceRequest from ShardId# " << shardId);
        state.State = EShardState::ReadTableClearancePending;
        state.ClearanceActor = ev->Sender;
        state.ClearanceCookie = ev->Cookie;
        ctx.Send(ev->Sender, new TEvTxProcessing::TEvStreamClearancePending(txId), 0, ev->Cookie);

        // This shard is confirmed to work, reset retry state
        state.Retries = 0;
        state.AllowInstantRetry = true;

        auto [pendingIt, inserted] = ClearancePendingShards.insert(shardId);
        Y_VERIFY_S(inserted, "Unexpected failure to add ShardId# " << shardId << " to ClearancePendingShards");

        ProcessClearanceRequests(ctx);
    }

    void ProcessClearanceRequests(const TActorContext& ctx) {
        if (!ClearancePendingShards) {
            return;
        }

        const auto& cfg = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();

        // In ordered mode we always read from the first shard in the list
        if (Settings.Ordered) {
            Y_ABORT_UNLESS(ShardList, "Unexpected empty shard list");
            auto& state = *ShardList.front();

            Y_ABORT_UNLESS(state.State != EShardState::Finished && state.State != EShardState::Error,
                "Unexpected state in shard list");

            if (state.State == EShardState::ReadTableClearancePending) {
                StartStreaming(state, ctx);
            }

            return;
        }

        size_t limit = cfg.GetMaxStreamingShards();
        while (StreamingShards.size() < limit && ClearancePendingShards) {
            auto first = ClearancePendingShards.begin();
            Y_ABORT_UNLESS(first != ClearancePendingShards.end());

            const ui64 shardId = *first;
            auto it = ShardMap.find(shardId);
            Y_ABORT_UNLESS(it != ShardMap.end());
            auto& state = it->second;

            StartStreaming(state, ctx);
        }
    }

    void StartStreaming(TShardState& state, const TActorContext& ctx) {
        Y_ABORT_UNLESS(state.State == EShardState::ReadTableClearancePending);

        ui64 shardId = state.ShardId;
        Y_ABORT_UNLESS(ClearancePendingShards.contains(shardId));
        Y_ABORT_UNLESS(!StreamingShards.contains(shardId));

        auto response = MakeHolder<TEvTxProcessing::TEvStreamClearanceResponse>();
        response->Record.SetTxId(state.ReadTxId);
        response->Record.SetCleared(true);

        TXLOG_D("Sending TEvStreamClearanceResponse to " << state.ClearanceActor << " ShardId# " << shardId);
        ctx.Send(state.ClearanceActor, response.Release(), 0, state.ClearanceCookie);

        state.State = EShardState::ReadTableStreaming;
        ClearancePendingShards.erase(shardId);
        StreamingShards.insert(shardId);
    }

    void HandleReadTable(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->GetOrigin();

        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
                "Received TEvProposeTransactionResult from unexpected ShardId# " << shardId);
        auto& state = it->second;

        if (state.State == EShardState::SnapshotPlanned) {
            // We are expecting a snapshot result from this shard
            return HandleSnapshotResult(ev, ctx);
        }

        switch (state.State) {
            case EShardState::ReadTableProposeSent:
            case EShardState::ReadTableClearancePending:
            case EShardState::ReadTableStreaming:
                if (state.ReadTxId == msg->GetTxId()) {
                    break;
                }
                [[fallthrough]];

            default:
                // Ignore unexpected results
                TXLOG_T("Ignore propose result from ShardId# " << shardId << " TxId# " << msg->GetTxId()
                        << " in State# " << state.State << " ReadTxId# " << state.ReadTxId);
                // Pretend we don't exist if sender tracks delivery
                ctx.Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown));
                return;
        }

        const auto& record = msg->Record;

        if (record.HasExecLatency())
            ElapsedExecExec = Max<TDuration>(ElapsedExecExec, TDuration::MilliSeconds(record.GetExecLatency()));
        if (record.HasProposeLatency())
            ElapsedExecComplete = Max<TDuration>(ElapsedExecComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

        switch (msg->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA:
                ProcessStreamData(state, ev, ctx);
                return;

            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE:
                ProcessStreamComplete(state, ev, ctx);
                return;

            default:
                ProcessStreamError(state, ev, ctx);
                break;
        }
    }

    void ProcessStreamData(TShardState& state, TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const ui64 shardId = state.ShardId;

        TXLOG_D("Received stream data from ShardId# " << shardId);

        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        // We shouldn't receive data before shard has been cleared
        Y_VERIFY_S(state.State == EShardState::ReadTableStreaming,
                "Unexpected RESPONSE_DATA from ShardId# " << shardId << " TxId# " << msg->GetTxId()
                << " in State# " << state.State);

        // Always notify scan about received data (otherwise it may become stuck)
        TXLOG_T("Sending TEvStreamDataAck to " << ev->Sender << " ShardId# " << shardId);
        ctx.Send(ev->Sender, new TEvTxProcessing::TEvStreamDataAck);

        if (record.HasDataSeqNo()) {
            if (record.GetDataSeqNo() != state.ExpectedSeqNo) {
                // Some data chunk is missing, probably TEvDeliveryProblem is a little late
                if (ScheduleShardRetry(state, ctx)) {
                    return;
                }
                const TString error = TStringBuilder()
                    << "Connection problem with ShardId# " << shardId << " TxId# " << TxId;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, error));
                // FIXME: why TIMEOUT?
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::TIMEOUT, ctx);
            }

            // Expect responses in the correct order
            ++state.ExpectedSeqNo;

            // On retries resume by skipping the last key
            state.Ranges.front().From.Parse(record.GetDataLastKey());
            state.Ranges.front().FromInclusive = false;
        } else {
            // We are using older version that doesn't support SeqNo
            // Shards like these cannot be retried
            state.ExpectedSeqNo = 0;
            state.Retriable = false;
        }

        TString data = record.GetTxResult();
        if (RemainingRows != Max<ui64>()) {
            // Trim data if there are too many rows
            auto rows = record.RowOffsetsSize();
            if (rows > RemainingRows) {
                data.resize(record.GetRowOffsets(RemainingRows));
                RemainingRows = 0;
            } else {
                RemainingRows -= rows;
            }
        }

        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(TEvTxUserProxy::TResultStatus::ExecResponseData);
        x->Record.SetStatusCode(NKikimrIssues::TStatusIds::TRANSIENT);

        if (PlanStep) {
            x->Record.SetStep(PlanStep);
        }
        x->Record.SetTxId(TxId);

        x->Record.SetSerializedReadTableResponse(data);
        x->Record.SetReadTableResponseVersion(record.GetApiVersion());

        // N.B. we use shard id 0 for virtualized quota management
        x->Record.SetDataShardTabletId(0);

        TxProxyMon->ReportStatusStreamData->Inc();
        ctx.Send(Settings.Owner, x.Release(), 0, Settings.Cookie);

        SentResultSet = true;

        if (state.QuotaReserved > 0) {
            Y_ABORT_UNLESS(Quota.Reserved > 0 && Quota.Allocated > 0);
            --Quota.Reserved;
            --Quota.Allocated;
            --state.QuotaReserved;
        } else {
            TXLOG_E("Response data from ShardId# " << shardId << " without a reserved quota");
        }

        if (RemainingRows == 0) {
            return ReplyAndDie(TEvTxUserProxy::TResultStatus::ExecComplete, NKikimrIssues::TStatusIds::SUCCESS, ctx);
        }

        ProcessQuotaRequests(ctx);
    }

    void SendEmptyResponseData(const TActorContext& ctx) {
        TString data;
        ui32 apiVersion = 0;

        switch (Settings.DataFormat) {
            case EReadTableFormat::OldResultSet:
                // we don't support empty result sets
                return;

            case EReadTableFormat::YdbResultSet: {
                Ydb::ResultSet res;
                for (auto& col : Columns) {
                    auto* meta = res.add_columns();
                    meta->set_name(col.Name);

                    if (col.PType.GetTypeId() == NScheme::NTypeIds::Pg) {
                        auto* typeDesc = col.PType.GetTypeDesc();
                        auto* pg = meta->mutable_type()->mutable_pg_type();
                        pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
                        pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
                    } else {
                        auto id = static_cast<NYql::NProto::TypeIds>(col.PType.GetTypeId());
                        if (id == NYql::NProto::Decimal) {
                            auto decimalType = meta->mutable_type()->mutable_optional_type()->mutable_item()
                                ->mutable_decimal_type();
                            //TODO: Pass decimal params here
                            decimalType->set_precision(22);
                            decimalType->set_scale(9);
                        } else {
                            meta->mutable_type()->mutable_optional_type()->mutable_item()
                                ->set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(id));
                        }
                    }
                }
                bool ok = res.SerializeToString(&data);
                Y_ABORT_UNLESS(ok, "Unexpected failure to serialize Ydb::ResultSet");
                apiVersion = NKikimrTxUserProxy::TReadTableTransaction::YDB_V1;
                break;
            }
        }

        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(TEvTxUserProxy::TResultStatus::ExecResponseData);
        x->Record.SetStatusCode(NKikimrIssues::TStatusIds::TRANSIENT);

        if (PlanStep) {
            x->Record.SetStep(PlanStep);
        }
        x->Record.SetTxId(TxId);

        x->Record.SetSerializedReadTableResponse(data);
        x->Record.SetReadTableResponseVersion(apiVersion);

        // N.B. we use shard id 0 for virtualized quota management
        x->Record.SetDataShardTabletId(0);

        TxProxyMon->ReportStatusStreamData->Inc();
        ctx.Send(Settings.Owner, x.Release(), 0, Settings.Cookie);
        SentResultSet = true;
    }

    void ProcessStreamComplete(TShardState& state, TEvDataShard::TEvProposeTransactionResult::TPtr&, const TActorContext& ctx) {
        TxProxyMon->TxResultComplete->Inc();

        TXLOG_D("Received stream complete from ShardId# " << state.ShardId);

        // We should have received a quota release message, this is just a safety net
        DiscardShardQuota(state, ctx);

        const ui64 shardId = state.ShardId;

        StreamingShards.erase(shardId);

        // We have completed reading the first range
        state.Ranges.pop_front();

        if (state.Ranges.empty()) {
            // We don't need to read from this shard anymore
            state.State = EShardState::Finished;

            // We no longer need this shard, remove it
            // N.B. it's technically quadratic, but there are usually not many shards
            auto shardPos = std::exchange(state.ShardPosition, ShardList.end());
            Y_ABORT_UNLESS(shardPos != ShardList.end());
            ShardList.erase(shardPos);

            if (ShardList.empty()) {
                // There are no shards left to stream
                if (!SentResultSet && Settings.RequireResultSet) {
                    SendEmptyResponseData(ctx);
                }
                return ReplyAndDie(TEvTxUserProxy::TResultStatus::ExecComplete, NKikimrIssues::TStatusIds::SUCCESS, ctx);
            }
        } else {
            // We need to start reading the next range
            state.State = EShardState::ReadTableNeedTxId;
            SendReadTablePropose(state, ctx);
        }

        // Process any queued requests
        ProcessClearanceRequests(ctx);
        ProcessQuotaRequests(ctx);
    }

    void ProcessStreamError(TShardState& state, TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status;
        NKikimrIssues::TStatusIds::EStatusCode code;

        bool schemeChanged = false;
        bool wrongShardState = false;
        for (const auto& e : record.GetError()) {
            switch (e.GetKind()) {
                case NKikimrTxDataShard::TError::SCHEME_CHANGED:
                    schemeChanged = true;
                    break;

                case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
                    wrongShardState = true;
                    break;

                default:
                    break;
            }
        }

        if (wrongShardState) {
            // If we detect possible split/merge avoid retrying immediately
            state.AllowInstantRetry = false;
        }

        TXLOG_D("Received TEvProposeTransactionResult Status# "
                << NKikimrTxDataShard::TEvProposeTransactionResult::EStatus_Name(msg->GetStatus())
                << " ShardId# " << state.ShardId);

        switch (msg->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED: {
                // We are using immediate read table transactions, however
                // old datashard version may decide to prepare it anyway.
                // Planning these transactions is very inefficient and not
                // currently supported.
                TxProxyMon->TxResultFatal->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest;
                code = NKikimrIssues::TStatusIds::BAD_REQUEST;

                // Cancel proposal so it doesn't wait unnecessarily.
                ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvCancelTransactionProposal(state.ReadTxId),
                    state.ShardId, false));
                break;
            }

            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR: {
                TxProxyMon->TxResultError->Inc();

                // Retry if error is due to splitting/merging
                if (wrongShardState && ScheduleShardRetry(state, ctx)) {
                    ScheduleResolveShards(ctx);
                    return;
                }

                // Retry impossible, will fail with an error
                if (schemeChanged) {
                    status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError;
                    code = NKikimrIssues::TStatusIds::REJECTED;
                } else if (wrongShardState) {
                    status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable;
                    code = NKikimrIssues::TStatusIds::REJECTED;
                } else {
                    status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable;
                    code = NKikimrIssues::TStatusIds::REJECTED;
                }

                break;
            }

            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
                TxProxyMon->TxResultAborted->Inc();

                // Aborted (possibly due to some conflicting transaction)
                if (ScheduleShardRetry(state, ctx)) {
                    if (wrongShardState) {
                        // This shard seems to be splitting/merging
                        ScheduleResolveShards(ctx);
                    }
                    return;
                }

                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted;
                code = NKikimrIssues::TStatusIds::SUCCESS; // FIXME: ???
                break;

            case NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER:
                TxProxyMon->TxResultShardTryLater->Inc();

                // Retry later (e.g. after some conflicting transaction completes)
                state.AllowInstantRetry = false;
                if (ScheduleShardRetry(state, ctx)) {
                    return;
                }

                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater;
                code = NKikimrIssues::TStatusIds::REJECTED;
                break;

            case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
                TxProxyMon->TxResultShardOverloaded->Inc();

                // Either split/merge or shard has too many transactions in flight
                state.AllowInstantRetry = false;
                if (ScheduleShardRetry(state, ctx)) {
                    if (wrongShardState) {
                        // This shard seems to be splitting/merging
                        ScheduleResolveShards(ctx);
                    }
                    return;
                }

                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded;
                code = NKikimrIssues::TStatusIds::OVERLOADED;
                RaiseShardOverloaded(record, state.ShardId);
                break;

            case NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR:
                TxProxyMon->TxResultExecError->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;

            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
                TxProxyMon->TxResultResultUnavailable->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;

            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
                TxProxyMon->TxResultCancelled->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;

            case NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST:
                TxProxyMon->TxResultCancelled->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest;
                code = NKikimrIssues::TStatusIds::BAD_REQUEST;
                break;

            default:
                TxProxyMon->TxResultFatal->Inc();
                status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown;
                code = NKikimrIssues::TStatusIds::ERROR;
                break;
        }

        ExtractDatashardErrors(record);

        DiscardShardQuota(state, ctx);

        state.State = EShardState::Error;

        return ReplyAndDie(status, code, ctx);
    }

    // Received from shards when they want to stream data
    void HandleReadTable(TEvTxProcessing::TEvStreamQuotaRequest::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        const ui64 shardId = record.GetShardId();
        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
                "Received TEvStreamQuotaRequest from unexpected ShardId# " << shardId);
        auto& state = it->second;

        if (state.State != EShardState::ReadTableStreaming || state.ReadTxId != record.GetTxId()) {
            // Ignore outdated messages and pretend we don't exist
            TXLOG_T("Ignoring outdated TEvStreamQuotaRequest from ShardId# " << shardId);
            ctx.Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown));
            return;
        }

        TXLOG_D("Received TEvStreamQuotaRequest from ShardId# " << shardId);

        state.QuotaActor = ev->Sender;
        ++state.QuotaRequests;
        QuotaNeeded.insert(shardId);

        // Send a new quota request to owner
        auto request = MakeHolder<TEvTxProcessing::TEvStreamQuotaRequest>();
        request->Record.SetTxId(TxId);
        request->Record.SetShardId(0);
        ctx.Send(Settings.Owner, request.Release(), IEventHandle::FlagTrackDelivery);

        // Use allocated quota that is already available
        ProcessQuotaRequests(ctx);
    }

    // Received from owner when quota is allocated
    void HandleReadTable(TEvTxProcessing::TEvStreamQuotaResponse::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        Quota.Allocated += record.GetReservedMessages();
        Quota.MessageSize = record.GetMessageSizeLimit();
        Quota.MessageRows = record.GetMessageRowsLimit();

        TXLOG_D("Updated quotas, allocated = " << Quota.Allocated
                << ", message size = " << Quota.MessageSize
                << ", message rows = " << Quota.MessageRows
                << ", available = " << (Quota.Allocated - Quota.Reserved));

        ProcessQuotaRequests(ctx);
    }

    // Received from shard when it no longer needs its quota
    void HandleReadTable(TEvTxProcessing::TEvStreamQuotaRelease::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        const ui64 shardId = record.GetShardId();
        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
                "Received TEvStreamQuotaRequest from unexpected ShardId# " << shardId);
        auto& state = it->second;

        if (state.State != EShardState::ReadTableStreaming || state.ReadTxId != record.GetTxId()) {
            // Ignore outdated messages and pretend we don't exist
            TXLOG_T("Ignoring outdated TEvStreamQuotaRelease from ShardId# " << shardId);
            ctx.Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown));
            return;
        }

        TXLOG_D("Received TEvStreamQuotaRelease from ShardId# " << shardId);

        DiscardShardQuota(state, ctx);
        ProcessQuotaRequests(ctx);
    }

    void ProcessQuotaRequests(const TActorContext& ctx) {
        const auto& cfg = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();
        const ui64 minQuotaSize = cfg.GetMinQuotaSize();
        const ui64 maxQuotaSize = cfg.GetMaxQuotaSize();

        while (QuotaNeeded && Quota.Reserved < Quota.Allocated) {
            ui64 available = Quota.Allocated - Quota.Reserved;

            if (Quota.Reserved > 0 && available < minQuotaSize) {
                // We want to avoid allocating quota in single message increments
                // Wait until there's at least minQuotaSize available
                TXLOG_T("Available quota " << available << " messages is less than " << minQuotaSize);
                break;
            }

            // Don't give all quota at once for better spread among shards
            if (ShardList.size() > 1) {
                available = Min(available, maxQuotaSize);
            }

            // Select some shard that requested a quota
            // TODO: prioritize it in some way (e.g. for maximum spread)
            auto quotaIt = QuotaNeeded.begin();
            Y_ABORT_UNLESS(quotaIt != QuotaNeeded.end());
            const ui64 shardId = *quotaIt;
            auto it = ShardMap.find(shardId);
            Y_ABORT_UNLESS(it != ShardMap.end());
            auto& state = it->second;

            Y_VERIFY_S(state.State == EShardState::ReadTableStreaming && state.QuotaRequests > 0,
                    "Unexpected QuotaNeeded ShardId# " << shardId
                    << " in State# " << state.State);

            // N.B. don't need to track delivery (assume it's implicitly tracked via propose pipe)
            TXLOG_D("Reserving quota " << available << " messages for ShardId# " << shardId);
            auto response = MakeHolder<TEvTxProcessing::TEvStreamQuotaResponse>();
            response->Record.SetTxId(state.ReadTxId);
            response->Record.SetReservedMessages(available);
            response->Record.SetMessageSizeLimit(Quota.MessageSize);
            response->Record.SetMessageRowsLimit(Quota.MessageRows);
            if (RemainingRows != Max<ui64>()) {
                response->Record.SetRowLimit(RemainingRows);
            }
            ctx.Send(state.QuotaActor, response.Release());

            Quota.Reserved += available;
            state.QuotaReserved += available;

            if (--state.QuotaRequests == 0) {
                QuotaNeeded.erase(quotaIt);
            }
        }
    }

    void DiscardShardQuota(TShardState& state, const TActorContext& ctx) {
        const ui64 shardId = state.ShardId;

        // Currently reserved quota becomes available to other shards
        auto released = std::exchange(state.QuotaReserved, 0);
        if (released > 0) {
            Y_ABORT_UNLESS(Quota.Reserved >= released);
            Quota.Reserved -= released;
            TXLOG_D("Released quota " << released << " reserved messages from ShardId# " << shardId);
        }

        // Respond to any outstanding requests with an empty quota
        for (size_t idx = 0; idx < state.QuotaRequests; ++idx) {
            auto response = MakeHolder<TEvTxProcessing::TEvStreamQuotaResponse>();
            response->Record.SetTxId(state.ReadTxId);
            response->Record.SetReservedMessages(0);
            response->Record.SetMessageSizeLimit(Quota.MessageSize);
            response->Record.SetMessageRowsLimit(Quota.MessageRows);
            ctx.Send(state.QuotaActor, response.Release());
        }

        // This shard no longer needs any quota
        state.QuotaActor = { };
        state.QuotaRequests = 0;
        QuotaNeeded.erase(shardId);
    }

    void HandleReadTable(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->TabletId;

        if (shardId == SelectedCoordinator) {
            if (msg->NotDelivered) {
                TxProxyMon->PlanCoordinatorDeclined->Inc();

                TXLOG_E("Plan to coordinator " << SelectedCoordinator << " was not delivered");

                TString error = TStringBuilder()
                    << "Snapshot failed to plan, TxId# " << TxId;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_BY_COORDINATOR, error));

                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::REJECTED, ctx);
            } else if (PlanStep != 0) {
                // We lost pipe to coordinator, but we already know tx is planned
                return;
            } else {
                TxProxyMon->PlanClientDestroyed->Inc();

                TXLOG_E("Plan delivery problem to coordinator " << SelectedCoordinator);

                TString error = TStringBuilder()
                    << "Snapshot state unknown, lost pipe to coordinator, TxId# " << TxId;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, error));

                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::TIMEOUT, ctx);
            }
        }

        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
            "Received TEvDeliveryProblem from unexpected shard " << shardId);
        auto& state = it->second;

        TXLOG_D("Delivery problem with ShardId# " << shardId);

        switch (state.State) {
            case EShardState::SnapshotProposeSent:
            case EShardState::SnapshotPrepared:
            case EShardState::SnapshotPlanned:
                // Connection problem during snapshot phase, cannot be retried
                TxProxyMon->ClientConnectedError->Inc();
                ComplainingDatashards.push_back(shardId);
                CancelProposal(shardId); // it's too late, but won't hurt

                if (msg->NotDelivered) {
                    const TString error = TStringBuilder()
                        << "could not deliver program to shard " << shardId << " with txid# " << TxId;
                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error));
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
                } else {
                    const TString error = TStringBuilder()
                        << "tx state unknown for shard " << shardId << " with txid# " << TxId;
                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, error));
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
                }

                return Die(ctx);

            case EShardState::ReadTableProposeSent:
            case EShardState::ReadTableClearancePending:
            case EShardState::ReadTableStreaming:
                // Attempt to retry reading from this shard
                if (!ScheduleShardRetry(state, ctx)) {
                    const TString error = TStringBuilder()
                        << "delivery problem to shard " << shardId << " with ReadTxId# " << state.ReadTxId;
                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, error));
                    // FIXME: why TIMEOUT?
                    return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::TIMEOUT, ctx);
                }
                break;

            default:
                // Ignore possibly stale notifications
                break;
        }

        if (state.SnapshotState == ESnapshotState::Refreshing) {
            ScheduleRefreshShardRetry(state, ctx);
        }
    }

    bool ScheduleShardRetry(TShardState& state, const TActorContext& ctx) {
        const ui64 shardId = state.ShardId;

        Y_ABORT_UNLESS(state.State != EShardState::ReadTableNeedRetry);
        state.State = EShardState::ReadTableNeedRetry;
        state.RetrySeqNo++;

        // Some shards cannot be retried
        if (!state.Retriable) {
            TXLOG_D("ScheduleShardRetry: not retriable ShardId# " << shardId);
            return false;
        }

        auto now = ctx.Now();
        if (state.Retries == 0) {
            state.RetryingSince = now;
            state.LastRetryDelay = TDuration::Zero();
        }

        if (state.Retries >= MAX_SHARD_RETRIES || (now - state.RetryingSince) >= MAX_SHARD_RETRY_TIME) {
            TXLOG_D("ScheduleShardRetry: too many retries ShardId# " << shardId);
            return false;
        }

        // This shard is no longer active, remove it from active sets
        ClearancePendingShards.erase(shardId);
        StreamingShards.erase(shardId);
        DiscardShardQuota(state, ctx);

        // We may start working with other shards in the mean time
        ProcessClearanceRequests(ctx);
        ProcessQuotaRequests(ctx);

        if (std::exchange(state.AllowInstantRetry, false)) {
            // Retry immediately
            TXLOG_D("ScheduleShardRetry: retrying immediately ShardId# " << shardId);
            StartShardRetry(state, ctx);
            return true;
        }

        // We want to retry with exponential backoff
        auto delay = state.SelectNextRetryDelay();
        TXLOG_D("ScheduleShardRetry: retrying in " << delay << " ShardId# " << shardId);
        state.RetryTimer = CreateLongTimer(
                ctx,
                delay,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvPrivate::TEvRetryShard(shardId, state.RetrySeqNo)));
        return true;
    }

    void HandleRetryShard(TEvPrivate::TEvRetryShard::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->ShardId;
        auto it = ShardMap.find(shardId);
        Y_ABORT_UNLESS(it != ShardMap.end());
        auto& state = it->second;

        if (state.RetrySeqNo != msg->SeqNo || state.State != EShardState::ReadTableNeedRetry) {
            // Ignore outdated messages
            TXLOG_T("Ignoring outdated TEvRetryShard ShardId# " << shardId << " State# " << state.State);
            return;
        }

        state.RetryTimer = { };
        TXLOG_D("ScheduleShardRetry: retry timer hit ShardId# " << shardId);
        StartShardRetry(state, ctx);
    }

    void StartShardRetry(TShardState& state, const TActorContext& ctx) {
        const ui64 shardId = state.ShardId;

        if (state.ReadTxId) {
            // Interrupt a previously proposed transaction
            TXLOG_T("Interrupting previous ReadTxId# " << state.ReadTxId << " ShardId# " << shardId);
            SendInterruptReadTable(state, ctx);
        }

        // Propose a new transaction using some new ReadTxId
        state.State = EShardState::ReadTableNeedTxId;
        state.Retries++;
        SendReadTablePropose(state, ctx);

        // Schedule resolve if there are too many retries already
        // It's possible for shard to split and be deleted in a short time,
        // in which case we would never succeed in connecting until we find
        // new shards.
        if ((state.Retries % MAX_RETRIES_TO_RESOLVE) == 0) {
            ScheduleResolveShards(ctx);
        }
    }

    void ScheduleRefreshShard(TShardState& state, const TActorContext& ctx) {
        state.RefreshTimer = CreateLongTimer(
            ctx,
            SNAPSHOT_TIMEOUT / 2,
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvPrivate::TEvRefreshShard(state.ShardId, ++state.RefreshSeqNo)));
    }

    void ScheduleRefreshShardRetry(TShardState& state, const TActorContext& ctx) {
        state.SnapshotState = ESnapshotState::RefreshNeedRetry;
        state.RefreshTimer = CreateLongTimer(
            ctx,
            state.SelectNextRefreshDelay(),
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvPrivate::TEvRefreshShard(state.ShardId, ++state.RefreshSeqNo)));
    }

    void HandleRefreshShard(TEvPrivate::TEvRefreshShard::TPtr& ev, const TActorContext&) {
        const auto* msg = ev->Get();
        const ui64 shardId = msg->ShardId;
        auto it = ShardMap.find(shardId);
        Y_ABORT_UNLESS(it != ShardMap.end());
        auto& state = it->second;

        if (state.RefreshSeqNo != msg->SeqNo || (
                state.SnapshotState != ESnapshotState::Confirmed &&
                state.SnapshotState != ESnapshotState::RefreshNeedRetry))
        {
            // Ignore outdated messages
            return;
        }

        state.RefreshTimer = { };
        state.SnapshotState = ESnapshotState::Refreshing;

        TXLOG_D("Sending TEvRefreshVolatileSnapshotRequest ShardId# " << shardId);
        auto req = MakeHolder<TEvDataShard::TEvRefreshVolatileSnapshotRequest>();
        req->Record.SetOwnerId(TableId.PathId.OwnerId);
        req->Record.SetPathId(TableId.PathId.LocalPathId);
        req->Record.SetStep(PlanStep);
        req->Record.SetTxId(TxId);
        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(req.Release(), shardId, true));
    }

    void HandleReadTable(TEvDataShard::TEvRefreshVolatileSnapshotResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetOrigin();
        auto it = ShardMap.find(shardId);
        Y_VERIFY_S(it != ShardMap.end(),
            "Unexpected TEvRefreshVolatileSnapshotResponse from ShardId# " << shardId);
        auto& state = it->second;

        if (state.SnapshotState != ESnapshotState::Refreshing) {
            // It's possible we got a delivery problem and then a reply, we're
            // probably going to retry soon, so ignore this particular message.
            TXLOG_T("Ignoring outdated TEvRefreshVolatileSnapshotResponse ShardId# " << shardId);
            return;
        }

        using TResponse = NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse;
        TXLOG_D("Received TEvRefreshVolatileSnapshotResponse Status# "
                << TResponse::EStatus_Name(record.GetStatus())
                << " ShardId# " << shardId);
        switch (record.GetStatus()) {
            case TResponse::REFRESHED:
            case TResponse::SNAPSHOT_NOT_READY:
                // Snapshot is (probably) confirmed to exist, schedule next refresh
                state.SnapshotState = ESnapshotState::Confirmed;
                state.LastRefreshDelay = TDuration::Zero();
                ScheduleRefreshShard(state, ctx);
                break;

            case TResponse::WRONG_SHARD_STATE:
            case TResponse::SNAPSHOT_NOT_FOUND:
                // Snapshot cannot be refreshed, stop trying
                state.SnapshotState = ESnapshotState::Unavailable;
                break;

            case TResponse::SNAPSHOT_TRANSFERRED:
                // This shard is splitting/merging, stop trying to refresh
                state.SnapshotState = ESnapshotState::Unavailable;
                // Gather all known new shards and schedule their refresh
                for (ui64 nextId : record.GetTransferredToShards()) {
                    auto nextIt = ShardMap.find(nextId);
                    if (nextIt == ShardMap.end()) {
                        auto [newIt, inserted] = ShardMap.emplace(
                            std::piecewise_construct,
                            std::forward_as_tuple(nextId),
                            std::forward_as_tuple(nextId));
                        Y_ABORT_UNLESS(inserted);
                        nextIt = newIt;
                    }
                    auto& nextState = nextIt->second;
                    if (nextState.SnapshotState == ESnapshotState::Unknown) {
                        nextState.SnapshotState = ESnapshotState::Confirmed;
                        ScheduleRefreshShard(nextState, ctx);
                    }
                }
                break;
        }
    }

    // When we suspect split/merge we schedule resolve to find new shard ids
    void ScheduleResolveShards(const TActorContext& ctx) {
        if (ResolveShardsScheduled || ResolveInProgress) {
            return;
        }

        // TODO: need a "resolve without changes" counter and exponential backoff
        TXLOG_D("Scheduling TEvResolveShards");
        ctx.Schedule(TDuration::MilliSeconds(64), new TEvPrivate::TEvResolveShards);
        ResolveShardsScheduled = true;
    }

    void HandleResolveShards(const TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveShardsScheduled);
        ResolveShardsScheduled = false;

        Y_ABORT_UNLESS(!ResolveInProgress);

        auto updatedKeyDesc = MakeHolder<TKeyDesc>(
                TableId, KeyDesc->Range, TKeyDesc::ERowOperation::Read,
                KeyDesc->KeyColumnTypes, KeyDesc->Columns);

        TXLOG_D("Sending TEvResolveKeySet update for table '" << Settings.TablePath << "'");
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        // Avoid setting DomainOwnerId to reduce possible races with schemeshard migration
        request->DatabaseName = Settings.DatabaseName;
        request->ResultSet.emplace_back(std::move(updatedKeyDesc));
        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, TActorId()));
        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        ResolveInProgress = true;
    }

    void HandleReadTable(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveInProgress, "Received TEvResolveKeySetResult without an active request");
        ResolveInProgress = false;

        TXLOG_D("Received TEvResolveKeySetResult update for table '" << Settings.TablePath << "'");

        auto* request = ev->Get()->Request.Get();
        if (request->ErrorCount > 0) {
            TXLOG_E("Resolve request failed for table '" << Settings.TablePath << "', ErrorCount# " << request->ErrorCount);

            bool gotHardResolveError = false;
            for (const auto& x : request->ResultSet) {
                if ((ui32)x.Status < (ui32) NSchemeCache::TSchemeCacheRequest::EStatus::OkScheme) {
                    TryToInvalidateTable(TableId, ctx);

                    TString error;
                    switch (x.Status) {
                        case NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist:
                            gotHardResolveError = true;
                            error = TStringBuilder() << "table not exists: " << x.KeyDescription->TableId;
                            break;
                        case NSchemeCache::TSchemeCacheRequest::EStatus::TypeCheckError:
                            gotHardResolveError = true;
                            error = TStringBuilder() << "type check error: " << x.KeyDescription->TableId;
                            break;
                        default:
                            error = TStringBuilder() << "unresolved table: " << x.KeyDescription->TableId << ". Status: " << x.Status;
                            break;
                    }

                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
                    UnresolvedKeys.push_back(error);
                }
            }

            if (gotHardResolveError) {
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
            } else {
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, ctx);
            }
        }

        KeyDesc = std::move(request->ResultSet[0].KeyDescription);

        if (KeyDesc->GetPartitions().empty()) {
            TString error = TStringBuilder() << "No partitions to read from '" << Settings.TablePath << "'";
            TXLOG_E(error);
            return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, NKikimrIssues::TStatusIds::BAD_REQUEST, ctx);
        }

        Y_ABORT_UNLESS(ShardList, "Unexpected empty shard list");

        // WARNING: we must work correctly even when ResolveKeySet flip-flops
        // between resolved partitions in some very unfortunate edge cases.

        TShardList oldShardList;
        ShardList.swap(oldShardList);
        auto oldShardListIt = oldShardList.begin();
        TShardState* oldShard = *oldShardListIt;

        auto oldShardNext = [&]() {
            if (++oldShardListIt != oldShardList.end()) {
                oldShard = *oldShardListIt;
            } else {
                oldShard = nullptr;
            }
        };

        Y_ABORT_UNLESS(!ShardList, "Unexpected non-empty shard list");

        THashSet<TShardState*> removed;

        for (size_t idx = 0; idx < KeyDesc->GetPartitions().size(); ++idx) {
            const auto& partition = KeyDesc->GetPartitions()[idx];
            const ui64 shardId = partition.ShardId;

            TXLOG_T("Processing resolved shard ShardId# " << shardId);

            auto [it, inserted] = ShardMap.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(shardId),
                std::forward_as_tuple(shardId));
            auto& state = it->second;

            if (PlanStep != 0 &&
                state.State == EShardState::Unknown &&
                state.SnapshotState == ESnapshotState::Unknown)
            {
                // Assume all new shards we discover have a confirmed snapshot
                // and schedule a refresh according to timeout. We can make
                // this assumption because split would be blocked until
                // snapshot is actually created.
                state.SnapshotState = ESnapshotState::Confirmed;
                ScheduleRefreshShard(state, ctx);
            }

            TSerializedTableRange shardRange;

            if (idx == 0) {
                // First shard in range
                shardRange.From = KeyFromValues;
                shardRange.FromInclusive = KeyDesc->Range.InclusiveFrom;
            } else {
                const auto& prevRange = *KeyDesc->GetPartitions()[idx - 1].Range;
                shardRange.From = prevRange.EndKeyPrefix;
                shardRange.FromInclusive = !prevRange.IsInclusive; // N.B. always true for now
            }

            if (idx == KeyDesc->GetPartitions().size() - 1) {
                // Last shard in range
                shardRange.To = KeyToValues;
                shardRange.ToInclusive = KeyDesc->Range.InclusiveTo;
            } else {
                const auto& partRange = *partition.Range;
                shardRange.To = partRange.EndKeyPrefix;
                shardRange.ToInclusive = partRange.IsInclusive; // N.B. always false for now
            }

            // We want to skip all ranges that are to the left of the current range
            // N.B. this code path should never actually trigger, since we are not
            // supposed to stop seeing a range on subsequent resolves.
            while (oldShard && oldShard != &state && !RangeEndBeginHasIntersection(
                    KeyDesc->KeyColumnTypes,
                    oldShard->Ranges.front().To.GetCells(), oldShard->Ranges.front().ToInclusive,
                    shardRange.From.GetCells(), shardRange.FromInclusive))
            {
                TXLOG_T("Removing old range ShardId# " << oldShard->ShardId << " from shard list");
                removed.insert(oldShard);
                CancelActiveShard(*oldShard, ctx);
                oldShard->Ranges.pop_front();

                if (oldShard->Ranges.empty()) {
                    oldShard->ShardPosition = ShardList.end();
                    oldShardNext();
                }
            }

            if (oldShard == &state) {
                TXLOG_T("Moving existing shard ShardId# " << state.ShardId << " to new shard list");
                Y_ABORT_UNLESS(!state.Ranges.empty(), "Re-adding an empty shard!");
                // Move this shard to new shard list without range changes
                state.ShardPosition = ShardList.insert(ShardList.end(), &state);
                // If shard is currently in a finished state, it means some
                // ranges have been moved to a different shard. It shouldn't
                // really happen, but don't fail when it does.
                if (state.State == EShardState::Finished) {
                    state.State = EShardState::ReadTableNeedTxId;
                    SendReadTablePropose(state, ctx);
                }
                oldShardNext();
                continue;
            }

            if (oldShard == nullptr || !RangeEndBeginHasIntersection(
                    KeyDesc->KeyColumnTypes,
                    shardRange.To.GetCells(), shardRange.ToInclusive,
                    oldShard->Ranges.front().From.GetCells(), oldShard->Ranges.front().FromInclusive))
            {
                TXLOG_T("Ignoring new shard ShardId# " << oldShard->ShardId << " (nothing to read)");

                // We don't want to read anything from current shard
                state.ShardPosition = ShardList.end();
                if (state.State == EShardState::Unknown) {
                    state.State = EShardState::Finished;
                }
                continue;
            }

            // Move some ranges over cropped to the current range
            for (;;) {
                if (oldShard == nullptr || !RangeEndBeginHasIntersection(
                        KeyDesc->KeyColumnTypes,
                        shardRange.To.GetCells(), shardRange.ToInclusive,
                        oldShard->Ranges.front().From.GetCells(), oldShard->Ranges.front().FromInclusive))
                {
                    // We don't want to read anything else from this shard
                    break;
                }

                removed.insert(oldShard);
                CancelActiveShard(*oldShard, ctx);

                int cmp = CompareRangeEnds(
                        KeyDesc->KeyColumnTypes,
                        shardRange.To.GetCells(),
                        shardRange.ToInclusive,
                        oldShard->Ranges.front().To.GetCells(),
                        oldShard->Ranges.front().ToInclusive);
                if (cmp < 0) {
                    // New shard ends first, we need to split the original shard
                    TXLOG_T("Adding new range ShardId# " << state.ShardId << ", partially consumes ShardId# " << oldShard->ShardId);
                    auto& newRange = state.Ranges.emplace_back();
                    newRange.From = oldShard->Ranges.front().From;
                    newRange.FromInclusive = oldShard->Ranges.front().FromInclusive;
                    newRange.To = shardRange.To;
                    newRange.ToInclusive = shardRange.ToInclusive;

                    // The old shard range starts just after new range ends
                    oldShard->Ranges.front().From = newRange.To;
                    oldShard->Ranges.front().FromInclusive = !newRange.ToInclusive;
                    break;
                } else {
                    // We consume the old shard range completely
                    TXLOG_T("Adding new range ShardId# " << state.ShardId << ", fully consumes ShardId# " << oldShard->ShardId);
                    state.Ranges.emplace_back(std::move(oldShard->Ranges.front()));
                    oldShard->Ranges.pop_front();

                    if (oldShard->Ranges.empty()) {
                        oldShard->ShardPosition = ShardList.end();
                        oldShardNext();
                    }

                    if (cmp == 0) {
                        break; // don't bother looking for more shards
                    }
                }
            }

            if (state.Ranges.empty()) {
                // We don't want to read anything from this new shard
                TXLOG_T("Ignoring new shard ShardId# " << state.ShardId << " (nothing to read)");
                state.ShardPosition = ShardList.end();
                if (state.State == EShardState::Unknown) {
                    state.State = EShardState::Finished;
                }
                continue;
            }

            // Otherwise we better start reading from it
            state.ShardPosition = ShardList.insert(ShardList.end(), &state);
            state.State = EShardState::ReadTableNeedTxId;
            SendReadTablePropose(state, ctx);
        }

        // N.B. this code path should never actually trigger, since even when
        // shard aliasing changes (due to partial read range) we should have
        // a truncated range for the final shard. So we would probably never
        // find a leftover shard range, that doesn't match any of new resolved
        // shards.
        while (oldShard) {
            TXLOG_T("Removing old shard ShardId# " << oldShard->ShardId << " from shard list");
            removed.insert(oldShard);
            CancelActiveShard(*oldShard, ctx);
            oldShard->Ranges.clear();
            oldShard->ShardPosition = ShardList.end();
            oldShardNext();
        }

        for (TShardState* shard : removed) {
            if (!shard->Retriable) {
                const TString error = TStringBuilder()
                    << "split/merge detected in shard " << shard->ShardId << " which cannot be safely retried";
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, error));
                return ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::SCHEME_ERROR, ctx);
            }
        }

        // Process requests based on the new shard list
        ProcessClearanceRequests(ctx);
        ProcessQuotaRequests(ctx);
    }

    STFUNC(StateZombie) {
        TRACE_EVENT(NKikimrServices::TX_PROXY)
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleZombieDie);
            default:
                // For all other events we play dead as if we didn't exist
                Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown));
                break;
        }
    }

    void HandleZombieDie(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr&, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ResolveInProgress, "Received TEvResolveKeySetResult without an active request");
        ResolveInProgress = false;

        // It is finally safe to die
        TBase::Die(ctx);
    }

private:
    void Handle(TEvDataShard::TEvGetReadTableSinkStateRequest::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvDataShard::TEvGetReadTableSinkStateResponse>();

        auto& rec = response->Record;
        rec.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        rec.SetTxId(TxId);
        rec.SetWallClockAccepted(WallClockAccepted.GetValue());
        rec.SetWallClockResolveStarted(WallClockResolveStarted.GetValue());
        rec.SetWallClockResolved(WallClockResolved.GetValue());
        rec.SetWallClockPrepared(WallClockPrepared.GetValue());
        rec.SetWallClockPlanned(WallClockPlanned.GetValue());
        rec.SetSelectedCoordinator(SelectedCoordinator);
        rec.SetRequestSource(ToString(Settings.Owner));

        switch (Settings.DataFormat) {
            case EReadTableFormat::OldResultSet:
                rec.SetRequestVersion(NKikimrTxUserProxy::TReadTableTransaction::UNSPECIFIED);
                rec.SetResponseVersion(NKikimrTxUserProxy::TReadTableTransaction::UNSPECIFIED);
                break;

            case EReadTableFormat::YdbResultSet:
                // FIXME: use ApiVersion from datashards?
                rec.SetRequestVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
                rec.SetResponseVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
                break;

            default:
                break;
        }

        for (ui64 shardId : ClearancePendingShards) {
            rec.AddClearanceRequests()->SetId(shardId);
        }
        for (ui64 shardId : QuotaNeeded) {
            rec.AddQuotaRequests()->SetId(shardId);
        }
        for (ui64 shardId : StreamingShards) {
            rec.AddStreamingShards()->SetId(shardId);
        }
        for (TShardState* state : ShardList) {
            rec.AddShardsQueue()->SetId(state->ShardId);
        }

        rec.SetOrdered(Settings.Ordered);
        rec.SetRowsLimited(Settings.MaxRows);
        rec.SetRowsRemain(RemainingRows);

        ctx.Send(ev->Sender, response.Release());
    }

    void Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Forward(Settings.Owner));
    }

private:
    void TryToInvalidateTable(TTableId tableId, const TActorContext& ctx) {
        const bool notYetInvalidated = InvalidatedTables.insert(tableId).second;
        if (notYetInvalidated) {
            ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(tableId, TActorId()));
        }
    }

    void ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, NKikimrIssues::TStatusIds::EStatusCode code, bool reportIssues, const TActorContext& ctx) {
        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(status);

        if (PlanStep) {
            x->Record.SetStep(PlanStep);
        }
        x->Record.SetTxId(TxId);

        if (reportIssues && IssueManager.GetIssues()) {
            IssuesToMessage(IssueManager.GetIssues(), x->Record.MutableIssues());
            IssueManager.Reset();
        }

        x->Record.SetStatusCode(code);

        for (auto& unresolvedKey : UnresolvedKeys) {
            x->Record.AddUnresolvedKeys(unresolvedKey);
        }

        if (!DatashardErrors.empty()) {
            x->Record.SetDataShardErrors(DatashardErrors);
        }

        if (const ui32 cs = ComplainingDatashards.size()) {
            x->Record.MutableComplainingDataShards()->Reserve(cs);
            for (auto ds : ComplainingDatashards) {
                x->Record.AddComplainingDataShards(ds);
            }
        }

        TInstant now = Now();

        if (Settings.ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyTrackWallClock) {
            auto* timings = x->Record.MutableTimings();
            if (WallClockAccepted)
                timings->SetWallClockAccepted(WallClockAccepted.MicroSeconds());
            if (WallClockResolved)
                timings->SetWallClockResolved(WallClockResolved.MicroSeconds());
            if (WallClockPrepared)
                timings->SetWallClockPrepared(WallClockPrepared.MicroSeconds());
            if (WallClockPlanned)
                timings->SetWallClockPlanned(WallClockPlanned.MicroSeconds());
            if (ElapsedPrepareExec)
                timings->SetElapsedPrepareExec(ElapsedExecExec.MicroSeconds());
            if (ElapsedPrepareComplete)
                timings->SetElapsedPrepareComplete(ElapsedExecComplete.MicroSeconds());
            if (ElapsedExecExec)
                timings->SetElapsedExecExec(ElapsedExecExec.MicroSeconds());
            if (ElapsedExecComplete)
                timings->SetElapsedExecComplete(ElapsedExecComplete.MicroSeconds());
            timings->SetWallClockNow(now.MicroSeconds());
        }

        TDuration prepareTime = WallClockPrepared ? WallClockPrepared - WallClockAccepted : TDuration::Zero();
        TDuration executeTime = WallClockPrepared ? now - WallClockPrepared : now - WallClockAccepted;
        TDuration totalTime = now - WallClockAccepted;

        auto fnLogStatus = [&](NActors::NLog::EPriority prio) {
            TXLOG_LOG(prio, "RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
                    << " shard: " << (ComplainingDatashards ? ComplainingDatashards.front() : 0)
                    << " table: " << Settings.TablePath);
        };

        switch (status) {
            case TEvTxUserProxy::TResultStatus::ProxyAccepted:
            case TEvTxUserProxy::TResultStatus::ProxyResolved:
            case TEvTxUserProxy::TResultStatus::ProxyPrepared:
            case TEvTxUserProxy::TResultStatus::CoordinatorPlanned:
            case TEvTxUserProxy::TResultStatus::ExecComplete:
            case TEvTxUserProxy::TResultStatus::ExecAborted:
            case TEvTxUserProxy::TResultStatus::ExecAlready:
                TXLOG_I("RESPONSE Status# " << TEvTxUserProxy::TResultStatus::Str(status)
                        << " prepare time: " << prepareTime.ToString()
                        << " execute time: " << executeTime.ToString()
                        << " total time: " << totalTime.ToString());

                TxProxyMon->ReportStatusOK->Inc();

                TxProxyMon->TxPrepareTimeHgram->Collect(prepareTime.MilliSeconds());
                TxProxyMon->TxExecuteTimeHgram->Collect(executeTime.MilliSeconds());
                TxProxyMon->TxTotalTimeHgram->Collect(totalTime.MilliSeconds());
                break;

            case TEvTxUserProxy::TResultStatus::ExecResponseData:
                // N.B. shouldn't happen!
                TxProxyMon->ReportStatusStreamData->Inc();
                break;

            case TEvTxUserProxy::TResultStatus::ProxyShardTryLater:
            case TEvTxUserProxy::TResultStatus::ProxyShardOverloaded:
                fnLogStatus(NActors::NLog::PRI_NOTICE);
                TxProxyMon->ReportStatusNotOK->Inc();
                break;

            case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable:
            case TEvTxUserProxy::TResultStatus::ProxyShardUnknown:
                fnLogStatus(NActors::NLog::PRI_INFO);
                TxProxyMon->ReportStatusNotOK->Inc();
                break;

            default:
                fnLogStatus(NActors::NLog::PRI_ERROR);
                TxProxyMon->ReportStatusNotOK->Inc();
                break;
        }

        ctx.Send(Settings.Owner, x.Release(), 0, Settings.Cookie);
    }

    void ReplyAndDie(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, NKikimrIssues::TStatusIds::EStatusCode code, const TActorContext& ctx) {
        ReportStatus(status, code, true, ctx);
        Die(ctx);
    }

private:
    const TReadTableSettings Settings;

    TString LogPrefix;

    ui64 TxId = 0;
    ui64 SelectedCoordinator = 0;
    NTxProxy::TTxProxyServices Services;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    THolder<const NACLib::TUserToken> UserToken;

    TTableId TableId;
    NSchemeCache::TDomainInfo::TPtr DomainInfo;
    TVector<TTableColumnInfo> Columns;
    TSerializedCellVec KeyFromValues;
    TSerializedCellVec KeyToValues;
    THolder<TKeyDesc> KeyDesc;

    TShardMap ShardMap;
    TShardList ShardList;
    size_t TabletsToPrepare = 0;
    size_t TabletPrepareErrors = 0;

    THashSet<ui64> ClearancePendingShards;
    THashSet<ui64> StreamingShards;

    NYql::TIssueManager IssueManager;
    TVector<TString> UnresolvedKeys;
    TTablePathHashSet InvalidatedTables;

    TStringBuilder DatashardErrors;
    TVector<ui64> ComplainingDatashards;

    ui64 PlanStep = 0;
    ui64 AggrMinStep = 0;
    ui64 AggrMaxStep = Max<ui64>();

    ui64 CurrentReadTxId = 0;
    bool AllocatingReadTxId = false;
    bool ResolveInProgress = false;

    bool ResolveShardsScheduled = false;

    bool SentResultSet = false;

    ui64 RemainingRows = Max<ui64>();

    TQuotaState Quota;
    THashSet<ui64> QuotaNeeded;

    TInstant WallClockAccepted;
    TInstant WallClockAllocated;
    TInstant WallClockResolveStarted;
    TInstant WallClockResolved;
    TInstant WallClockPrepared;
    TInstant WallClockPlanned;

    TDuration ElapsedPrepareExec;
    TDuration ElapsedPrepareComplete;
    TDuration ElapsedExecExec;
    TDuration ElapsedExecComplete;
};

IActor* CreateReadTableSnapshotWorker(const TReadTableSettings& settings) {
    return new TReadTableWorker(settings);
}

} // namespace NTxProxy
} // namespace NKikimr
