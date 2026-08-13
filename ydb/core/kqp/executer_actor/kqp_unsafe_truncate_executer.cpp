#include "kqp_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

namespace {

// Bounds the resolve->prepare loop below. A table that keeps splitting must eventually fail with
// a clear error rather than spin: the scan fetcher has an unbounded re-resolve on schema errors
// and it burns a core until the query times out.
constexpr ui32 MaxResolveAttempts = 10;

/*
 * Drives TRUNCATE TABLE ... WITH (unsafe = true).
 *
 * Terminology, kept apart on purpose:
 *   T_user  - the user transaction the statement runs inside. This actor never touches its
 *             TxManager or buffer actor; it only borrows its lock id so the shards spare it.
 *   T_trunc - the distributed transaction driven here. Own TxId, own TxManager, commits
 *             independently of T_user and is not rolled back with it.
 *
 * Flow: allocate TxId -> resolve shards -> prepare on every shard -> plan via coordinator ->
 * collect completions. Everything before the coordinator plan can be aborted cleanly, which is
 * where a concurrent split/merge is absorbed by re-resolving. After the plan there is no rollback
 * and the only failure the client can see is UNDETERMINED.
 */
class TKqpUnsafeTruncateExecuter: public TActorBootstrapped<TKqpUnsafeTruncateExecuter> {
    struct TTableToTruncate {
        TString Path;
        TTableId TableId;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
        bool Navigated = false;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpUnsafeTruncateExecuter(
            TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target, const TString& database,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken, ui64 userLockTxId,
            TIntrusivePtr<TUserRequestContext> requestContext, TTxAllocatorState::TPtr txAlloc)
        : PhyTx(std::move(phyTx))
        , Target(target)
        , Database(database)
        , UserToken(std::move(userToken))
        , UserLockTxId(userLockTxId)
        , RequestContext(std::move(requestContext))
        , TxAlloc(std::move(txAlloc))
    {
        YQL_ENSURE(PhyTx);
        YQL_ENSURE(PhyTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_UNSAFE_TRUNCATE);

        MainTablePath = PhyTx->GetUnsafeTruncate().GetTablePath();
        YQL_ENSURE(!MainTablePath.empty());

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(
            TxAlloc, TEvKqpExecuter::TEvTxResponse::EExecutionType::Data);
    }

    void Bootstrap() {
        Become(&TKqpUnsafeTruncateExecuter::PrepareState);
        AllocateTxIdAndResolve();
    }

private:
    void AllocateTxIdAndResolve() {
        ++ResolveAttempt;
        if (ResolveAttempt > MaxResolveAttempts) {
            ReplyError(Ydb::StatusIds::UNAVAILABLE, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Unsafe truncate could not settle on a shard set after "
                                 << MaxResolveAttempts << " attempts, the table keeps repartitioning");
            return;
        }

        // A fresh TxId per attempt: the previous one may be prepared on some shards and must not
        // be reused once we decided to abandon that attempt.
        TxManager = CreateKqpTransactionManager();
        Immediate = false;

        // Start over from the main table so that a repartitioning or re-indexing between attempts
        // is picked up rather than carried over.
        Tables.assign(1, TTableToTruncate{.Path = MainTablePath});

        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;
        // From here on results of the abandoned attempt are told apart by their TxId.
        Restarting = false;
        NavigateTables();
    }

    // Resolving shards needs the key column types, and this is also where the restrictions that
    // cannot be checked at compile time are enforced against the live schema.
    // Runs in rounds: the main table first, then the index impl tables it turned out to have.
    void NavigateTables() {
        PendingNavigate.clear();
        for (size_t i = 0; i < Tables.size(); ++i) {
            if (!Tables[i].Navigated) {
                PendingNavigate.push_back(i);
            }
        }

        if (PendingNavigate.empty()) {
            ResolveTables();
            return;
        }

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        // An empty token still fails the DescribeSchema ACL check with an empty SID, and the
        // scheme cache reports that as PathErrorUnknown.
        if (UserToken && !UserToken->GetSerializedToken().empty()) {
            request->UserToken = UserToken;
        }

        for (const size_t i : PendingNavigate) {
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = NKikimr::SplitPath(Tables[i].Path);
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            entry.ShowPrivatePath = true;
            entry.SyncVersion = true;
            request->ResultSet.emplace_back(std::move(entry));
        }

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& request = ev->Get()->Request;

        if (request->ResultSet.size() != PendingNavigate.size()) {
            ReplyError(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                "Unexpected navigate result size for unsafe truncate");
            return;
        }

        TVector<TString> discovered;
        for (size_t r = 0; r < request->ResultSet.size(); ++r) {
            const auto& entry = request->ResultSet[r];
            auto& table = Tables.at(PendingNavigate[r]);

            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                ReplyError(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    TStringBuilder() << "Failed to navigate " << table.Path << " for unsafe truncate: "
                                     << ToString(entry.Status));
                return;
            }

            if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
                ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssuesIds::KIKIMR_BAD_OPERATION,
                    TStringBuilder() << "Unsafe TRUNCATE TABLE supports row tables only, " << table.Path
                                     << " is not one");
                return;
            }

            if (!entry.CdcStreams.empty()) {
                ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssuesIds::KIKIMR_BAD_OPERATION,
                    TStringBuilder() << "Unsafe TRUNCATE TABLE is not supported for " << table.Path
                                     << ": it has a changefeed");
                return;
            }

            for (const auto& index : entry.Indexes) {
                if (index.GetType() != NKikimrSchemeOp::EIndexTypeGlobal) {
                    ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssuesIds::KIKIMR_BAD_OPERATION,
                        TStringBuilder() << "Unsafe TRUNCATE TABLE is not supported for " << table.Path
                                         << ": index " << index.GetName() << " is not a synchronous one");
                    return;
                }

                // The impl tables hold the index data and must be wiped in the same transaction,
                // otherwise the table and its index silently disagree.
                for (auto& implPath : NSchemeHelpers::CreateIndexTablePath(table.Path, NYql::TIndexDescription(index))) {
                    discovered.push_back(std::move(implPath));
                }
            }

            TVector<NScheme::TTypeInfo> keyColumnTypes;
            for (const auto& [_, column] : entry.Columns) {
                if (column.KeyOrder < 0) {
                    continue;
                }
                if (keyColumnTypes.size() <= static_cast<size_t>(column.KeyOrder)) {
                    keyColumnTypes.resize(column.KeyOrder + 1);
                }
                keyColumnTypes[column.KeyOrder] = column.PType;
            }

            if (keyColumnTypes.empty()) {
                ReplyError(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    TStringBuilder() << "Table " << table.Path << " has no key columns");
                return;
            }

            table.TableId = TTableId(entry.TableId.PathId.OwnerId, entry.TableId.PathId.LocalPathId,
                entry.TableId.SchemaVersion);
            table.KeyColumnTypes = std::move(keyColumnTypes);
            table.Navigated = true;
        }

        for (auto& path : discovered) {
            Tables.push_back(TTableToTruncate{.Path = std::move(path)});
        }

        // Another round if the tables just navigated brought in impl tables of their own.
        NavigateTables();
    }

    void ResolveTables() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->DatabaseName = Database;
        // An empty token still fails the DescribeSchema ACL check with an empty SID, and the
        // scheme cache reports that as PathErrorUnknown.
        if (UserToken && !UserToken->GetSerializedToken().empty()) {
            request->UserToken = UserToken;
        }

        for (const auto& table : Tables) {
            // A range covering the whole table, so every partition comes back.
            const TVector<TCell> minKey(table.KeyColumnTypes.size());
            const TTableRange range(minKey, true, {}, false, false);
            YQL_ENSURE(range.IsFullRange(table.KeyColumnTypes.size()));

            auto keyDesc = MakeHolder<TKeyDesc>(table.TableId, range, TKeyDesc::ERowOperation::Erase,
                table.KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{});
            auto& entry = request->ResultSet.emplace_back(std::move(keyDesc));
            // The row operation alone checks nothing: rights are taken from this field, which
            // defaults to zero. Without it DescribeSchema on the path would be enough to wipe the
            // table, while a plain DELETE of a single row is refused.
            entry.Access = NACLib::EAccessRights::EraseRow;
        }

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto& request = ev->Get()->Request;

        if (request->ErrorCount > 0) {
            TStringBuilder details;
            bool accessDenied = false;
            for (size_t i = 0; i < request->ResultSet.size(); ++i) {
                const auto& entry = request->ResultSet[i];
                if (entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
                    continue;
                }
                accessDenied = accessDenied
                    || entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::AccessDenied;
                details << " " << Tables.at(i).Path << ": " << ToString(entry.Status);
            }
            if (accessDenied) {
                ReplyError(Ydb::StatusIds::UNAUTHORIZED, NYql::TIssuesIds::KIKIMR_ACCESS_DENIED,
                    TStringBuilder() << "Access denied for unsafe TRUNCATE TABLE:" << details);
                return;
            }
            ReplyError(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "Failed to resolve tables for unsafe truncate:" << details);
            return;
        }

        ShardTables.clear();
        for (size_t i = 0; i < request->ResultSet.size(); ++i) {
            const auto& entry = request->ResultSet[i];
            const auto& table = Tables.at(i);

            if (entry.KeyDescription->GetPartitions().empty()) {
                ReplyError(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    TStringBuilder() << "No partitions to truncate in '" << table.Path << "'");
                return;
            }

            for (const auto& partition : entry.KeyDescription->GetPartitions()) {
                // Shards already confirmed truncated by an earlier attempt are skipped: the
                // operation is idempotent, so re-truncating a descendant of one is harmless, but
                // re-sending to an untouched survivor is pure waste.
                if (TruncatedShards.contains(partition.ShardId)) {
                    continue;
                }
                ShardTables[partition.ShardId].push_back(table.TableId);
            }
        }

        if (ShardTables.empty()) {
            // Everything this statement had to wipe is already wiped.
            ReplySuccess();
            return;
        }

        for (const auto& [shardId, tables] : ShardTables) {
            TxManager->AddShard(shardId, /* isOlap */ false, Tables.front().Path);
            TxManager->AddAction(shardId, IKqpTransactionManager::EAction::WRITE);
        }

        SendWrites();
    }

    void SendWrites() {
        // One shard needs no coordinator: an immediate write is already atomic by itself, and the
        // transaction manager explicitly forbids driving a distributed commit in that case
        // (see the CanUseImmediateCommit assert in StartExecute).
        Immediate = (ShardTables.size() == 1);

        if (!Immediate) {
            TxManager->StartPrepare();
        }

        const auto mode = Immediate
            ? NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE
            : NKikimrDataEvents::TEvWrite::MODE_PREPARE;

        for (const auto& [shardId, tables] : ShardTables) {
            auto evWrite = std::make_unique<NEvents::TDataEvents::TEvWrite>(*TxId, mode);

            for (const auto& tableId : tables) {
                evWrite->AddUnsafeTruncateOperation(tableId);
            }

            // Spare the locks of T_user so it survives its own statement. Everything else on the
            // shard is broken by the operation.
            if (UserLockTxId) {
                evWrite->Record.AddPreserveLockTxIds(UserLockTxId);
            }

            Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true));
        }
    }

    void Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetOrigin();

        // While a restart is under way TxId still names the abandoned attempt, so its remaining
        // shards would pass the check below and reach a transaction manager that has already been
        // replaced by a fresh one.
        if (Restarting || !TxId || record.GetTxId() != *TxId) {
            YDB_LOG_INFO("Ignoring a write result of an abandoned unsafe truncate attempt",
                {"txId", TxId ? *TxId : 0}, {"resultTxId", record.GetTxId()},
                {"shardId", shardId}, {"restarting", Restarting});
            return;
        }

        switch (record.GetStatus()) {
            case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
                IKqpTransactionManager::TPrepareResult result{
                    .ShardId = shardId,
                    .MinStep = record.GetMinStep(),
                    .MaxStep = record.GetMaxStep(),
                    .Coordinator = record.GetDomainCoordinators().empty()
                        ? 0
                        : TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                      record.GetDomainCoordinators().end())).Select(*TxId),
                };
                if (TxManager->ConsumePrepareTransactionResult(std::move(result))) {
                    SendCommitToCoordinator();
                }
                return;
            }

            case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
                TruncatedShards.insert(shardId);
                if (Immediate || TxManager->ConsumeCommitResult(shardId)) {
                    ReplySuccess();
                }
                return;
            }

            case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE:
            case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED:
            // A shard that has started splitting rejects everything with STATUS_OVERLOADED rather
            // than a distinctive status (see TExecutionUnit::CheckRejectDataTx), so an overload has
            // to be retried through a fresh resolve too. Plain overload converges on the attempt
            // cap instead of the fresh shard set.
            case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
                // The shard set or the schema moved under us. Nothing is applied yet, so the whole
                // attempt is dropped and retried against a freshly resolved shard set.
                RestartOrFail(record.GetStatus());
                return;
            }

            default:
                ReplyWriteError(record);
                return;
        }
    }

    void SendCommitToCoordinator() {
        TxManager->StartExecute();
        const auto commitInfo = TxManager->GetCommitInfo();

        auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
        YQL_ENSURE(commitInfo.Coordinator);
        ev->Record.SetCoordinatorID(commitInfo.Coordinator);

        auto& transaction = *ev->Record.MutableTransaction();
        transaction.SetTxId(*TxId);
        transaction.SetMinStep(commitInfo.MinStep);
        transaction.SetMaxStep(commitInfo.MaxStep);

        auto& affectedSet = *transaction.MutableAffectedSet();
        affectedSet.Reserve(commitInfo.ShardsInfo.size());
        for (const auto& shardInfo : commitInfo.ShardsInfo) {
            auto& item = *affectedSet.Add();
            item.SetTabletId(shardInfo.ShardId);
            item.SetFlags(shardInfo.AffectedFlags);
        }

        Planned = true;
        Become(&TKqpUnsafeTruncateExecuter::ExecuteState);
        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.Release(), commitInfo.Coordinator, /* subscribe */ true));
    }

    void Handle(TEvTxProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto status = ev->Get()->GetStatus();

        switch (status) {
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                // Progress on the way to the plan, not an outcome: the shards report that.
                return;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting:
                // An explicit refusal is definitive: the transaction was never planned, so no shard
                // applied anything and the attempt can be retried from scratch.
                Planned = false;
                RestartOrFail(NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
                return;

            default:
                ReplyError(Ydb::StatusIds::UNAVAILABLE, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Coordinator answered the unsafe truncate with an unknown status "
                                     << (int)status);
                return;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (!Planned) {
            // Pre-plan: nothing was applied anywhere, so a clean retry is safe.
            RestartOrFail(NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
            return;
        }
        ReplyError(Ydb::StatusIds::UNDETERMINED, NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
            TStringBuilder() << "Lost contact with tablet " << ev->Get()->TabletId
                             << " after the unsafe truncate was planned");
    }

    void RestartOrFail(NKikimrDataEvents::TEvWriteResult::EStatus status) {
        if (Planned) {
            ReplyError(Ydb::StatusIds::UNDETERMINED, NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                "Unsafe truncate lost a shard after it was planned");
            return;
        }
        if (Restarting) {
            return;
        }
        // Stays set until a new TxId is assigned, so that the remaining shards of the abandoned
        // attempt reporting the same failure do not each start a restart of their own.
        Restarting = true;
        YDB_LOG_INFO("Restarting unsafe truncate after a shard set change",
            {"txId", TxId ? *TxId : 0}, {"status", (int)status}, {"attempt", ResolveAttempt});

        // A coordinator refusal restarts us from ExecuteState, which does not expect the events of
        // a fresh attempt.
        Become(&TKqpUnsafeTruncateExecuter::PrepareState);
        AllocateTxIdAndResolve();
    }

    void ReplySuccess() {
        if (Replied) {
            return;
        }
        Replied = true;
        // Without this the response carries no status at all and the client sees STATUS_UNDEFINED.
        ResponseEv->Record.MutableResponse()->SetStatus(Ydb::StatusIds::SUCCESS);
        Send(Target, ResponseEv.release());
        PassAway();
    }

    void ReplyWriteError(const NKikimrDataEvents::TEvWriteResult& record) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        ReplyError(Ydb::StatusIds::ABORTED, NYql::TIssuesIds::DEFAULT_ERROR,
            TStringBuilder() << "Unsafe truncate failed on shard " << record.GetOrigin()
                             << ": " << issues.ToOneLineString());
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, NYql::TIssuesIds::EIssueCode issueCode, const TString& message) {
        // The response event is released on the first reply, so a second one would dereference it.
        if (Replied) {
            return;
        }
        Replied = true;
        auto issue = NYql::TIssue(message);
        NYql::SetIssueCode(issueCode, issue);
        ResponseEv->ResultsSize();
        ResponseEv->BrokenLockShardId = 0;
        ResponseEv->Record.MutableResponse()->SetStatus(status);
        NYql::IssueToMessage(issue, ResponseEv->Record.MutableResponse()->MutableIssues()->Add());
        Send(Target, ResponseEv.release());
        PassAway();
    }

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        const auto& msg = ev->Get()->Record;
        const NYql::TIssues issues = ev->Get()->GetIssues();

        YDB_LOG_INFO("Got EvAbortExecution for unsafe truncate",
            {"txId", TxId ? *TxId : 0}, {"planned", Planned},
            {"status", NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())},
            {"issues", issues.ToOneLineString()});

        if (Planned) {
            // The coordinator already holds the transaction. It cannot be taken back, and unlike an
            // ordinary write it is not rolled back with the user transaction either.
            ReplyError(Ydb::StatusIds::UNDETERMINED, NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                "Unsafe truncate was aborted after it had been planned and is not rolled back");
            return;
        }

        ReplyError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
            TStringBuilder() << "Unsafe truncate aborted before it was planned: " << issues.ToOneLineString());
    }

    void UnexpectedEvent(TStringBuf state, ui32 eventType) {
        ReplyError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::UNEXPECTED,
            TStringBuilder() << "Unexpected event " << eventType << " in TKqpUnsafeTruncateExecuter " << state);
    }

    STATEFN(PrepareState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("PrepareState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            ReplyError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::UNEXPECTED, e.what());
        }
    }

    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxy::TEvProposeTransactionStatus, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            ReplyError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::UNEXPECTED, e.what());
        }
    }

private:
    const TKqpPhyTxHolder::TConstPtr PhyTx;
    const TActorId Target;
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const ui64 UserLockTxId;
    const TIntrusivePtr<TUserRequestContext> RequestContext;
    const TTxAllocatorState::TPtr TxAlloc;

    TString MainTablePath;
    TVector<TTableToTruncate> Tables;
    TVector<size_t> PendingNavigate;
    THashMap<ui64, TVector<TTableId>> ShardTables;
    THashSet<ui64> TruncatedShards;

    IKqpTransactionManagerPtr TxManager;
    std::optional<ui64> TxId;
    ui32 ResolveAttempt = 0;
    bool Planned = false;
    bool Restarting = false;
    bool Replied = false;
    bool Immediate = false;

    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
};

} // anonymous namespace

IActor* CreateKqpUnsafeTruncateExecuter(
    TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, ui64 userLockTxId,
    TIntrusivePtr<TUserRequestContext> requestContext, TTxAllocatorState::TPtr txAlloc)
{
    return new TKqpUnsafeTruncateExecuter(
        std::move(phyTx), target, database, std::move(userToken), userLockTxId,
        std::move(requestContext), std::move(txAlloc));
}

} // namespace NKikimr::NKqp
