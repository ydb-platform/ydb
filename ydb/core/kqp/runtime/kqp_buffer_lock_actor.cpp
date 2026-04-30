#include "kqp_buffer_lock_actor.h"
#include "kqp_buffer_lookup_actor.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/common/kqp_locks_tli_helpers.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_stream_lock_worker.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>


namespace NKikimr {
namespace NKqp {

namespace {

class TKqpBufferLockActor : public NActors::TActorBootstrapped<TKqpBufferLockActor>, public IKqpBufferTableLock {
private:
    struct TEvPrivate {
        enum EEv {
            EvRetryLock = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryLock : public TEventLocal<TEvRetryLock, EvRetryLock> {
            explicit TEvRetryLock(ui64 requestId)
                : RequestId(requestId) {
            }

            const ui64 RequestId;
        };
    };

    struct TLockState {
        std::unique_ptr<TKqpStreamLockWorker> Worker;
        ui64 LocksInflight = 0;
        TVector<NKikimrDataEvents::TLock> CollectedLocks;
        std::vector<std::pair<TOwnedCellVec, bool>> CollectedRows;

        bool IsAllLocksFinished() const {
            return LocksInflight == 0;
        }
    };

    struct TShardState {
        bool HasPipe = false;
    };

    struct TRequestState {
        const ui64 LockCookie;
        const ui64 ShardId;
        bool Blocked = false;
        ui64 RetryAttempts = 0;
    };

public:
    TKqpBufferLockActor(TKqpBufferLockSettings&& settings)
        : Settings(std::move(settings))
        , Partitioning(Settings.TxManager->GetPartitioning(Settings.TableId))
        , LogPrefix(TStringBuilder() << "Table: `" << Settings.TablePath << "` (" << Settings.TableId << "), "
            << "SessionActorId: " << Settings.SessionActorId)
        , LockActorSpan(TWilsonKqp::LockActor, std::move(Settings.ParentTraceId), "LockActor") {
    }

    void Bootstrap() {
        CA_LOG_D("Start buffer lock actor");

        Settings.Counters->StreamLookupActorsCount->Inc();
        Become(&TKqpBufferLockActor::StateFunc);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_LOCK_ACTOR";

    void PassAway() final {
        Settings.Counters->StreamLookupActorsCount->Dec();

        AFL_ENSURE(Settings.Alloc);
        {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Settings.Alloc);
            CookieToLockState.clear();
        }

        for (const auto& [requestId, state] : LockIdToState) {
            Settings.Counters->SentIteratorCancels->Inc();
            auto cancel = MakeHolder<NEvents::TDataEvents::TEvLockRowsCancel>();
            cancel->Record.SetRequestId(requestId);
            Send(PipeCacheId, new TEvPipeCache::TEvForward(cancel.Release(), state.ShardId, false));
        }
        LockIdToState.clear();

        Unlink();

        TActorBootstrapped<TKqpBufferLockActor>::PassAway();

        LockActorSpan.End();
    }

    void Terminate() override {
        PassAway();
    }

    void Unlink() override {
        AFL_ENSURE(LockIdToState.empty());

        for (auto& [_, state] : ShardToState) {
            state.HasPipe = false;
        }
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NEvents::TDataEvents::TEvLockRowsResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvRetryLock, Handle);
            default:
                RuntimeError(
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                    TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite());
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                "Memory limit exceeded at stream lock");
        } catch (const yexception& e) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                e.what());
        }
    }

    void SetLockSettings(
            ui64 cookie,
            TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns) override
    {
        TKqpStreamLockSettings lockSettings(Settings.HolderFactory);
        lockSettings.Table.SetOwnerId(Settings.TableId.PathId.OwnerId);
        lockSettings.Table.SetTableId(Settings.TableId.PathId.LocalPathId);
        lockSettings.Table.SetVersion(Settings.TableId.SchemaVersion);

        for (const auto& keyColumn : keyColumns) {
            lockSettings.KeyColumns.push_back(keyColumn);
            lockSettings.Columns.push_back(keyColumn);
        }

        lockSettings.LockTxId = Settings.LockTxId;
        lockSettings.LockNodeId = Settings.LockNodeId;
        lockSettings.LockMode = Settings.LockMode;
        lockSettings.QuerySpanId = Settings.QuerySpanId;

        if (Settings.MvccSnapshot) {
            lockSettings.Snapshot = *Settings.MvccSnapshot;
        }

        if (KeyColumnTypes.empty()) {
            for (const auto& keyColumn : keyColumns) {
                NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
                KeyColumnTypes.push_back(typeInfo);
            }
        } else {
            AFL_ENSURE(KeyColumnTypes.size() == keyColumns.size());
        }

        AFL_ENSURE(CookieToLockState.emplace(
                cookie,
                TLockState{
                    .Worker = CreateStreamLockWorker(std::move(lockSettings)),
                    .LocksInflight = 0,
                }
            ).second);
    }

    void AddLockTask(ui64 cookie, const std::vector<TConstArrayRef<TCell>>& rows) override {
        auto& state = CookieToLockState.at(cookie);
        auto& worker = state.Worker;

        AFL_ENSURE(state.LocksInflight == 0);

        for (const auto& row : rows) {
            worker->AddInputRow(row);
        }

        StartLockTask(cookie, state);
    }

    void StartLockTask(ui64 cookie, TLockState& state) {
        auto& worker = state.Worker;
        auto requests = worker->BuildLockRequests(Partitioning, LockRequestId);

        for (auto& [shardId, lockRequest] : requests) {
            ++state.LocksInflight;
            StartLockRequest(cookie, shardId, std::move(lockRequest));
        }
    }

    bool HasResult(ui64 cookie) override {
        const auto& state = CookieToLockState.at(cookie);
        return state.LocksInflight == 0 && !state.CollectedLocks.empty();
    }

    bool IsEmpty(ui64 cookie) override {
        const auto& state = CookieToLockState.at(cookie);
        return state.LocksInflight == 0 && state.CollectedLocks.empty();
    }

    void ExtractResult(ui64 cookie, std::function<void(const TOwnedCellVec& row, bool modified)>&& callback) override {
        AFL_ENSURE(HasResult(cookie) || IsEmpty(cookie));
        auto& state = CookieToLockState.at(cookie);

        for (const auto& [row, modified] : state.CollectedRows) {
            callback(row, modified);
        }

        state.CollectedRows.clear();
        LockRowsCount += state.CollectedRows.size();
    }

    TTableId GetTableId() const override {
        return Settings.TableId;
    }

    const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const override {
        return KeyColumnTypes;
    }

    void StartLockRequest(ui64 cookie, ui64 shardId, THolder<NEvents::TDataEvents::TEvLockRows> request) {
        Settings.Counters->CreatedIterators->Inc();
        auto& record = request->Record;

        CA_LOG_D("Start locking of table: " << Settings.TablePath << ", requestId: " << record.GetRequestId()
            << ", shardId: " << shardId);

        Settings.TxManager->AddShard(shardId, false, Settings.TablePath);
        Settings.TxManager->AddAction(shardId, IKqpTransactionManager::EAction::WRITE);

        auto& shardState = ShardToState[shardId];
        const bool needToCreatePipe = !shardState.HasPipe;

        const ui64 requestId = record.GetRequestId();

        Send(PipeCacheId,
            new TEvPipeCache::TEvForward(
                request.Release(),
                shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = needToCreatePipe,
                    .Subscribe = needToCreatePipe,
                }),
            IEventHandle::FlagTrackDelivery,
            0,
            LockActorSpan.GetTraceId());

        shardState.HasPipe = true;

        AFL_ENSURE(LockIdToState.emplace(
            requestId,
            TRequestState {
                .LockCookie = cookie,
                .ShardId = shardId,
                .Blocked = false,
            }).second);
    }

    void Handle(NEvents::TDataEvents::TEvLockRowsResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto requestIt = LockIdToState.find(record.GetRequestId());
        if (requestIt == LockIdToState.end() || requestIt->second.Blocked) {
            CA_LOG_D("Drop lock with requestId: " << record.GetRequestId() << ", because it's already completed or blocked");
            return;
        }

        Settings.TxManager->AddParticipantNode(ev->Sender.NodeId());

        auto& requestState = requestIt->second;
        const auto shardId = requestState.ShardId;
        const auto cookie = requestState.LockCookie;

        Y_UNUSED(shardId);

        auto& lockState = CookieToLockState.at(cookie);
        AFL_ENSURE(lockState.Worker);
        AFL_ENSURE(lockState.LocksInflight > 0);

        CA_LOG_D("Recv TEvLockRowsResult (buffer lock) from ShardID=" << shardId
            << ", Table = " << Settings.TablePath
            << ", RequestId=" << record.GetRequestId()
            << ", Status=" << NKikimrDataEvents::TEvLockRowsResult::EStatus_Name(record.GetStatus()));

        auto getIssues = [&record]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            return issues;
        };

        switch (record.GetStatus()) {
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS:
                break;
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN: {
                CA_LOG_D("STATUS_LOCKS_BROKEN from shard: " << shardId);
                BrokenLocksCount += record.GetLocks().size();
                Settings.TxManager->SetError(shardId);
                RuntimeError(NYql::NDqProto::StatusIds::ABORTED,
                    NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                    MakeLockInvalidatedMessage(Settings.TxManager, Settings.TablePath));
                return;
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED: {
                CA_LOG_D("STATUS_OVERLOADED from shard: " << shardId);
                if (!RetryLockRequest(record.GetRequestId(), false)) {
                    return RuntimeError(
                        NYql::NDqProto::StatusIds::OVERLOADED,
                        NYql::TIssuesIds::KIKIMR_OVERLOADED,
                        TStringBuilder() << "Table: `" << Settings.TablePath << "` retry limit exceeded.",
                        getIssues());
                }
                return;
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_DEADLOCK: {
                CA_LOG_D("STATUS_DEADLOCK from shard: " << shardId);
                return RuntimeError(
                    NYql::NDqProto::StatusIds::ABORTED,
                    NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                    "Deadlock detected",
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_ERROR:
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_CHANGED: {
                return RuntimeError(
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                    "Scheme error",
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_INTERNAL_ERROR: {
                return RuntimeError(
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                    TStringBuilder() << "Internal error",
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST:{
                return RuntimeError(
                    NYql::NDqProto::StatusIds::BAD_REQUEST,
                    NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    "Bad request",
                    getIssues());
            }
            case NKikimrDataEvents::TEvLockRowsResult::STATUS_WRONG_SHARD_STATE: {
                return RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    "Wrong shard state.",
                    getIssues());
            }
            default: {
                return RuntimeError(
                    NYql::NDqProto::StatusIds::ABORTED,
                    NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                    "Lock request aborted",
                    getIssues());
            }
        }

        for (const auto& lock : record.GetLocks()) {
            AFL_ENSURE(Settings.TxManager->AddLock(shardId, lock, Settings.QuerySpanId));
            lockState.CollectedLocks.push_back(lock);
        }

        lockState.Worker->AddLockResult(record.GetRequestId(), ev->Get());

        lockState.Worker->ProcessRowsByLockResult(record.GetRequestId(),
            [&](const TOwnedCellVec& row, bool modified) {
                lockState.CollectedRows.emplace_back(row, modified);
            });

        --lockState.LocksInflight;
        LockIdToState.erase(requestIt);

        Settings.Callbacks->OnLookupTaskFinished();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        ShardToState.at(ev->Get()->TabletId).HasPipe = false;

        TVector<ui64> toRetry;
        for (const auto& [requestId, requestState] : LockIdToState) {
            if (requestState.ShardId == ev->Get()->TabletId && !requestState.Blocked) {
                Settings.Counters->IteratorDeliveryProblems->Inc();
                toRetry.push_back(requestId);
            }
        }

        for (const auto& requestId : toRetry) {
            if (!RetryLockRequest(requestId, true)) {
                const auto& failedRequest = LockIdToState.at(requestId);
                Y_UNUSED(failedRequest);
                return RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Table: `" << Settings.TablePath << "` retry limit exceeded.",
                    {});
            }
        }
    }

    void Handle(TEvPrivate::TEvRetryLock::TPtr& ev) {
        const ui64 failedRequestId = ev->Get()->RequestId;
        auto requestIt = LockIdToState.find(failedRequestId);
        if (requestIt == LockIdToState.end()) {
            CA_LOG_D("received retry request for already finished/non-existing request, request_id: " << failedRequestId);
            return;
        }

        auto& failedRequest = requestIt->second;
        auto& lockState = CookieToLockState.at(failedRequest.LockCookie);
        DoRetryLockRequest(failedRequestId, lockState, failedRequest);
    }

    bool RetryLockRequest(const ui64 failedRequestId, bool allowInstantRetry) {
        auto& failedRequest = LockIdToState.at(failedRequestId);
        auto& lockState = CookieToLockState.at(failedRequest.LockCookie);
        CA_LOG_D("Retry locking of table: " << Settings.TablePath << ", failedRequestId: " << failedRequestId
            << ", shardId: " << failedRequest.ShardId);
        failedRequest.Blocked = true;

        if (failedRequest.RetryAttempts >= MaxShardRetries()) {
            return false;
        }

        ++failedRequest.RetryAttempts;
        auto delay = CalcDelay(failedRequest.RetryAttempts, allowInstantRetry);
        if (delay == TDuration::Zero()) {
            DoRetryLockRequest(failedRequestId, lockState, failedRequest);
        } else {
            TlsActivationContext->Schedule(
                delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryLock(failedRequestId))
            );
        }

        return true;
    }

    void DoRetryLockRequest(const ui64 failedRequestId, TLockState& lockState, TRequestState& failedRequest) {
        AFL_ENSURE(failedRequest.Blocked);
        --lockState.LocksInflight;
        const auto guard = Settings.TypeEnv.BindAllocator();
        auto requests = lockState.Worker->RebuildLockRequest(failedRequestId, LockRequestId);
        for (auto& request : requests) {
            const ui64 newRequestId = request.second->Record.GetRequestId();
            ++lockState.LocksInflight;
            StartLockRequest(failedRequest.LockCookie, failedRequest.ShardId, std::move(request.second));
            LockIdToState.at(newRequestId).RetryAttempts = failedRequest.RetryAttempts;
        }
        LockIdToState.erase(failedRequestId);
    }

    void RuntimeError(
            NYql::NDqProto::StatusIds::StatusCode statusCode,
            NYql::EYqlIssueCode id,
            const TString& message,
            const NYql::TIssues& subIssues = {}) {
        if (LockActorSpan) {
            LockActorSpan.EndError(message);
        }
        Settings.Callbacks->OnLookupError(statusCode, id, message, subIssues);
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats) override {
        NYql::NDqProto::TDqTableStats* tableStats = nullptr;
        for (size_t i = 0; i < stats->TablesSize(); ++i) {
            auto* table = stats->MutableTables(i);
            if (table->GetTablePath() == Settings.TablePath) {
                tableStats = table;
            }
        }
        if (!tableStats) {
            tableStats = stats->AddTables();
            tableStats->SetTablePath(Settings.TablePath);
        }

        tableStats->SetReadRows(tableStats->GetReadRows() + LockRowsCount);
        LockRowsCount = 0;

        if (BrokenLocksCount > 0) {
            NKqpProto::TKqpTaskExtraStats extraStats;
            if (stats->HasExtra()) {
                stats->GetExtra().UnpackTo(&extraStats);
            }
            extraStats.MutableLockStats()->SetBrokenAsVictim(
                extraStats.GetLockStats().GetBrokenAsVictim() + BrokenLocksCount);
            stats->MutableExtra()->PackFrom(extraStats);
            BrokenLocksCount = 0;
        }
    }

private:
    TKqpBufferLockSettings Settings;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    const TString LogPrefix;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;

    const TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

    THashMap<ui64, TLockState> CookieToLockState;
    THashMap<ui64, TShardState> ShardToState;
    THashMap<ui64, TRequestState> LockIdToState;

    ui64 LockRequestId = 0;

    ui64 LockRowsCount = 0;
    ui64 BrokenLocksCount = 0;

    NWilson::TSpan LockActorSpan;
};

}

std::pair<IKqpBufferTableLock*, NActors::IActor*> CreateKqpBufferTableLock(TKqpBufferLockSettings&& settings) {
    auto* ptr = new TKqpBufferLockActor(std::move(settings));
    return {ptr, ptr};
}

}
}
