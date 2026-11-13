#include "table_checksum.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/query_actor/query_actor.h>
#include <util/string/subst.h>
#include <util/string/builder.h>

namespace NKikimr::NBackup {
namespace {

    class TChecksumQuery : public TQueryBase {
    public:
        TChecksumQuery(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
            const TString& tablePath, bool importing, TRowVersion snapshot,
            std::shared_ptr<NKqp::TSnapshotHandle> snapshotHandle)
            : TQueryBase(importing ? NKikimrServices::IMPORT : NKikimrServices::EXPORT, {}, database, true)
            , ReplyTo(replyTo)
            , OperationId(operationId)
            , ItemIdx(itemIdx)
            , TablePath(tablePath)
            , SnapshotHandle(std::move(snapshotHandle))
        {
            RequestType = TString(NRequestTypes::ChecksumTable);
            MvccVersion = snapshot;
        }

    private:
        void OnRunQuery() override {
            if (Cancelled) {
                Finish(Ydb::StatusIds::CANCELLED, "table checksum query cancelled");
                return;
            }
            TString path = TablePath;
            SubstGlobal(path, "\\", "\\\\");
            SubstGlobal(path, "`", "\\`");
            // ChecksumTable bypasses the default ceiling only with an explicit timeout
            RunDataQuery(TStringBuilder()
                             << "SELECT count(*), SUM(Digest::CityHash(StablePickle(TableRow()))) FROM `" << path
                             << "`;", nullptr, TTxControl::BeginAndCommitTx(true), TDuration::Max());
        }

        void OnQueryResult() override {
            try {
                Y_ENSURE(ResultSets.size() == 1, "table checksum query returned an invalid result set count");
                const auto& result = ResultSets.front();
                Y_ENSURE(result.RowsCount() == 1 && result.ColumnsCount() == 2, "invalid table checksum result shape");
                NYdb::TResultSetParser parser(result);
                Y_ENSURE(parser.TryNextRow(), "empty table checksum result");
                Checksum.Count = parser.ColumnParser(0).GetUint64();
                const auto hash = parser.ColumnParser(1).GetOptionalUint64();
                Y_ENSURE(hash || Checksum.Count == 0, "NULL checksum for a nonempty table");
                Checksum.Hash = hash.value_or(0);
            } catch (const std::exception& e) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    TStringBuilder() << "invalid table checksum query result: " << e.what());
            }
            Finish();
        }

        void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
            const TString error = status != Ydb::StatusIds::SUCCESS && issues.Empty()
                                      ? TString(Ydb::StatusIds::StatusCode_Name(status))
                                      : issues.ToOneLineString();
            Send(ReplyTo, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, status, Checksum, error));
        }

        STFUNC(StateFunc) override {
            if (ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType) {
                if (Finished) {
                    return;
                }
                if (!SessionId.empty()) {
                    Finish(Ydb::StatusIds::CANCELLED, "table checksum query cancelled");
                } else {
                    Cancelled = true;
                }
                return;
            }
            TQueryBase::StateFunc(ev);
        }

        TActorId ReplyTo;
        ui64 OperationId;
        ui32 ItemIdx;
        TString TablePath;
        // Keep retention until the query and its session have finished closing
        std::shared_ptr<NKqp::TSnapshotHandle> SnapshotHandle;
        bool Cancelled = false;
        NBackup::TTableChecksum Checksum;
    };

    // Defers query creation until the owner has committed the required state
    class TChecksumQueryStarter : public TActorBootstrapped<TChecksumQueryStarter> {
    public:
        TChecksumQueryStarter(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
            const TString& tablePath, bool importing, const TTableId& tableId, TMaybe<TRowVersion> snapshot)
            : ReplyTo(replyTo)
            , OperationId(operationId)
            , ItemIdx(itemIdx)
            , Database(database)
            , TablePath(tablePath)
            , Importing(importing)
            , TableId(tableId)
            , Snapshot(snapshot)
        {}

        void Bootstrap() {
            Become(&TChecksumQueryStarter::StateFunc);
        }

    private:
        enum class ESnapshotState {
            Initial,
            CheckingSupport,
            Acquiring,
            Retained,
        };

        static constexpr TDuration RetryWindow = TDuration::Minutes(5);
        static constexpr TDuration InitialRetryDelay = TDuration::Seconds(1);
        static constexpr TDuration MaxRetryDelay = TDuration::Seconds(10);

        bool IsExportSnapshot() const {
            return !Importing && Snapshot && TableId.PathId;
        }

        void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
            Send(ReplyTo, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, status, {}, error));
            PassAway();
        }

        void SendRetentionRequest(THolder<IEventBase> request) {
            Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(request.Release(), TableId.PathId.OwnerId, true, RetentionRequestId),
                IEventHandle::FlagTrackDelivery, RetentionRequestId);
            Schedule(RetryWindow, new TEvents::TEvWakeup(RetentionRequestId));
        }

        void CheckExportRetentionSupport() {
            SnapshotState = ESnapshotState::CheckingSupport;
            ++RetentionRequestId;
            SendRetentionRequest(MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(TableId));
        }

        void CheckExportRetention() {
            SnapshotState = ESnapshotState::Acquiring;
            auto request = MakeHolder<TEvTableChecksum::TEvCheckRetentionRequest>();
            auto& record = request->Record;
            record.SetBackupTxId(OperationId);
            record.SetTableOwnerId(TableId.PathId.OwnerId);
            record.SetTableId(TableId.PathId.LocalPathId);
            record.SetSnapshotStep(Snapshot->Step);
            record.SetSnapshotTxId(Snapshot->TxId);
            record.SetRequestId(++RetentionRequestId);
            SendRetentionRequest(std::move(request));
        }

        void Start() {
            if (Query || SnapshotState == ESnapshotState::Acquiring ||
                SnapshotState == ESnapshotState::CheckingSupport) {
                return;
            }
            if (SnapshotState == ESnapshotState::Initial) {
                SnapshotState = ESnapshotState::Acquiring;
                if (IsExportSnapshot()) {
                    CheckExportRetentionSupport();
                    return;
                }
                auto request = MakeHolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>(
                    Database, TableId.PathId ? TVector<TTableId>{ TableId } : TVector<TTableId>{});
                request->Snapshot = Snapshot;
                Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), request.Release());
                return;
            }
            Query = Register(new TChecksumQuery(
                SelfId(), OperationId, ItemIdx, Database, TablePath, Importing, *Snapshot, SnapshotHandle));
        }

        void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
            SnapshotState = ESnapshotState::Initial;
            auto* result = ev->Get();
            if (result->Status != Ydb::StatusIds::SUCCESS) {
                if (!Retry(result->Status)) {
                    ReplyError(result->Status, result->Issues.ToOneLineString());
                }
                return;
            }
            if (!result->SnapshotHandle || !AppData()->FeatureFlags.GetEnableDataShardSnapshotLocking()) {
                return ReplyError(Ydb::StatusIds::PRECONDITION_FAILED,
                    "Table checksums require EnableSnapshotsLocking and EnableDataShardSnapshotLocking");
            }
            const bool persistSnapshot = Importing && !Snapshot;
            Snapshot = result->Snapshot;
            SnapshotHandle = std::make_shared<NKqp::TSnapshotHandle>(std::move(result->SnapshotHandle));
            SnapshotState = ESnapshotState::Retained;
            if (persistSnapshot) {
                Send(ReplyTo, new TEvTableChecksum::TEvSnapshotAcquired(OperationId, ItemIdx, *Snapshot));
                Become(&TChecksumQueryStarter::StateWaitSnapshotCommit);
                return;
            }
            Start();
        }

        void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
            if (SnapshotState != ESnapshotState::CheckingSupport || ev->Cookie != RetentionRequestId) {
                return;
            }
            const auto& result = ev->Get()->GetRecord();
            if (result.GetStatus() != NKikimrScheme::StatusSuccess) {
                SnapshotState = ESnapshotState::Initial;
                if (result.GetStatus() == NKikimrScheme::StatusNotAvailable && Retry(Ydb::StatusIds::UNAVAILABLE)) {
                    return;
                }
                return ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, result.GetReason());
            }
            if (!result.GetSupportsBackupSnapshotRetention()) {
                // An old SchemeShard answered a supported request but does not implement retention confirmation
                Send(ReplyTo, new TEvTableChecksum::TEvRetentionUnsupported());
                return PassAway();
            }
            CheckExportRetention();
        }

        void Handle(TEvTableChecksum::TEvCheckRetentionResponse::TPtr& ev) {
            const auto& result = ev->Get()->Record;
            if (!RetentionRequestId || SnapshotState != ESnapshotState::Acquiring ||
                result.GetRequestId() != RetentionRequestId) {
                return;
            }
            SnapshotState = ESnapshotState::Initial;
            const auto status = static_cast<Ydb::StatusIds::StatusCode>(result.GetStatus());
            if (status == Ydb::StatusIds::UNSUPPORTED) {
                Send(ReplyTo, new TEvTableChecksum::TEvRetentionUnsupported());
                return PassAway();
            }
            if (status != Ydb::StatusIds::SUCCESS) {
                if (!Retry(status)) {
                    ReplyError(status, result.GetError());
                }
                return;
            }
            if (!AppData()->FeatureFlags.GetEnableSnapshotsLocking() ||
                !AppData()->FeatureFlags.GetEnableDataShardSnapshotLocking()) {
                return ReplyError(Ydb::StatusIds::PRECONDITION_FAILED,
                    "Table checksums require EnableSnapshotsLocking and EnableDataShardSnapshotLocking");
            }
            // SchemeShard holds the original registration until the backup has saved the checksum
            SnapshotState = ESnapshotState::Retained;
            Start();
        }

        void RetentionUnavailable() {
            if (!RetentionRequestId ||
                (SnapshotState != ESnapshotState::Acquiring && SnapshotState != ESnapshotState::CheckingSupport)) {
                return;
            }
            SnapshotState = ESnapshotState::Initial;
            if (!Retry(Ydb::StatusIds::UNAVAILABLE)) {
                ReplyError(Ydb::StatusIds::UNAVAILABLE, "Cannot confirm export snapshot retention");
            }
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            if (ev->Cookie == RetentionRequestId) {
                RetentionUnavailable();
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            if (ev->Cookie == RetentionRequestId) {
                RetentionUnavailable();
            }
        }

        void Handle(TEvents::TEvWakeup::TPtr& ev) {
            if (!ev->Get()->Tag) {
                Start();
            } else if (ev->Get()->Tag == RetentionRequestId) {
                RetentionUnavailable();
            }
        }

        void OnSnapshotCommitted() {
            Become(&TChecksumQueryStarter::StateFunc);
            Start();
        }

        STATEFN(StateWaitSnapshotCommit) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvTableChecksum::TEvStart, OnSnapshotCommitted);
                sFunc(TEvents::TEvPoisonPill, PassAway);
            }
        }

        bool Retry(Ydb::StatusIds::StatusCode status) {
            if (TQueryRetryActorBase::Retryable(status) == ERetryErrorClass::NoRetry) {
                return false;
            }
            const auto now = TActivationContext::Monotonic();
            if (!RetryDeadline) {
                RetryDeadline = now + RetryWindow;
            }
            if (now >= RetryDeadline) {
                return false;
            }
            Schedule(Min(RetryBackoff.Next(), RetryDeadline - now), new TEvents::TEvWakeup());
            return true;
        }

        void Reply(TEvTableChecksum::TEvResult::TPtr& ev) {
            if (ev->Sender == Query) {
                Query = {};
                if (Retry(ev->Get()->Status)) {
                    if (IsExportSnapshot()) {
                        SnapshotState = ESnapshotState::Initial;
                    }
                    return;
                }
                Send(ReplyTo, ev->Release().Release());
                PassAway();
            }
        }

        void PassAway() override {
            if (Query) {
                Send(Query, new TEvents::TEvPoisonPill());
            }
            if (RetentionRequestId) {
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TableId.PathId.OwnerId));
            }
            TActorBootstrapped::PassAway();
        }

        STATEFN(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvWakeup, Handle);
                sFunc(TEvTableChecksum::TEvStart, Start);
                sFunc(TEvents::TEvPoisonPill, PassAway);
                hFunc(TEvTableChecksum::TEvResult, Reply);
                hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, Handle);
                hFunc(TEvTableChecksum::TEvCheckRetentionResponse, Handle);
                hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        TActorId ReplyTo;
        ui64 OperationId;
        ui32 ItemIdx;
        TString Database;
        TString TablePath;
        bool Importing;
        TTableId TableId;
        TMaybe<TRowVersion> Snapshot;
        std::shared_ptr<NKqp::TSnapshotHandle> SnapshotHandle;
        ESnapshotState SnapshotState = ESnapshotState::Initial;
        ui64 RetentionRequestId = 0;
        TActorId Query;
        TMonotonic RetryDeadline;
        TBackoff RetryBackoff{ InitialRetryDelay, MaxRetryDelay };
    };

} // namespace

IActor* CreateTableChecksumQuery(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
    const TString& tablePath, bool importing, const TTableId& tableId, TMaybe<TRowVersion> snapshot) {
    return new TChecksumQueryStarter(replyTo, operationId, itemIdx, database, tablePath, importing, tableId, snapshot);
}

} // namespace NKikimr::NBackup
