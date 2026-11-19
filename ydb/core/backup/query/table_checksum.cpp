#include "table_checksum.h"
#include "table_checksum_snapshot.h"
#include <ydb/core/backup/common/checksum.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/actors/core/log.h>
#include <util/string/subst.h>
#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/generic/algorithm.h>

namespace NKikimr::NBackup {

TString GetTableChecksumSchemaHash(const NKikimrSchemeOp::TTableDescription& table) {
    TVector<const NKikimrSchemeOp::TColumnDescription*> columns;
    for (const auto& column : table.GetColumns()) {
        columns.push_back(&column);
    }
    SortBy(columns, [](const auto* column) {
        return column->GetName();
    });
    NKikimrSchemeOp::TTableDescription schema;
    for (const auto* column : columns) {
        auto& copy = *schema.AddColumns();
        copy.SetId(column->GetId());
        copy.SetName(column->GetName());
        copy.SetType(column->GetType());
        copy.SetTypeId(column->GetTypeId());
        copy.SetNotNull(column->GetNotNull());
        if (column->HasTypeInfo()) {
            *copy.MutableTypeInfo() = column->GetTypeInfo();
        }
    }
    return ComputeChecksum(schema.SerializeAsString());
}

namespace {

    class TChecksumQuery : public TQueryBase {
    public:
        TChecksumQuery(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
            const TString& tablePath, bool importing, TRowVersion snapshot, const TString& algorithm)
            : TQueryBase(importing ? NKikimrServices::IMPORT : NKikimrServices::EXPORT, {}, database, true)
            , ReplyTo(replyTo)
            , OperationId(operationId)
            , ItemIdx(itemIdx)
            , TablePath(tablePath)
            , Algorithm(algorithm)
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
            TStringBuilder query;
            query << "CHECKSUM TABLE `" << path << "`";
            if (!Algorithm.empty()) {
                query << " WITH (ALGORITHM = \"" << EscapeC(Algorithm) << "\")";
            }
            query << ";";
            // ChecksumTable bypasses the default ceiling only with an explicit timeout
            RunDataQuery(query, nullptr, TTxControl::BeginAndCommitTx(true), TDuration::Max());
        }

        void OnQueryResult() override {
            try {
                Y_ENSURE(ResultSets.size() == 1, "table checksum query returned an invalid result set count");
                const auto& result = ResultSets.front();
                Y_ENSURE(result.RowsCount() == 1 && result.ColumnsCount() == 4, "invalid table checksum result shape");
                NYdb::TResultSetParser parser(result);
                Y_ENSURE(parser.TryNextRow(), "empty table checksum result");
                Y_ENSURE(parser.ColumnParser("version").GetUint32() == TTableChecksum::Version,
                    "unsupported table checksum result version");
                Checksum.Algorithm = parser.ColumnParser("algorithm").GetUtf8();
                Y_ENSURE(!Checksum.Algorithm.empty(), "empty table checksum result algorithm");
                Checksum.Count = parser.ColumnParser("count").GetUint64();
                Checksum.Hash = parser.ColumnParser("hash").GetString();
                Y_ENSURE(Checksum.Count || Checksum.Hash.find_first_not_of('0') == TString::npos,
                    "nonzero checksum for an empty table");
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
        TString Algorithm;
        bool Cancelled = false;
        NBackup::TTableChecksum Checksum;
    };

    // Defers query creation until the owner has committed the required state
    class TChecksumQueryStarter : public TActorBootstrapped<TChecksumQueryStarter> {
    public:
        TChecksumQueryStarter(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
            const TString& tablePath, bool importing, const TTableId& tableId, ui64 schemeShardId, TRowVersion snapshot,
            const TString& algorithm, const TString& sourceSchemaHash, const TTableId& copyTableId)
            : ReplyTo(replyTo)
            , OperationId(operationId)
            , ItemIdx(itemIdx)
            , Database(database)
            , TablePath(tablePath)
            , Importing(importing)
            , TableId(tableId)
            , CopyTableId(copyTableId)
            , SchemeShardId(schemeShardId)
            , Snapshot(snapshot)
            , Algorithm(algorithm)
            , SourceSchemaHash(sourceSchemaHash)
        {}

        void Bootstrap() {
            Become(&TChecksumQueryStarter::StateFunc);
            if (Snapshot.Step) {
                RetainSnapshot();
            }
        }

    private:
        enum class ESnapshotState {
            Initial,
            CheckingSource,
            CheckingCopy,
            CheckingShards,
            Retained,
            CheckingResult,
            ResultReported,
        };

        static constexpr TDuration RetryWindow = TDuration::Minutes(5);
        static constexpr TDuration InitialRetryDelay = TDuration::Seconds(1);
        static constexpr TDuration MaxRetryDelay = TDuration::Seconds(10);

        void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
            Send(ReplyTo, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, status, {}, error));
            FinishResult();
        }

        void FinishResult() {
            // The owner stops us after committing the comparison or finishing the upload
            SnapshotState = ESnapshotState::ResultReported;
        }

        void RetainSnapshot() {
            if (!Snapshot.Step || Snapshot.Step == Max<ui64>() || !Snapshot.TxId || SnapshotRequested ||
                (!Importing && !CopyTableId.PathId)) {
                return;
            }
            TVector<TTableId> tables{ TableId };
            if (!Importing) {
                tables.push_back(CopyTableId);
            }
            SnapshotRequested = true;
            Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
                new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database, std::move(tables), Snapshot));
        }

        void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
            auto& result = *ev->Get();
            if (!SnapshotRequested) {
                return;
            }
            if (result.Status != Ydb::StatusIds::SUCCESS) {
                // Durable DataShard pins cover recovery outside the registration window
                LOG_WARN_S(TActivationContext::AsActorContext(),
                    Importing ? NKikimrServices::IMPORT : NKikimrServices::EXPORT,
                    "Cannot register table checksum snapshot; using durable pins: " << result.Issues.ToOneLineString());
            } else if (result.Snapshot == Snapshot) {
                SnapshotHandle = std::move(result.SnapshotHandle);
            }
        }

        void CheckExportSource(bool afterQuery) {
            SnapshotState = afterQuery ? ESnapshotState::CheckingResult : ESnapshotState::CheckingSource;
            ++SourceRequestId;
            auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(TablePath);
            request->Record.MutableOptions()->SetReturnChildren(false);
            request->Record.MutableOptions()->SetReturnPartitioningInfo(false);
            request->Record.MutableOptions()->SetReturnPartitionConfig(false);
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true,
                                                    SourceRequestId), IEventHandle::FlagTrackDelivery, SourceRequestId);
            Schedule(RetryWindow, new TEvents::TEvWakeup(SourceRequestId));
        }

        void ReplySourceChanged(const TString& code, const TString& message) {
            TTableChecksum checksum{ .Error = TTableChecksum::TError{ .Code = code, .Message = message } };
            Send(ReplyTo, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, Ydb::StatusIds::SUCCESS, checksum, ""));
            FinishResult();
        }

        void Start() {
            if (Query || (SnapshotState != ESnapshotState::Initial && SnapshotState != ESnapshotState::Retained)) {
                return;
            }
            if (SnapshotState == ESnapshotState::Initial) {
                if (!TableId.PathId || !SchemeShardId || !Snapshot.TxId || (!Importing && !Snapshot.Step)) {
                    return ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, "Table checksum checkpoint is missing");
                }
                if (!Importing) {
                    CheckExportSource(false);
                    return;
                }
                ConfirmSnapshot();
                return;
            }
            QueryStartedAt = TActivationContext::Monotonic();
            Query = Register(new TChecksumQuery(
                SelfId(), OperationId, ItemIdx, Database, TablePath, Importing, Snapshot, Algorithm));
        }

        void ConfirmSnapshot(bool copy = false) {
            SnapshotState = copy ? ESnapshotState::CheckingCopy : ESnapshotState::CheckingShards;
            SnapshotReader = Register(CreateTableChecksumSnapshotReader(
                SelfId(), copy ? CopyTableId : TableId, SchemeShardId, Snapshot, copy));
        }

        void Handle(TEvTableChecksum::TEvSnapshotConfirmed::TPtr& ev) {
            if (ev->Sender != SnapshotReader) {
                return;
            }
            const bool copy = SnapshotState == ESnapshotState::CheckingCopy;
            SnapshotReader = {};
            SnapshotState = ESnapshotState::Initial;
            const auto& result = *ev->Get();
            if (result.Unsupported) {
                if (Importing) {
                    Send(ReplyTo,
                        new TEvTableChecksum::TEvResult(OperationId, ItemIdx, Ydb::StatusIds::SUCCESS, {}, "", true));
                } else {
                    Send(ReplyTo, new TEvTableChecksum::TEvRetentionUnsupported());
                }
                return FinishResult();
            }
            if (result.Status != Ydb::StatusIds::SUCCESS) {
                if (Retry(result.Status)) {
                    return;
                }
                if (!Importing) {
                    PendingSnapshotFailure = std::move(ev);
                    CheckExportSource(false);
                } else {
                    ReplyError(result.Status, result.Error);
                }
                return;
            }
            if (copy) {
                ConfirmSnapshot();
                return;
            }
            Snapshot = result.Snapshot;
            if (Importing) {
                RetainSnapshot();
            }
            SnapshotState = ESnapshotState::Retained;
            Start();
        }

        void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
            if (ev->Cookie != SourceRequestId) {
                return;
            }
            if (SnapshotState != ESnapshotState::CheckingSource && SnapshotState != ESnapshotState::CheckingResult) {
                return;
            }
            const auto& result = ev->Get()->GetRecord();
            if (result.GetStatus() == NKikimrScheme::StatusNotAvailable) {
                return SourceUnavailable();
            }
            if (result.GetStatus() == NKikimrScheme::StatusPathDoesNotExist ||
                result.GetStatus() == NKikimrScheme::StatusPathIsNotDirectory) {
                return ReplySourceChanged(
                    "SOURCE_TABLE_CHANGED", "Source table is no longer available at " + TablePath);
            }
            if (result.GetStatus() != NKikimrScheme::StatusSuccess) {
                return ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, result.GetReason());
            }
            const auto& description = result.GetPathDescription();
            const auto& self = description.GetSelf();
            if (TPathId(self.GetSchemeshardId(), self.GetPathId()) != TableId.PathId) {
                return ReplySourceChanged("SOURCE_TABLE_CHANGED", "Source table identity changed at " + TablePath);
            }
            const auto version = description.GetTable().GetTableSchemaVersion();
            if (SourceSchemaHash.empty()) {
                return ReplySourceChanged(
                    "SOURCE_SCHEMA_UNAVAILABLE", "Backup task does not contain a captured source schema");
            }
            if (GetTableChecksumSchemaHash(description.GetTable()) != SourceSchemaHash ||
                (SnapshotState == ESnapshotState::CheckingResult && version != QuerySchemaVersion)) {
                return ReplySourceChanged("SOURCE_SCHEMA_CHANGED",
                    TStringBuilder() << "Source schema changed after the backup copy was created at " << TablePath
                                     << " (current version " << version << ")");
            }
            const bool afterQuery = SnapshotState == ESnapshotState::CheckingResult;
            QuerySchemaVersion = version;
            SnapshotState = ESnapshotState::Retained;
            if (afterQuery) {
                FinishQueryResult();
            } else if (PendingSnapshotFailure) {
                ReplyError(PendingSnapshotFailure->Get()->Status, PendingSnapshotFailure->Get()->Error);
            } else {
                ConfirmSnapshot(bool(CopyTableId.PathId));
            }
        }

        void SourceUnavailable() {
            if (!SourceRequestId ||
                (SnapshotState != ESnapshotState::CheckingSource && SnapshotState != ESnapshotState::CheckingResult)) {
                return;
            }
            SnapshotState = ESnapshotState::Initial;
            PendingResult.Reset();
            if (!Retry(Ydb::StatusIds::UNAVAILABLE)) {
                ReplyError(Ydb::StatusIds::UNAVAILABLE, "Cannot check table checksum source");
            }
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            if (ev->Cookie == SourceRequestId) {
                SourceUnavailable();
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            if (ev->Cookie == SourceRequestId) {
                SourceUnavailable();
            }
        }

        void Handle(TEvents::TEvWakeup::TPtr& ev) {
            if (!ev->Get()->Tag) {
                Start();
            } else if (ev->Get()->Tag == SourceRequestId) {
                SourceUnavailable();
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

        void FinishQueryResult() {
            auto ev = std::move(PendingResult);
            if (Retry(ev->Get()->Status)) {
                if (!Importing) {
                    SnapshotState = ESnapshotState::Initial;
                }
                return;
            }
            Send(ReplyTo, ev->Release().Release());
            FinishResult();
        }

        void Reply(TEvTableChecksum::TEvResult::TPtr& ev) {
            if (ev->Sender != Query) {
                return;
            }
            if (TActivationContext::Monotonic() - QueryStartedAt >= RetryWindow) {
                // A long-running attempt separates consecutive failure bursts
                RetryDeadline = TMonotonic();
                RetryBackoff.Reset();
            }
            Query = {};
            PendingResult = std::move(ev);
            if (!Importing) {
                CheckExportSource(true);
            } else {
                FinishQueryResult();
            }
        }

        void PassAway() override {
            if (SnapshotReader) {
                Send(SnapshotReader, new TEvents::TEvPoisonPill());
            }
            if (Query) {
                Send(Query, new TEvents::TEvPoisonPill());
            }
            if (SourceRequestId && !Importing) {
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(SchemeShardId));
            }
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvTableChecksum::TEvFinish::TPtr& ev) {
            if (ev->Sender != ReplyTo) {
                return;
            }
            if (Importing && TableId.PathId && Snapshot.TxId) {
                Register(CreateTableChecksumSnapshotReleaser(TableId, SchemeShardId, Snapshot));
            }
            PassAway();
        }

        STATEFN(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvWakeup, Handle);
                sFunc(TEvTableChecksum::TEvStart, Start);
                hFunc(TEvTableChecksum::TEvFinish, Handle);
                sFunc(TEvents::TEvPoisonPill, PassAway);
                hFunc(TEvTableChecksum::TEvResult, Reply);
                hFunc(TEvTableChecksum::TEvSnapshotConfirmed, Handle);
                hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, Handle);
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
        TTableId CopyTableId;
        const ui64 SchemeShardId;
        TRowVersion Snapshot;
        NKqp::TSnapshotHandle SnapshotHandle;
        bool SnapshotRequested = false;
        TString Algorithm;
        TString SourceSchemaHash;
        ui64 QuerySchemaVersion = 0;
        ESnapshotState SnapshotState = ESnapshotState::Initial;
        ui64 SourceRequestId = 0;
        TActorId Query;
        TMonotonic QueryStartedAt;
        TActorId SnapshotReader;
        TEvTableChecksum::TEvResult::TPtr PendingResult;
        TEvTableChecksum::TEvSnapshotConfirmed::TPtr PendingSnapshotFailure;
        TMonotonic RetryDeadline;
        TBackoff RetryBackoff{ InitialRetryDelay, MaxRetryDelay };
    };

} // namespace

IActor* CreateTableChecksumQuery(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
    const TString& tablePath, bool importing, const TTableId& tableId, ui64 schemeShardId, TRowVersion snapshot,
    const TString& algorithm, const TString& sourceSchemaHash, const TTableId& copyTableId) {
    return new TChecksumQueryStarter(replyTo, operationId, itemIdx, database, tablePath, importing, tableId,
        schemeShardId, snapshot, algorithm, sourceSchemaHash, copyTableId);
}

} // namespace NKikimr::NBackup
