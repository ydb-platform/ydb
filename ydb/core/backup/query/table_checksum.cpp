#include "table_checksum.h"

#include <ydb/library/query_actor/query_actor.h>
#include <util/string/subst.h>
#include <util/string/builder.h>
#include <atomic>

namespace NKikimr::NBackup {
namespace {

    // Empty integration hooks for the separately implemented snapshot support
    void AcquireChecksumSnapshotStub() {}

    void RetainChecksumSnapshotStub() {}

    void ExecuteChecksumSnapshotStub() {}

    void ReleaseChecksumSnapshotStub() {}

    class TChecksumQuery : public TQueryBase {
    public:
        TChecksumQuery(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
            const TString& tablePath, bool importing)
            : TQueryBase(importing ? NKikimrServices::IMPORT : NKikimrServices::EXPORT, {}, database, true)
            , ReplyTo(replyTo)
            , OperationId(operationId)
            , ItemIdx(itemIdx)
            , TablePath(tablePath)
        {}

    private:
        void OnRunQuery() override {
            if (Cancelled) {
                Finish(Ydb::StatusIds::CANCELLED, "table checksum query cancelled");
                return;
            }
            AcquireChecksumSnapshotStub();
            RetainChecksumSnapshotStub();
            TString path = TablePath;
            SubstGlobal(path, "\\", "\\\\");
            SubstGlobal(path, "`", "\\`");
            ExecuteChecksumSnapshotStub();
            RunDataQuery(TStringBuilder() << "SELECT count(*), SUM(Digest::CityHash(StablePickle(TableRow()))) FROM `"
                                          << path << "`;", nullptr, TTxControl::BeginAndCommitTx(true));
        }

        void OnQueryResult() override {
            Y_ENSURE(ResultSets.size() == 1, "table checksum query returned an invalid result set count");
            const auto& result = ResultSets.front();
            Y_ENSURE(result.RowsCount() == 1 && result.ColumnsCount() == 2, "invalid table checksum result shape");
            NYdb::TResultSetParser parser(result);
            Y_ENSURE(parser.TryNextRow(), "empty table checksum result");
            Checksum.Count = parser.ColumnParser(0).GetUint64();
            const auto hash = parser.ColumnParser(1).GetOptionalUint64();
            Y_ENSURE(hash || Checksum.Count == 0, "NULL checksum for a nonempty table");
            Checksum.Hash = hash.value_or(0);
            Finish();
        }

        void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
            ReleaseChecksumSnapshotStub();
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
        bool Cancelled = false;
        NBackup::TTableChecksum Checksum;
    };

    // Defers query creation until the owner has committed the required state
    class TChecksumQueryStarter : public TActorBootstrapped<TChecksumQueryStarter> {
    public:
        TChecksumQueryStarter(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
            const TString& tablePath, bool importing)
            : ReplyTo(replyTo)
            , OperationId(operationId)
            , ItemIdx(itemIdx)
            , Database(database)
            , TablePath(tablePath)
            , Importing(importing)
        {}

        ~TChecksumQueryStarter() override {
            if (Admitted) {
                ActiveQueries.fetch_sub(1, std::memory_order_relaxed);
            }
        }

        void Bootstrap() {
            Become(&TChecksumQueryStarter::StateFunc);
        }

    private:
        void Start() {
            if (!Admitted) {
                ui32 active = ActiveQueries.load(std::memory_order_relaxed);
                do {
                    if (active >= 4) {
                        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
                        return;
                    }
                } while (!ActiveQueries.compare_exchange_weak(active, active + 1, std::memory_order_relaxed));
                Admitted = true;
            }
            if (!Query) {
                ++Attempts;
                Query = Register(new TChecksumQuery(SelfId(), OperationId, ItemIdx, Database, TablePath, Importing));
            }
        }

        void Reply(TEvTableChecksum::TEvResult::TPtr& ev) {
            if (ev->Sender == Query) {
                Query = {};
                const auto status = ev->Get()->Status;
                if (Attempts < 3 && (status == Ydb::StatusIds::OVERLOADED || status == Ydb::StatusIds::UNAVAILABLE ||
                                        status == Ydb::StatusIds::ABORTED)) {
                    Schedule(TDuration::MilliSeconds(100 * Attempts), new TEvents::TEvWakeup());
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
            TActorBootstrapped::PassAway();
        }

        STATEFN(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvents::TEvWakeup, Start);
                sFunc(TEvTableChecksum::TEvStart, Start);
                sFunc(TEvents::TEvPoisonPill, PassAway);
                hFunc(TEvTableChecksum::TEvResult, Reply);
            }
        }

        TActorId ReplyTo;
        ui64 OperationId;
        ui32 ItemIdx;
        TString Database;
        TString TablePath;
        bool Importing;
        TActorId Query;
        ui32 Attempts = 0;
        bool Admitted = false;
        inline static std::atomic<ui32> ActiveQueries = 0;
    };

} // namespace

IActor* CreateTableChecksumQuery(TActorId replyTo, ui64 operationId, ui32 itemIdx, const TString& database,
    const TString& tablePath, bool importing) {
    return new TChecksumQueryStarter(replyTo, operationId, itemIdx, database, tablePath, importing);
}

} // namespace NKikimr::NBackup
