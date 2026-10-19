#include "events.h"

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSql {

namespace {

class TSubqueryActor : public NActors::TActorBootstrapped<TSubqueryActor> {
public:
    TSubqueryActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, TVector<TSelectStmt> scalars, std::optional<TSelectStmt> fromSubquery, ui32 internalErrorRetries, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Text(std::move(text))
        , Statement(std::move(statement))
        , Scalars(std::move(scalars))
        , FromSubquery(std::move(fromSubquery))
        , InternalErrorRetries(internalErrorRetries)
        , LockTxId(lockTxId)
    {
    }

    void Bootstrap() {
        if (Statement.Join || (Statement.FromQuery && !FromSubquery)) {
            Fail("0A000", "not supported");
            return;
        }
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database));
        Become(&TSubqueryActor::StateWork);
    }

private:
    void Fail(TString sqlState, TString message) {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Succeed(TVector<TQueryColumn> columns, TVector<TVector<TQueryCell>> rows) {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "SELECT " + ToString(rows.size());
        result->Columns = std::move(columns);
        result->Rows = std::move(rows);
        result->Locks = std::move(Locks);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Run(const TSelectStmt& statement, const TString& text) {
        Register(CreateSelectActor(SelfId(), Database, text, statement, InternalErrorRetries, std::make_pair(SnapshotStep, SnapshotTxId), LockTxId));
    }

    TSelectStmt OuterTable() const {
        TSelectStmt outer = Statement;
        outer.Columns.clear();
        outer.ScalarOf.clear();
        outer.FromQuery = false;
        for (size_t i = 0; i < Statement.Columns.size(); ++i) {
            if (i < Statement.ScalarOf.size() && Statement.ScalarOf[i] >= 0) {
                continue;
            }
            outer.Columns.push_back(Statement.Columns[i]);
        }
        return outer;
    }

    void Start() {
        if (ScalarPos < Scalars.size()) {
            Phase = 0;
            Run(Scalars[ScalarPos], Text + " #scalar" + ToString(ScalarPos));
            return;
        }
        if (FromSubquery) {
            Phase = 1;
            Run(*FromSubquery, Text + " #from");
            return;
        }
        const TSelectStmt outer = OuterTable();
        if (outer.Columns.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        Phase = 2;
        Run(outer, Text + " #outer");
    }

    bool TakeScalar(TEvQueryResult& result) {
        if (result.Columns.size() != 1) {
            Fail("42601", "subquery must return only one column");
            return false;
        }
        if (result.Rows.size() > 1) {
            Fail("21000", "more than one row");
            return false;
        }
        if (result.Rows.empty()) {
            ScalarValues.push_back({true, {}});
        } else {
            ScalarValues.push_back(result.Rows[0][0]);
        }
        ++ScalarPos;
        return true;
    }

    bool Project(const TEvQueryResult& result, TVector<TQueryColumn>& columns, TVector<TVector<TQueryCell>>& rows) {
        TVector<int> source;
        for (size_t i = 0; i < Statement.Columns.size(); ++i) {
            if (i < Statement.ScalarOf.size() && Statement.ScalarOf[i] >= 0) {
                columns.push_back({Statement.Columns[i].Name.empty() ? TString("scalar") : Statement.Columns[i].Name, 25, -1});
                source.push_back(-1);
                continue;
            }
            int found = -2;
            for (size_t column = 0; column < result.Columns.size(); ++column) {
                if (result.Columns[column].Name == Statement.Columns[i].Name) {
                    found = static_cast<int>(column);
                    columns.push_back(result.Columns[column]);
                    break;
                }
            }
            if (found < 0) {
                Fail("42703", Statement.Columns[i].Name);
                return false;
            }
            source.push_back(found);
        }
        for (const auto& row : result.Rows) {
            TVector<TQueryCell> line;
            size_t real = 0;
            for (size_t i = 0; i < source.size(); ++i) {
                if (i < Statement.ScalarOf.size() && Statement.ScalarOf[i] >= 0) {
                    const int slot = Statement.ScalarOf[i];
                    line.push_back(slot >= 0 && static_cast<size_t>(slot) < ScalarValues.size() ? ScalarValues[slot] : TQueryCell{true, {}});
                    continue;
                }
                Y_UNUSED(real);
                line.push_back(row[source[i]]);
            }
            rows.push_back(std::move(line));
        }
        return true;
    }

    bool Stitch(TEvQueryResult& result, TVector<TQueryColumn>& columns, TVector<TVector<TQueryCell>>& rows) {
        size_t real = 0;
        for (size_t i = 0; i < Statement.Columns.size(); ++i) {
            if (i < Statement.ScalarOf.size() && Statement.ScalarOf[i] >= 0) {
                columns.push_back({"scalar", 25, -1});
                continue;
            }
            if (real >= result.Columns.size()) {
                Fail("42703", Statement.Columns[i].Name);
                return false;
            }
            columns.push_back(result.Columns[real]);
            ++real;
        }
        for (auto& row : result.Rows) {
            TVector<TQueryCell> line;
            size_t next = 0;
            for (size_t i = 0; i < Statement.Columns.size(); ++i) {
                if (i < Statement.ScalarOf.size() && Statement.ScalarOf[i] >= 0) {
                    const int slot = Statement.ScalarOf[i];
                    line.push_back(slot >= 0 && static_cast<size_t>(slot) < ScalarValues.size() ? ScalarValues[slot] : TQueryCell{true, {}});
                    continue;
                }
                line.push_back(std::move(row[next]));
                ++next;
            }
            rows.push_back(std::move(line));
        }
        return true;
    }

    void HandleSnapshot(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Fail("XX000", "internal error");
            return;
        }
        SnapshotStep = ev->Get()->Snapshot.Step;
        SnapshotTxId = ev->Get()->Snapshot.TxId;
        Start();
    }

    void HandleResult(TEvQueryResult::TPtr& ev) {
        for (auto& lock : ev->Get()->Locks) {
            Locks.push_back(std::move(lock));
        }
        if (!ev->Get()->Ok) {
            Fail(ev->Get()->SqlState.empty() ? TString("XX000") : ev->Get()->SqlState, ev->Get()->Message.empty() ? TString("internal error") : ev->Get()->Message);
            return;
        }
        if (Phase == 0) {
            if (!TakeScalar(*ev->Get())) {
                return;
            }
            Start();
            return;
        }
        TVector<TQueryColumn> columns;
        TVector<TVector<TQueryCell>> rows;
        if (Phase == 1) {
            if (!Project(*ev->Get(), columns, rows)) {
                return;
            }
            Succeed(std::move(columns), std::move(rows));
            return;
        }
        if (!Stitch(*ev->Get(), columns, rows)) {
            return;
        }
        Succeed(std::move(columns), std::move(rows));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, HandleSnapshot);
            hFunc(TEvQueryResult, HandleResult);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TString Text;
    const TSelectStmt Statement;
    const TVector<TSelectStmt> Scalars;
    const std::optional<TSelectStmt> FromSubquery;
    const ui32 InternalErrorRetries;
    const ui64 LockTxId;
    ui64 SnapshotStep = 0;
    ui64 SnapshotTxId = 0;
    TVector<TQueryCell> ScalarValues;
    TVector<NKikimrDataEvents::TLock> Locks;
    size_t ScalarPos = 0;
    int Phase = 0;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateSubqueryActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, TVector<TSelectStmt> scalars, std::optional<TSelectStmt> fromSubquery, ui32 internalErrorRetries, ui64 lockTxId) {
    return new TSubqueryActor(replyTo, std::move(database), std::move(text), std::move(statement), std::move(scalars), std::move(fromSubquery), internalErrorRetries, lockTxId);
}

} // namespace NKikimr::NSql
