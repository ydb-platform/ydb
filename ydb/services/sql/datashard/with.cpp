#include "events.h"

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NSql {

namespace {

TString RowKey(const TVector<TQueryCell>& row) {
    TString key;
    for (const TQueryCell& cell : row) {
        key.push_back(cell.Null ? '\0' : '\1');
        key.append(cell.Text);
        key.push_back('\0');
    }
    return key;
}

class TWithActor : public NActors::TActorBootstrapped<TWithActor> {
public:
    TWithActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, std::optional<TSelectStmt> right, ECombine combine, TSelectStmt cte, TString cteName, ui32 internalErrorRetries, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Text(std::move(text))
        , Statement(std::move(statement))
        , Right(std::move(right))
        , Combine(combine)
        , Cte(std::move(cte))
        , CteName(std::move(cteName))
        , InternalErrorRetries(internalErrorRetries)
        , LockTxId(lockTxId)
    {
    }

    void Bootstrap() {
        if (!Uses(Statement) || (Right && !Uses(*Right)) || (Combine != ECombine::None && Combine != ECombine::Union && Combine != ECombine::UnionAll)) {
            Fail("0A000", "not supported");
            return;
        }
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database));
        Become(&TWithActor::StateWork);
    }

private:
    bool Uses(const TSelectStmt& statement) const {
        return statement.From.Name == CteName && !statement.Join && !statement.FromQuery && statement.Where.empty() && statement.Aggs.empty()
            && statement.Group.empty() && !statement.Distinct;
    }

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

    bool Project(const TSelectStmt& statement, TVector<TVector<TQueryCell>>& rows) {
        TVector<int> source;
        for (const TIdentifier& column : statement.Columns) {
            int found = -1;
            for (size_t i = 0; i < Columns.size(); ++i) {
                if (Columns[i].Name == column.Name) {
                    found = static_cast<int>(i);
                    break;
                }
            }
            if (found < 0) {
                Fail("42703", column.Name);
                return false;
            }
            source.push_back(found);
        }
        for (const auto& row : Cached) {
            TVector<TQueryCell> line;
            for (int index : source) {
                line.push_back(row[index]);
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
        Register(CreateSelectActor(SelfId(), Database, Text + " #cte", Cte, InternalErrorRetries, std::make_pair(ev->Get()->Snapshot.Step, ev->Get()->Snapshot.TxId), LockTxId));
    }

    void HandleResult(TEvQueryResult::TPtr& ev) {
        if (!ev->Get()->Ok) {
            Fail(ev->Get()->SqlState.empty() ? TString("XX000") : ev->Get()->SqlState, ev->Get()->Message.empty() ? TString("internal error") : ev->Get()->Message);
            return;
        }
        Columns = ev->Get()->Columns;
        Cached = ev->Get()->Rows;
        TVector<TVector<TQueryCell>> rows;
        if (!Project(Statement, rows)) {
            return;
        }
        if (Right) {
            TVector<TVector<TQueryCell>> extra;
            if (!Project(*Right, extra)) {
                return;
            }
            if (Combine == ECombine::UnionAll) {
                for (auto& row : extra) {
                    rows.push_back(std::move(row));
                }
            } else {
                THashSet<TString> seen;
                TVector<TVector<TQueryCell>> unique;
                for (auto& row : rows) {
                    if (seen.insert(RowKey(row)).second) {
                        unique.push_back(std::move(row));
                    }
                }
                for (auto& row : extra) {
                    if (seen.insert(RowKey(row)).second) {
                        unique.push_back(std::move(row));
                    }
                }
                rows = std::move(unique);
            }
        }
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "SELECT " + ToString(rows.size());
        result->Columns = Columns;
        if (!Statement.Columns.empty()) {
            result->Columns.clear();
            for (const TIdentifier& column : Statement.Columns) {
                for (const TQueryColumn& source : Columns) {
                    if (source.Name == column.Name) {
                        result->Columns.push_back(source);
                        break;
                    }
                }
            }
        }
        result->Rows = std::move(rows);
        result->Locks = std::move(ev->Get()->Locks);
        Send(ReplyTo, result.Release());
        PassAway();
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
    const std::optional<TSelectStmt> Right;
    const ECombine Combine;
    const TSelectStmt Cte;
    const TString CteName;
    const ui32 InternalErrorRetries;
    const ui64 LockTxId;
    TVector<TQueryColumn> Columns;
    TVector<TVector<TQueryCell>> Cached;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateWithActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, std::optional<TSelectStmt> right, ECombine combine, TSelectStmt cte, TString cteName, ui32 internalErrorRetries, ui64 lockTxId) {
    return new TWithActor(replyTo, std::move(database), std::move(text), std::move(statement), std::move(right), combine, std::move(cte), std::move(cteName), internalErrorRetries, lockTxId);
}

} // namespace NKikimr::NSql
