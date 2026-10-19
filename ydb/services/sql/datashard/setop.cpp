#include "events.h"

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

class TSetOpActor : public NActors::TActorBootstrapped<TSetOpActor> {
public:
    TSetOpActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt left, TSelectStmt right, ECombine combine, ui32 internalErrorRetries, ui64 lockTxId)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Text(std::move(text))
        , Left(std::move(left))
        , Right(std::move(right))
        , Combine(combine)
        , InternalErrorRetries(internalErrorRetries)
        , LockTxId(lockTxId)
    {
    }

    void Bootstrap() {
        if (Combine != ECombine::Union && Combine != ECombine::UnionAll && Combine != ECombine::Intersect) {
            Fail("0A000", "not supported");
            return;
        }
        if (Catalog(Left) || Catalog(Right)) {
            Fail("0A000", "not supported");
            return;
        }
        Run(Left, Text + " #left");
    }

private:
    bool Catalog(const TSelectStmt& statement) const {
        return statement.Schema && (statement.Schema->Name == "information_schema" || statement.Schema->Name == "pg_catalog");
    }

    void Run(const TSelectStmt& statement, const TString& text) {
        if (statement.Join) {
            Register(CreateJoinActor(SelfId(), Database, statement, InternalErrorRetries, LockTxId));
        } else {
            Register(CreateSelectActor(SelfId(), Database, text, statement, InternalErrorRetries, std::nullopt, LockTxId));
        }
        Become(&TSetOpActor::StateWork);
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

    void Succeed() {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        result->CommandTag = "SELECT " + ToString(Rows.size());
        result->Columns = std::move(Columns);
        result->Rows = std::move(Rows);
        result->Locks = std::move(Locks);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void HandleResult(TEvQueryResult::TPtr& ev) {
        if (!ev->Get()->Ok) {
            Fail(ev->Get()->SqlState.empty() ? TString("XX000") : ev->Get()->SqlState, ev->Get()->Message.empty() ? TString("internal error") : ev->Get()->Message);
            return;
        }
        for (auto& lock : ev->Get()->Locks) {
            Locks.push_back(std::move(lock));
        }
        if (!HaveLeft) {
            Columns = ev->Get()->Columns;
            LeftRows = std::move(ev->Get()->Rows);
            HaveLeft = true;
            Run(Right, Text + " #right");
            return;
        }
        if (ev->Get()->Columns.size() != Columns.size()) {
            Fail("42601", "each UNION query must have the same number of columns");
            return;
        }
        Rows = std::move(LeftRows);
        if (Combine == ECombine::Intersect) {
            THashSet<TString> rightKeys;
            for (const auto& row : ev->Get()->Rows) {
                rightKeys.insert(RowKey(row));
            }
            TVector<TVector<TQueryCell>> shared;
            THashSet<TString> seen;
            for (auto& row : Rows) {
                const TString key = RowKey(row);
                if (!rightKeys.contains(key) || !seen.insert(key).second) {
                    continue;
                }
                shared.push_back(std::move(row));
            }
            Rows = std::move(shared);
        } else if (Combine == ECombine::UnionAll) {
            for (auto& row : ev->Get()->Rows) {
                Rows.push_back(std::move(row));
            }
        } else {
            THashSet<TString> seen;
            for (const auto& row : Rows) {
                seen.insert(RowKey(row));
            }
            for (auto& row : ev->Get()->Rows) {
                if (!seen.insert(RowKey(row)).second) {
                    continue;
                }
                Rows.push_back(std::move(row));
            }
            TVector<TVector<TQueryCell>> unique;
            THashSet<TString> kept;
            for (auto& row : Rows) {
                if (!kept.insert(RowKey(row)).second) {
                    continue;
                }
                unique.push_back(std::move(row));
            }
            Rows = std::move(unique);
        }
        Succeed();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQueryResult, HandleResult);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TString Text;
    const TSelectStmt Left;
    const TSelectStmt Right;
    const ECombine Combine;
    const ui32 InternalErrorRetries;
    const ui64 LockTxId;
    TVector<TQueryColumn> Columns;
    TVector<TVector<TQueryCell>> LeftRows;
    TVector<TVector<TQueryCell>> Rows;
    TVector<NKikimrDataEvents::TLock> Locks;
    bool HaveLeft = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateSetOpActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt left, TSelectStmt right, ECombine combine, ui32 internalErrorRetries, ui64 lockTxId) {
    return new TSetOpActor(replyTo, std::move(database), std::move(text), std::move(left), std::move(right), combine, internalErrorRetries, lockTxId);
}

} // namespace NKikimr::NSql
