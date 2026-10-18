#include "events.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

TString TablePath(const TString& database, const TSelectStmt& statement) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (statement.Schema) {
        path += "/" + statement.Schema->Name;
    }
    path += "/" + statement.From.Name;
    return path;
}

class TSelectAnalyzer : public NActors::TActorBootstrapped<TSelectAnalyzer> {
public:
    TSelectAnalyzer(const NActors::TActorId& replyTo, TString database, TSelectStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        if (Database.empty()) {
            Fail("3D000", "database is required");
            return;
        }
        Path = TablePath(Database, Statement);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TSelectAnalyzer::StateWait);
    }

private:
    void Fail(TString sqlState, TString message) {
        auto result = MakeHolder<TEvAnalyzeResult>();
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->Path = Path;
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void Succeed(TVector<ui32> columnIds) {
        auto result = MakeHolder<TEvAnalyzeResult>();
        result->Ok = true;
        result->Path = Path;
        result->ColumnIds = std::move(columnIds);
        Send(ReplyTo, result.Release());
        PassAway();
    }

    const TSysTables::TTableColumnInfo* FindColumn(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& name) const {
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.Name == name) {
                return &column;
            }
        }
        return nullptr;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1 || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Fail("42P01", Path);
            return;
        }
        const auto& entry = request->ResultSet[0];
        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            Fail("0A000", "column table");
            return;
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            Fail("42P01", Path);
            return;
        }

        TVector<TString> names;
        if (!Statement.Star) {
            for (const TIdentifier& column : Statement.Columns) {
                names.push_back(column.Name);
            }
        }
        for (const TComparison& comparison : Statement.Where) {
            names.push_back(comparison.Column.Name);
        }

        TVector<ui32> ids;
        if (Statement.Star) {
            for (const auto& [id, column] : entry.Columns) {
                Y_UNUSED(column);
                ids.push_back(id);
            }
        }
        for (const TString& name : names) {
            const auto* column = FindColumn(entry, name);
            if (!column) {
                Fail("42703", name);
                return;
            }
            if (!Statement.Star) {
                ids.push_back(column->Id);
            }
        }
        Succeed(std::move(ids));
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TSelectStmt Statement;
    TString Path;
};

} // namespace

NActors::IActor* CreateSelectAnalyzer(const NActors::TActorId& replyTo, TString database, TSelectStmt statement) {
    return new TSelectAnalyzer(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
