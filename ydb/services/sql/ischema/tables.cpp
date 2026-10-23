#include "tables.h"

#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

bool KnownColumn(const TString& name) {
    return name == "table_catalog" || name == "table_schema" || name == "table_name" || name == "table_type";
}

int CompareText(const TString& left, const TString& right) {
    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

class TTablesActor : public NActors::TActorBootstrapped<TTablesActor> {
public:
    TTablesActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Catalog(TString(ExtractDomain(Database)))
    {
    }

    void Bootstrap() {
        if (Statement.Star) {
            Columns = {"table_catalog", "table_schema", "table_name", "table_type"};
        } else {
            for (const TIdentifier& name : Statement.Columns) {
                if (!KnownColumn(name.Name)) {
                    Fail("0A000", "not supported");
                    return;
                }
                Columns.push_back(name.Name);
            }
        }
        if (Columns.empty()) {
            Fail("0A000", "not supported");
            return;
        }
        for (const TComparison& comparison : Statement.Where) {
            if (!KnownColumn(comparison.Column.Name)) {
                Fail("0A000", "not supported");
                return;
            }
            if (!SchemaFilter && comparison.Column.Name == "table_schema" && comparison.Op == ECompareOp::Eq) {
                SchemaFilter = comparison.Value.Text;
            }
        }

        if (SchemaFilter && !SchemaFilter->empty()) {
            SendList(Database + "/" + *SchemaFilter, *SchemaFilter, false);
        } else {
            SendList(Database, {}, true);
        }
        Become(&TTablesActor::StateNavigate);
    }

private:
    struct TFound {
        TString Schema;
        TString Name;
    };

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

    void SendList(const TString& path, TString schema, bool root) {
        ListingRoot = root;
        CurrentSchema = std::move(schema);
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    TString Cell(const TFound& row, const TString& column) const {
        if (column == "table_schema") {
            return row.Schema;
        }
        if (column == "table_name") {
            return row.Name;
        }
        if (column == "table_type") {
            return "BASE TABLE";
        }
        return Catalog;
    }

    bool Matches(const TFound& row) const {
        for (const TComparison& comparison : Statement.Where) {
            const int compared = CompareText(Cell(row, comparison.Column.Name), comparison.Value.Text);
            switch (comparison.Op) {
                case ECompareOp::Eq:
                    if (compared != 0) {
                        return false;
                    }
                    break;
                case ECompareOp::NotEq:
                    if (compared == 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Lt:
                    if (compared >= 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Gt:
                    if (compared <= 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Le:
                    if (compared > 0) {
                        return false;
                    }
                    break;
                case ECompareOp::Ge:
                    if (compared < 0) {
                        return false;
                    }
                    break;
            }
        }
        return true;
    }

    void Succeed() {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        for (const TString& name : Columns) {
            result->Columns.push_back({name, 25, -1});
        }
        for (const TFound& row : Found) {
            if (!Matches(row)) {
                continue;
            }
            TVector<TQueryCell> line;
            line.reserve(Columns.size());
            for (const TString& name : Columns) {
                line.push_back({false, Cell(row, name)});
            }
            result->Rows.push_back(std::move(line));
            if (Statement.Limit && result->Rows.size() >= *Statement.Limit) {
                break;
            }
        }
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void NextDirectory() {
        if (Dirs.empty()) {
            Succeed();
            return;
        }
        const TString name = Dirs.back();
        Dirs.pop_back();
        SendList(Database + "/" + name, name, false);
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const bool ok = request && request->ResultSet.size() == 1
            && request->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        if (!ok) {
            if (ListingRoot) {
                Fail("42P01", Database);
            } else if (SchemaFilter) {
                Succeed();
            } else {
                NextDirectory();
            }
            return;
        }
        const auto& list = request->ResultSet[0].ListNodeEntry;
        if (list) {
            for (const auto& child : list->Children) {
                if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindTable
                    || child.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable)
                {
                    Found.push_back({CurrentSchema, child.Name});
                } else if (!SchemaFilter && ListingRoot && child.Kind == NSchemeCache::TSchemeCacheNavigate::KindPath) {
                    Dirs.push_back(child.Name);
                }
            }
        }
        if (SchemaFilter) {
            Succeed();
            return;
        }
        NextDirectory();
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TSelectStmt Statement;
    const TString Catalog;
    TVector<TString> Columns;
    std::optional<TString> SchemaFilter;
    TVector<TFound> Found;
    TVector<TString> Dirs;
    TString CurrentSchema;
    bool ListingRoot = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateTablesActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement) {
    return new TTablesActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
