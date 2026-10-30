#include "schemata.h"

#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

bool KnownColumn(const TString& name) {
    return name == "catalog_name" || name == "schema_name";
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

class TSchemataActor : public NActors::TActorBootstrapped<TSchemataActor> {
public:
    TSchemataActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Catalog(TString(ExtractDomain(Database)))
    {
    }

    void Bootstrap() {
        if (Statement.Star) {
            Columns.push_back("catalog_name");
            Columns.push_back("schema_name");
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
            if (!SchemaName && comparison.Column.Name == "schema_name" && comparison.Op == ECompareOp::Eq) {
                SchemaName = comparison.Value.Text;
            }
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        if (SchemaName) {
            entry.Path = SplitPath(Database + "/" + *SchemaName);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        } else {
            entry.Path = SplitPath(Database);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        }
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TSchemataActor::StateNavigate);
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

    bool Matches(const TString& schemaName) const {
        for (const TComparison& comparison : Statement.Where) {
            const TString& left = comparison.Column.Name == "schema_name" ? schemaName : Catalog;
            const int compared = CompareText(left, comparison.Value.Text);
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

    void Succeed(TVector<TString> schemaNames) {
        if (Replied) {
            return;
        }
        Replied = true;
        auto result = MakeHolder<TEvQueryResult>();
        result->Ok = true;
        for (const TString& name : Columns) {
            result->Columns.push_back({name, 25, -1});
        }
        for (const TString& schemaName : schemaNames) {
            if (!Matches(schemaName)) {
                continue;
            }
            TVector<TQueryCell> row;
            row.reserve(Columns.size());
            for (const TString& name : Columns) {
                row.push_back({false, name == "schema_name" ? schemaName : Catalog});
            }
            result->Rows.push_back(std::move(row));
            if (Statement.Limit && result->Rows.size() >= *Statement.Limit) {
                break;
            }
        }
        Send(ReplyTo, result.Release());
        PassAway();
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const bool ok = request && request->ResultSet.size() == 1
            && request->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        if (SchemaName) {
            if (!ok || request->ResultSet[0].Kind != NSchemeCache::TSchemeCacheNavigate::KindPath) {
                Succeed({});
                return;
            }
            Succeed({*SchemaName});
            return;
        }
        if (!ok) {
            Fail("42P01", Database);
            return;
        }
        TVector<TString> names;
        const auto& list = request->ResultSet[0].ListNodeEntry;
        if (list) {
            for (const auto& child : list->Children) {
                if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindPath) {
                    names.push_back(child.Name);
                }
            }
        }
        Succeed(std::move(names));
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
    std::optional<TString> SchemaName;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateSchemataActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement) {
    return new TSchemataActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
