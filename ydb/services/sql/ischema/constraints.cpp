#include "constraints.h"

#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/cast.h>

#include <algorithm>

namespace NKikimr::NSql {

namespace {

bool KnownColumn(const TString& name, bool usage) {
    if (name == "constraint_catalog" || name == "constraint_schema" || name == "constraint_name"
        || name == "table_name" || name == "constraint_type")
    {
        return true;
    }
    return usage && (name == "column_name" || name == "ordinal_position");
}

class TConstraintsActor : public NActors::TActorBootstrapped<TConstraintsActor> {
public:
    TConstraintsActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Catalog(TString(ExtractDomain(Database)))
        , Usage(Statement.From.Name == "key_column_usage")
    {
    }

    void Bootstrap() {
        if (Statement.Star) {
            Columns = {"constraint_catalog", "constraint_schema", "constraint_name", "table_name", "constraint_type"};
            if (Usage) {
                Columns.push_back("column_name");
                Columns.push_back("ordinal_position");
            }
        } else {
            for (const TIdentifier& name : Statement.Columns) {
                if (!KnownColumn(name.Name, Usage)) {
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
            if (!KnownColumn(comparison.Column.Name, Usage)) {
                Fail("0A000", "not supported");
                return;
            }
            if (!SchemaFilter && comparison.Column.Name == "constraint_schema" && comparison.Op == ECompareOp::Eq) {
                SchemaFilter = comparison.Value.Text;
            }
            if (!NameFilter && comparison.Column.Name == "table_name" && comparison.Op == ECompareOp::Eq) {
                NameFilter = comparison.Value.Text;
            }
        }
        Become(&TConstraintsActor::StateNavigate);
        if (SchemaFilter && NameFilter) {
            Targets.push_back({*SchemaFilter, *NameFilter});
            DescribeNext();
            return;
        }
        if (SchemaFilter && !SchemaFilter->empty()) {
            SendList(Database + "/" + *SchemaFilter, *SchemaFilter, false);
        } else {
            SendList(Database, {}, true);
        }
    }

private:
    struct TTarget {
        TString Schema;
        TString Name;
    };

    struct TFound {
        TString Schema;
        TString Table;
        TString Name;
        TString Type;
        TString Column;
        TString Position;
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
        Listing = true;
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

    void DescribeNext() {
        if (Targets.empty()) {
            Succeed();
            return;
        }
        const TTarget target = Targets.back();
        Targets.pop_back();
        Listing = false;
        DescribedSchema = target.Schema;
        DescribedName = target.Name;
        const TString path = target.Schema.empty()
            ? Database + "/" + target.Name
            : Database + "/" + target.Schema + "/" + target.Name;
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    TString Cell(const TFound& row, const TString& column) const {
        if (column == "constraint_schema") {
            return row.Schema;
        }
        if (column == "constraint_name") {
            return row.Name;
        }
        if (column == "table_name") {
            return row.Table;
        }
        if (column == "constraint_type") {
            return row.Type;
        }
        if (column == "column_name") {
            return row.Column;
        }
        if (column == "ordinal_position") {
            return row.Position;
        }
        return Catalog;
    }

    bool Matches(const TFound& row) const {
        for (const TComparison& comparison : Statement.Where) {
            const TString left = Cell(row, comparison.Column.Name);
            const int compared = left < comparison.Value.Text ? -1 : (left == comparison.Value.Text ? 0 : 1);
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
            DescribeNext();
            return;
        }
        const TString name = Dirs.back();
        Dirs.pop_back();
        SendList(Database + "/" + name, name, false);
    }

    void AddConstraint(const TString& name, const TString& type, const TVector<TString>& keyColumns) {
        if (!Usage) {
            Found.push_back({DescribedSchema, DescribedName, name, type, {}, {}});
            return;
        }
        ui32 position = 1;
        for (const TString& column : keyColumns) {
            Found.push_back({DescribedSchema, DescribedName, name, type, column, ToString(position++)});
        }
    }

    void TakeTable(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        TVector<const TSysTables::TTableColumnInfo*> keys;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            if (column.KeyOrder >= 0) {
                keys.push_back(&column);
            }
        }
        std::sort(keys.begin(), keys.end(), [](const auto* left, const auto* right) {
            return left->KeyOrder < right->KeyOrder;
        });
        if (!keys.empty()) {
            TVector<TString> names;
            for (const auto* column : keys) {
                names.push_back(column->Name);
            }
            AddConstraint(DescribedName + "_pkey", "PRIMARY KEY", names);
        }
        for (const auto& index : entry.Indexes) {
            if (index.GetType() != NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                continue;
            }
            TVector<TString> names;
            for (const auto& column : index.GetKeyColumnNames()) {
                names.push_back(column);
            }
            AddConstraint(index.GetName(), "UNIQUE", names);
        }
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const bool ok = request && request->ResultSet.size() == 1
            && request->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        if (!Listing) {
            if (ok && request->ResultSet[0].Kind == NSchemeCache::TSchemeCacheNavigate::KindTable) {
                TakeTable(request->ResultSet[0]);
            }
            DescribeNext();
            return;
        }
        if (!ok) {
            if (ListingRoot) {
                Fail("42P01", Database);
            } else if (SchemaFilter) {
                DescribeNext();
            } else {
                NextDirectory();
            }
            return;
        }
        const auto& list = request->ResultSet[0].ListNodeEntry;
        if (list) {
            for (const auto& child : list->Children) {
                const bool table = child.Kind == NSchemeCache::TSchemeCacheNavigate::KindTable
                    || child.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable;
                if (table && (!NameFilter || *NameFilter == child.Name) && (!SchemaFilter || *SchemaFilter == CurrentSchema)) {
                    Targets.push_back({CurrentSchema, child.Name});
                } else if (!SchemaFilter && ListingRoot && child.Kind == NSchemeCache::TSchemeCacheNavigate::KindPath) {
                    Dirs.push_back(child.Name);
                }
            }
        }
        if (SchemaFilter) {
            DescribeNext();
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
    const bool Usage;
    TVector<TString> Columns;
    std::optional<TString> SchemaFilter;
    std::optional<TString> NameFilter;
    TVector<TTarget> Targets;
    TVector<TString> Dirs;
    TVector<TFound> Found;
    TString CurrentSchema;
    TString DescribedSchema;
    TString DescribedName;
    bool Listing = false;
    bool ListingRoot = false;
    bool Replied = false;
};

} // namespace

NActors::IActor* CreateConstraintsActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement) {
    return new TConstraintsActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
