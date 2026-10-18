#include "columns.h"

#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/string/cast.h>

#include <algorithm>

namespace NKikimr::NSql {

namespace {

bool KnownColumn(const TString& name) {
    return name == "table_catalog" || name == "table_schema" || name == "table_name" || name == "column_name"
        || name == "ordinal_position" || name == "data_type" || name == "is_nullable" || name == "column_default";
}

TString PgTypeName(const TString& ydb) {
    if (ydb == "Bool") {
        return "bool";
    }
    if (ydb == "Int16") {
        return "int2";
    }
    if (ydb == "Int32") {
        return "int4";
    }
    if (ydb == "Int64") {
        return "int8";
    }
    if (ydb == "Float") {
        return "float4";
    }
    if (ydb == "Double") {
        return "float8";
    }
    if (ydb == "Utf8") {
        return "text";
    }
    if (ydb == "String") {
        return "bytea";
    }
    if (ydb == "Uuid") {
        return "uuid";
    }
    if (ydb == "Json") {
        return "json";
    }
    if (ydb == "JsonDocument") {
        return "jsonb";
    }
    if (ydb == "Timestamp64" || ydb == "Datetime64") {
        return "timestamp";
    }
    if (ydb == "Interval64") {
        return "interval";
    }
    if (ydb == "Date") {
        return "date";
    }
    if (ydb.StartsWith("Decimal")) {
        return "numeric";
    }
    return ydb;
}

TString LiteralText(const Ydb::TypedValue& value) {
    switch (value.type().type_id()) {
        case Ydb::Type::INT64:
            return ToString(value.value().int64_value());
        case Ydb::Type::INT32:
        case Ydb::Type::INT16:
        case Ydb::Type::INT8:
            return ToString(value.value().int32_value());
        case Ydb::Type::UTF8:
        case Ydb::Type::JSON:
        case Ydb::Type::JSON_DOCUMENT:
            return value.value().text_value();
        case Ydb::Type::STRING:
            return value.value().bytes_value();
        case Ydb::Type::BOOL:
            return value.value().bool_value() ? "true" : "false";
        default:
            return {};
    }
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

class TColumnsActor : public NActors::TActorBootstrapped<TColumnsActor> {
public:
    TColumnsActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Catalog(TString(ExtractDomain(Database)))
    {
    }

    void Bootstrap() {
        if (Statement.Star) {
            Columns = {"table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "data_type", "is_nullable", "column_default"};
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
            if (!NameFilter && comparison.Column.Name == "table_name" && comparison.Op == ECompareOp::Eq) {
                NameFilter = comparison.Value.Text;
            }
        }
        Become(&TColumnsActor::StateNavigate);
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
        TString Position;
        TString DataType;
        TString Nullable;
        bool DefaultNull = true;
        TString Default;
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
        if (column == "table_schema") {
            return row.Schema;
        }
        if (column == "table_name") {
            return row.Table;
        }
        if (column == "column_name") {
            return row.Name;
        }
        if (column == "ordinal_position") {
            return row.Position;
        }
        if (column == "data_type") {
            return row.DataType;
        }
        if (column == "is_nullable") {
            return row.Nullable;
        }
        if (column == "column_default") {
            return row.Default;
        }
        return Catalog;
    }

    bool Matches(const TFound& row) const {
        for (const TComparison& comparison : Statement.Where) {
            if (comparison.Column.Name == "column_default" && row.DefaultNull) {
                return false;
            }
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
                if (name == "column_default" && row.DefaultNull) {
                    line.push_back({true, {}});
                } else {
                    line.push_back({false, Cell(row, name)});
                }
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

    bool WantTable(const TString& schema, const TString& name) const {
        if (SchemaFilter && *SchemaFilter != schema) {
            return false;
        }
        if (NameFilter && *NameFilter != name) {
            return false;
        }
        return true;
    }

    void TakeTable(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        TVector<const TSysTables::TTableColumnInfo*> ordered;
        for (const auto& [id, column] : entry.Columns) {
            Y_UNUSED(id);
            ordered.push_back(&column);
        }
        std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
            return left->Id < right->Id;
        });
        ui32 position = 1;
        for (const auto* column : ordered) {
            TFound row;
            row.Schema = DescribedSchema;
            row.Table = DescribedName;
            row.Name = column->Name;
            row.Position = ToString(position++);
            row.DataType = PgTypeName(NScheme::TypeName(column->PType, column->PTypeMod));
            row.Nullable = column->IsNotNullColumn ? "NO" : "YES";
            if (column->IsDefaultFromSequence()) {
                row.DefaultNull = false;
            } else if (column->IsDefaultFromLiteral()) {
                row.DefaultNull = false;
                row.Default = LiteralText(column->DefaultFromLiteral);
            }
            Found.push_back(std::move(row));
        }
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const bool ok = request && request->ResultSet.size() == 1
            && request->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        if (!Listing) {
            if (ok && (request->ResultSet[0].Kind == NSchemeCache::TSchemeCacheNavigate::KindTable
                || request->ResultSet[0].Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable))
            {
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
                if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindTable
                    || child.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable)
                {
                    if (WantTable(CurrentSchema, child.Name)) {
                        Targets.push_back({CurrentSchema, child.Name});
                    }
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

NActors::IActor* CreateColumnsActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement) {
    return new TColumnsActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
