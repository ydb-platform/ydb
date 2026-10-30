#include "sequences.h"

#include <ydb/services/sql/datashard/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/cast.h>

namespace NKikimr::NSql {

namespace {

bool KnownColumn(const TString& name) {
    return name == "sequence_catalog" || name == "sequence_schema" || name == "sequence_name"
        || name == "data_type" || name == "numeric_precision" || name == "start_value"
        || name == "minimum_value" || name == "maximum_value" || name == "increment" || name == "cycle_option";
}

TString PgType(const TString& dataType) {
    if (dataType == "Int16" || dataType == "pgint2") {
        return "int2";
    }
    if (dataType == "Int32" || dataType == "pgint4") {
        return "int4";
    }
    return "int8";
}

TString Precision(const TString& dataType) {
    if (dataType == "Int16" || dataType == "pgint2") {
        return "16";
    }
    if (dataType == "Int32" || dataType == "pgint4") {
        return "32";
    }
    return "64";
}

class TSequencesActor : public NActors::TActorBootstrapped<TSequencesActor> {
public:
    TSequencesActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
        , Catalog(TString(ExtractDomain(Database)))
    {
    }

    void Bootstrap() {
        if (Statement.Star) {
            Columns = {"sequence_catalog", "sequence_schema", "sequence_name", "data_type", "numeric_precision",
                "start_value", "minimum_value", "maximum_value", "increment", "cycle_option"};
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
            if (!SchemaFilter && comparison.Column.Name == "sequence_schema" && comparison.Op == ECompareOp::Eq) {
                SchemaFilter = comparison.Value.Text;
            }
            if (!NameFilter && comparison.Column.Name == "sequence_name" && comparison.Op == ECompareOp::Eq) {
                NameFilter = comparison.Value.Text;
            }
        }
        Become(&TSequencesActor::StateNavigate);
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
        TString Name;
        TString DataType;
        TString Precision;
        TString Start;
        TString Minimum;
        TString Maximum;
        TString Increment;
        TString Cycle;
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
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    TString Cell(const TFound& row, const TString& column) const {
        if (column == "sequence_schema") {
            return row.Schema;
        }
        if (column == "sequence_name") {
            return row.Name;
        }
        if (column == "data_type") {
            return row.DataType;
        }
        if (column == "numeric_precision") {
            return row.Precision;
        }
        if (column == "start_value") {
            return row.Start;
        }
        if (column == "minimum_value") {
            return row.Minimum;
        }
        if (column == "maximum_value") {
            return row.Maximum;
        }
        if (column == "increment") {
            return row.Increment;
        }
        if (column == "cycle_option") {
            return row.Cycle;
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

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const bool ok = request && request->ResultSet.size() == 1
            && request->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        if (!Listing) {
            if (ok && request->ResultSet[0].Kind == NSchemeCache::TSchemeCacheNavigate::KindSequence && request->ResultSet[0].SequenceInfo) {
                const auto& description = request->ResultSet[0].SequenceInfo->Description;
                const TString dataType = description.GetDataType();
                Found.push_back({
                    DescribedSchema,
                    DescribedName,
                    PgType(dataType),
                    Precision(dataType),
                    ToString(description.GetStartValue()),
                    ToString(description.GetMinValue()),
                    ToString(description.GetMaxValue()),
                    ToString(description.GetIncrement()),
                    description.GetCycle() ? "YES" : "NO",
                });
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
                if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindSequence
                    && (!NameFilter || *NameFilter == child.Name)
                    && (!SchemaFilter || *SchemaFilter == CurrentSchema))
                {
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

NActors::IActor* CreateSequencesActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement) {
    return new TSequencesActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
