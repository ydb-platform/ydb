#include "archive.h"

#include "percentile.h"

#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <utility>

namespace NKikimr {

static constexpr TStringBuf kResultTablePath = ".load_test_archive";
static constexpr TStringBuf kMinSuffix = "_min";
static constexpr TStringBuf kAvgSuffix = "_avg";
static constexpr TStringBuf kMaxSuffix = "_max";

namespace {

using TColumnDescription = TVector<std::pair<TString, TString>>;

const TColumnDescription BuildTableColumns() {
    TColumnDescription columns;
    columns.emplace_back("id", "String");
    for (const auto& name : {"start", "finish"}) {
        columns.emplace_back(name, "Datetime");
    }
    columns.emplace_back("total_nodes", "Uint32");
    columns.emplace_back("success_nodes", "Uint32");

    auto addStatsColumn = [&columns](const TString& name, const TString& typeName) {
        for (const auto& suffix : {kMinSuffix, kAvgSuffix, kMaxSuffix}) {
            TString columnTypeName = (suffix == kAvgSuffix) ? "Double" : typeName;
            columns.emplace_back(name + suffix, std::move(columnTypeName));
        }
    };
    addStatsColumn("transactions", "Uint64");
    addStatsColumn("transactions_per_sec", "Double");
    addStatsColumn("errors_per_sec", "Double");
    for (ui32 level : xrange(EPL_COUNT_NUM)) {
        addStatsColumn("p" + ToString(static_cast<EPercentileLevel>(level)) + "_ms", "Double");
    }
    columns.emplace_back("config", "String");
    return columns;
}

const TColumnDescription& GetTableColumns() {
    static const TColumnDescription kColumns = BuildTableColumns();
    return kColumns;
}

const TVector<TString> BuildTableColumnNames() {
    TVector<TString> names;
    for (const auto& [name, _] : GetTableColumns()) {
        names.push_back(name);
    }
    return names;
}

const TVector<TString> GetTableColumnNames() {
    static const TVector<TString> kNames = BuildTableColumnNames();
    return kNames;
}

TString DatetimeFromInstant(TInstant instant) {
    return instant.FormatGmTime("Datetime(\"%Y-%m-%dT%H:%M:%SZ\")");
}

TString EscapeQuotes(const TString& rawValue) {
    TVector<TString> parts;
    Split(rawValue, "\"", parts);
    return JoinSeq("\\\"", parts);
}

}  // anonymous namespace

TString MakeTableCreationYql() {
    TVector<TString> schemaParts;
    for (const auto& [name, typeName] : GetTableColumns()) {
        schemaParts.push_back(name + " " + typeName);
    };
    schemaParts.push_back("PRIMARY KEY(id)");

    TStringStream ss;
    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << kResultTablePath << "` ";
    ss << "(" << JoinSeq(", ", schemaParts) << ")";
    return ss.Str();
}

TString MakeRecordInsertionYql(const TVector<TAggregatedResult>& items) {
    TStringStream ss;
    ss << "--!syntax_v1\n";
    ss << "INSERT INTO `" << kResultTablePath << "` ";
    ss << "(" << JoinSeq(", ", GetTableColumnNames()) << ") ";
    ss << "VALUES ";

    bool first = true;
    for (const auto& item : items) {
        if (first) {
            first = false;
        } else {
            ss << ", ";
        }
        ss << "(\"" << item.Uuid << "\", " <<
            DatetimeFromInstant(item.Start) << ", " <<
            DatetimeFromInstant(item.Finish) << ", " <<
            item.Stats.TotalNodes << ", " <<
            item.Stats.SuccessNodes << ", " <<
            item.Stats.Transactions << ", " <<
            item.Stats.TransactionsPerSecond << ", " <<
            item.Stats.ErrorsPerSecond << ", ";
        for (ui32 level : xrange(EPL_COUNT_NUM)) {
            ss << item.Stats.Percentiles[level] << ", ";
        }
        ss << "\"" << EscapeQuotes(item.Config) << "\")";
    }
    ss << ";";
    return ss.Str();
}

TString MakeRecordSelectionYql(ui32 offset, ui32 limit) {
    TStringStream ss;
    ss << "--!syntax_v1\n";
    ss << "SELECT * FROM `" << kResultTablePath << "` ";
    ss << "ORDER BY `start` DESC ";
    ss << "LIMIT " << limit << " OFFSET " << offset << ";";
    return ss.Str();
}

}  // namespace NKikimr
