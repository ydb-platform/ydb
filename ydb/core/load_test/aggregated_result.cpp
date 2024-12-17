#include "aggregated_result.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/cast.h>

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {

IOutputStream& operator<<(IOutputStream& output, const TAggregatedStats& stats) {
    output << "{total=" << stats.TotalNodes << ", success=" << stats.SuccessNodes << ", ";
    output << "transactions: [" << stats.Transactions << "], ";
    output << "transactions_per_sec: [" << stats.TransactionsPerSecond << "], ";
    output << "errors_per_sec: [" << stats.ErrorsPerSecond << "], ";
    for (ui32 level : xrange(stats.Percentiles.size())) {
        output << "percentile" << ToString(static_cast<EPercentileLevel>(level)) << ": [" << stats.Percentiles[level] << "], ";
    }
    output << "}";
    return output;
}

void TStatsAggregator::Add(const TEvNodeFinishResponse::TNodeStats& stats) {
    ++SuccessNodes;
    Transactions.Add(stats.GetTransactions());
    TransactionsPerSecond.Add(stats.GetTransactionsPerSecond());
    ErrorsPerSecond.Add(stats.GetErrorsPerSecond());
    Y_ENSURE(stats.PercentilesSize() == Percentiles.size(),
        stats.PercentilesSize() << " != " << Percentiles.size());
    for (ui32 i = 0; i < Percentiles.size(); ++i) {
        Percentiles[i].Add(stats.GetPercentiles(i));
    }
}

TAggregatedStats TStatsAggregator::Get() const {
    Y_ENSURE(SuccessNodes <= TotalNodes, SuccessNodes << " > " << TotalNodes);
    auto result = TAggregatedStats {
        .TotalNodes = TotalNodes,
        .SuccessNodes = SuccessNodes,
        .Transactions = Transactions.Get(),
        .TransactionsPerSecond = TransactionsPerSecond.Get(),
        .ErrorsPerSecond = ErrorsPerSecond.Get()
    };
    for (ui32 i = 0; i < Percentiles.size(); ++i) {
        result.Percentiles[i] = Percentiles[i].Get();
    }
    return result;
}

TString PrintShortDate(const TInstant& instant) {
    // Output format: Apr 13
    return instant.FormatGmTime("%b %d");
}

TString PrintShortTime(const TInstant& instant) {
    // Output format: 16:45:03
    return instant.FormatGmTime("%H:%M:%S");
}

void PrintStartFinishToHtml(const TInstant& start, const TInstant& finish, IOutputStream& output) {
    output << "<span title=\"" << start.ToStringUpToSeconds() << " / " << finish.ToStringUpToSeconds() <<
        "\">" << PrintShortDate(start) << " " <<
        PrintShortTime(start) << " / " << PrintShortTime(finish) << "</span>";
}

void PrintUuidToHtml(const TString& uuid, IOutputStream& output) {
    auto dashPos = uuid.find('-');
    output << "<span title=\"" << uuid << "\">" << uuid.substr(0, dashPos) << "..</span>";
}

IOutputStream& operator<<(IOutputStream& output, const TAggregatedResult& result) {
    output << "{'" << result.Uuid << "',\n";
    output << result.Start.ToString() << " - " << result.Finish.ToString() << ",\n";
    output << result.Stats << ",\n";
    output << "'" << result.Config << "'}";
    return output;
}

template<typename T>
T ExtractValue(NYdb::TResultSetParser& parser, const TString& column) {
    Y_UNUSED(parser, column);
    Y_ABORT("unimplemented");
}

template<>
ui32 ExtractValue(NYdb::TResultSetParser& parser, const TString& column) {
    return parser.ColumnParser(column).GetOptionalUint32().GetOrElse(0);
}

template<>
ui64 ExtractValue(NYdb::TResultSetParser& parser, const TString& column) {
    return parser.ColumnParser(column).GetOptionalUint64().GetOrElse(0);
}

template<>
double ExtractValue(NYdb::TResultSetParser& parser, const TString& column) {
    return parser.ColumnParser(column).GetOptionalDouble().GetOrElse(static_cast<double>(0));
}

template<>
TString ExtractValue(NYdb::TResultSetParser& parser, const TString& column) {
    return parser.ColumnParser(column).GetOptionalString().GetOrElse("");
}

template<>
TInstant ExtractValue(NYdb::TResultSetParser& parser, const TString& column) {
    return TInstant::Seconds(parser.ColumnParser(column).GetOptionalUint32().GetOrElse(0));
}

bool GetStatName(TStringBuf columnName, TStringBuf& statName, TStringBuf& suffix) {
    for (const auto& s : {"_min", "_avg", "_max"}) {
        if (columnName.EndsWith(s)) {
            columnName.BeforeSuffix(s, statName);
            suffix = columnName.SubStr(statName.size());
            return true;
        }
    }
    return false;
}

bool GetPercentileLevel(TStringBuf statName, TStringBuf& level) {
    if (statName.StartsWith("p") && statName.EndsWith("_ms")) {
        statName.AfterPrefix("p", level);
        level.BeforeSuffix("_ms", level);
        return true;
    }
    return false;
}

template<typename T, typename U>
void SetInAggregatedField(TStringBuf suffix, T value, TAggregatedField<U>& dst) {
    if (suffix == "_min") {
        dst.MinValue = value;
    } else if (suffix == "_avg") {
        dst.AvgValue = value;
    } else if (suffix == "_max") {
        dst.MaxValue = value;
    } else {
        ythrow yexception() << "invalid suffix for aggregated field: " << suffix;
    }
}

TAggregatedResult GetResultFromValueListItem(NYdb::TResultSetParser& parser, const NYdb::TResultSet& rs) {
    TAggregatedResult result;
    TStringBuf statName;
    TStringBuf suffix;
    TStringBuf levelSb;
    for (const auto& columnMeta : rs.GetColumnsMeta()) {
        TString column = columnMeta.Name;

        if (column == "id") {
            result.Uuid = ExtractValue<TString>(parser, column);
        } else if (column == "start") {
            result.Start = ExtractValue<TInstant>(parser, column);
        } else if (column == "finish") {
            result.Finish = ExtractValue<TInstant>(parser, column);
        } else if (column == "total_nodes") {
            result.Stats.TotalNodes = ExtractValue<ui32>(parser, column);
        } else if (column == "success_nodes") {
            result.Stats.SuccessNodes = ExtractValue<ui32>(parser, column);
        } else if (column == "config") {
            result.Config = ExtractValue<TString>(parser, column);
        } else if (GetStatName(column, statName, suffix)) {
            if (statName == "transactions") {
                if (suffix == "_avg") {
                    SetInAggregatedField(suffix, ExtractValue<double>(parser, column), result.Stats.Transactions);
                } else {
                    SetInAggregatedField(suffix, ExtractValue<ui64>(parser, column), result.Stats.Transactions);
                }
            } else if (statName == "transactions_per_sec") {
                SetInAggregatedField(suffix, ExtractValue<double>(parser, column), result.Stats.TransactionsPerSecond);
            } else if (statName == "errors_per_sec") {
                SetInAggregatedField(suffix, ExtractValue<double>(parser, column), result.Stats.ErrorsPerSecond);
            } else if (GetPercentileLevel(statName, levelSb)) {
                auto level = FromString<EPercentileLevel>(levelSb);
                SetInAggregatedField(suffix, ExtractValue<double>(parser, column), result.Stats.Percentiles[level]);
            }
        }
    }
    return result;
}

bool LoadResultFromResponseProto(const NKikimrKqp::TQueryResponse& response, TVector<TAggregatedResult>& results) {
    Y_ABORT_UNLESS(response.GetYdbResults().size() > 0);

    NYdb::TResultSet rs(response.GetYdbResults(0));
    NYdb::TResultSetParser parser(response.GetYdbResults(0));

    results.clear();
    while(parser.TryNextRow()) {
        results.push_back(GetResultFromValueListItem(parser, rs));
    }

    return true;
}

}  // namespace NKikimr
