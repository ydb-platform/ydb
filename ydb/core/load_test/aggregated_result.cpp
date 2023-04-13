#include "aggregated_result.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/cast.h>

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

void PrintUuidToHtml(const TString& uuid, IOutputStream& output) {
    auto dashPos = uuid.find('-');
    output << "<abbr title=\"" << uuid << "\">" << uuid.substr(0, dashPos) << "</abbr>";
}

IOutputStream& operator<<(IOutputStream& output, const TAggregatedResult& result) {
    output << "{'" << result.Uuid << "',\n";
    output << result.Start.ToString() << " - " << result.Finish.ToString() << ",\n";
    output << result.Stats << ",\n";
    output << "'" << result.Config << "'}";
    return output;
}

using TColumnPositions = THashMap<TString, ui32>;

TColumnPositions GetColumnPositionsInResponse(const NKikimrMiniKQL::TType& ttype) {
    TColumnPositions columnPositions;
    for (const NKikimrMiniKQL::TMember& member : ttype.GetStruct().GetMember()) {
        if (member.GetName() == "Data") {
            const auto& listStruct = member.GetType().GetList().GetItem().GetStruct();
            for (const NKikimrMiniKQL::TMember& listMember : listStruct.GetMember()) {
                columnPositions.emplace(listMember.GetName(), columnPositions.size());
            }
            break;
        }
    }
    return columnPositions;
}

NKikimrMiniKQL::TValue GetOptional(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    return listItem.GetStruct(pos).GetOptional();
}

template<typename T>
T ExtractValue(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    Y_UNUSED(listItem, pos);
    Y_FAIL("unimplemented");    
}

template<>
ui32 ExtractValue(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    return GetOptional(listItem, pos).GetUint32();
}

template<>
ui64 ExtractValue(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    return GetOptional(listItem, pos).GetUint64();
}

template<>
double ExtractValue(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    return GetOptional(listItem, pos).GetDouble();
}

template<>
TString ExtractValue(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    return GetOptional(listItem, pos).GetBytes();
}

template<>
TInstant ExtractValue(const NKikimrMiniKQL::TValue& listItem, ui32 pos) {
    return TInstant::Seconds(GetOptional(listItem, pos).GetUint32());
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

template<typename T>
void SetInAggregatedField(TStringBuf suffix, T value, TAggregatedField<T>& dst) {
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

TAggregatedResult GetResultFromValueListItem(const NKikimrMiniKQL::TValue& listItem, const TColumnPositions& columnPositions) {
    TAggregatedResult result;
    TStringBuf statName;
    TStringBuf suffix;
    TStringBuf levelSb;
    for (const auto& [column, pos] : columnPositions) {
        if (column == "id") {
            result.Uuid = ExtractValue<TString>(listItem, pos);
        } else if (column == "start") {
            result.Start = ExtractValue<TInstant>(listItem, pos);
        } else if (column == "finish") {
            result.Finish = ExtractValue<TInstant>(listItem, pos);
        } else if (column == "total_nodes") {
            result.Stats.TotalNodes = ExtractValue<ui32>(listItem, pos);
        } else if (column == "success_nodes") {
            result.Stats.SuccessNodes = ExtractValue<ui32>(listItem, pos);
        } else if (column == "config") {
            result.Config = ExtractValue<TString>(listItem, pos);
        } else if (GetStatName(column, statName, suffix)) {
            if (statName == "transactions") {
                SetInAggregatedField(suffix, ExtractValue<ui64>(listItem, pos), result.Stats.Transactions);
            } else if (statName == "transactions_per_sec") {
                SetInAggregatedField(suffix, ExtractValue<double>(listItem, pos), result.Stats.TransactionsPerSecond);
            } else if (statName == "errors_per_sec") {
                SetInAggregatedField(suffix, ExtractValue<double>(listItem, pos), result.Stats.ErrorsPerSecond);
            } else if (GetPercentileLevel(statName, levelSb)) {
                auto level = FromString<EPercentileLevel>(levelSb);
                SetInAggregatedField(suffix, ExtractValue<double>(listItem, pos), result.Stats.Percentiles[level]);
            }
        }
    }
    return result;
}

bool LoadResultFromResponseProto(const NKikimrKqp::TQueryResponse& response, TVector<TAggregatedResult>& results) {
    const auto& ttype = response.GetResults(0).GetType();
    auto columnPositions = GetColumnPositionsInResponse(ttype);
    if (columnPositions.empty()) {
        return false;
    }

    results.clear();
    for (const NKikimrMiniKQL::TValue& listItem : response.GetResults(0).GetValue().GetStruct().Get(0).GetList()) {
        results.push_back(GetResultFromValueListItem(listItem, columnPositions));
    }
    return true;
}

}  // namespace NKikimr
