#include "select_builder.h"

#include <util/string/builder.h>

#include <format>

namespace NKikimr::NStat {

ui32 TSelectBuilder::AddBuiltinAggregation(std::optional<TString> columnName, TString aggName) {
    auto column = TAggColumn{
        .Seq = static_cast<ui32>(Columns.size()),
        .ColumnName = std::move(columnName),
        .AggName = std::move(aggName),
    };
    Columns.push_back(std::move(column));
    return Columns.back().Seq;
}

ui32 TSelectBuilder::AddFactory(const TStringBuf& udafName, size_t paramCount) {
    // TODO: check UDAF existence, validate paramcount
    ui32 curId = Udaf2Factory.size();
    auto [it, emplaced] = Udaf2Factory.try_emplace(udafName, curId, udafName, paramCount);
    return it->second.Id;
}

TString TSelectBuilder::Build(const TStringBuf& table) const {
    TStringBuilder res;
    for (const auto& [udaf, factory] : Udaf2Factory) {
        TStringBuilder paramsStr;
        for (size_t i = 0; i < factory.ParamCount; ++i) {
            if (i > 0) {
                paramsStr << ",";
            }
            paramsStr << "$p" << i;
        }

        res << std::format(R"($f{0} = ({2}) -> {{ return AggregationFactory(
    "UDAF",
    ($item,$parent) -> {{ return Udf(StatisticsInternal::{1}Create, $parent as Depends)($item,{2}) }},
    ($state,$item,$parent) -> {{ return Udf(StatisticsInternal::{1}AddValue, $parent as Depends)($state, $item) }},
    StatisticsInternal::{1}Merge,
    StatisticsInternal::{1}Finalize,
    StatisticsInternal::{1}Serialize,
    StatisticsInternal::{1}Deserialize,
)
}};
)",
            factory.Id, std::string_view(factory.Udaf), std::string_view(paramsStr));
    }

    res << "SELECT ";
    bool first = true;
    for (const auto& agg : Columns ) {
        if (first) {
            first = false;
        } else {
            res << ",";
        }
        if (agg.UdafFactory) {
            Y_ABORT_UNLESS(agg.ColumnName);
            res << "AGGREGATE_BY(" << agg.ColumnName
                << "," << "$f" << *agg.UdafFactory << "(" << agg.Params << "))";
        } else {
            Y_ABORT_UNLESS(agg.AggName);
            res << *agg.AggName;
            if (agg.ColumnName) {
                res << "(" << *agg.ColumnName << ")";
            } else {
                res << "(*)";
            }
        }
    }

    res << " FROM `" << table << "`";
    return res;
}

} // NKikimr::NStat
