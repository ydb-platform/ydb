#pragma once

#include "abstract.h"

#include <util/charset/utf8.h>
#include <util/string/cast.h>

namespace NKikimr::NArrow::NSSA {

    // Shared expression DAGs must not expand into exponentially large labels.
    inline constexpr size_t MaxStatsDescriptionBytes = 512;

    inline TString BoundStatsDescription(TStringBuf description, ui32 nodeId) {
        if (description.size() <= MaxStatsDescriptionBytes) {
            return TString(description);
        }
        const TString suffix = "... [expr #" + ToString(nodeId) + "]";
        return TString(Utf8TruncateRobust(description, MaxStatsDescriptionBytes - suffix.size())) + suffix;
    }

    inline TString BuildStatsExpression(TStringBuf function, const std::vector<TString>& arguments, ui32 nodeId) {
        TString description;
        const auto append = [&](TStringBuf part) {
            if (description.size() <= MaxStatsDescriptionBytes) {
                description.append(part.substr(0, MaxStatsDescriptionBytes + 1 - description.size()));
            }
        };
        TStringBuf infix;
        if (function == "And") { infix = " AND "; }
        else if (function == "Or") { infix = " OR "; }
        else if (function == "TCompareKernel{Equals}") { infix = " = "; }
        else if (function == "TCompareKernel{NotEquals}") { infix = " != "; }
        else if (function == "TCompareKernel{Greater}") { infix = " > "; }
        else if (function == "TCompareKernel{GreaterOrEqual}") { infix = " >= "; }
        else if (function == "TCompareKernel{Less}") { infix = " < "; }
        else if (function == "TCompareKernel{LessOrEqual}") { infix = " <= "; }
        else if (function == "Add") { infix = " + "; }
        else if (function == "Mul") { infix = " * "; }
        if (arguments.size() < 2) { infix = {}; }
        if (!infix) { append(function); }
        append("(");
        bool first = true;
        for (const auto& argument : arguments) {
            if (!first) {
                append(infix ? infix : TStringBuf(", "));
            }
            append(argument);
            first = false;
        }
        append(")");
        return BoundStatsDescription(description, nodeId);
    }

    // One aligned argument domain; scalars broadcast over arrays and do not add rows.
    inline std::optional<ui64> GetStatsRowsPopulation(const std::vector<arrow::Datum>& arguments) {
        std::optional<ui64> rows;
        for (const auto& argument : arguments) {
            if (argument.is_array() || argument.kind() == arrow::Datum::CHUNKED_ARRAY) {
                const ui64 length = argument.length();
                if (rows && *rows != length) {
                    return std::nullopt;
                }
                rows = length;
            } else if (!argument.is_scalar()) {
                return std::nullopt;
            }
        }
        return rows.value_or(1);
    }

    inline void AddStatsArgumentRows(const std::vector<arrow::Datum>& arguments) {
        const auto& operation = NScanStats::Current();
        if (!operation) {
            return;
        }
        const auto rows = GetStatsRowsPopulation(arguments);
        if (!rows) {
            return;
        }
        operation->Update([&](NScanStats::TStep& record) {
            if (record.GetStepKind() == NScanStats::TStep::Aggregation) {
                auto* details = record.MutableDetails()->MutableAggregation();
                details->SetInputRows(details->GetInputRows() + *rows);
            } else if (record.GetStepKind() == NScanStats::TStep::Calculation) {
                auto* details = record.MutableDetails()->MutableCalculation();
                details->SetRowsCount(details->GetRowsCount() + *rows);
            }
        });
    }

} // namespace NKikimr::NArrow::NSSA
