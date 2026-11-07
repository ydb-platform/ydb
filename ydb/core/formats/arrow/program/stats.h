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

    inline TString BuildStatsConstant(const arrow::Scalar& scalar, ui32 nodeId) {
        if (!scalar.is_valid) {
            return "NULL";
        }
        auto value = scalar.ToString();
        const auto type = scalar.type->id();
        const bool binary = type == arrow::Type::BINARY || type == arrow::Type::LARGE_BINARY ||
            type == arrow::Type::FIXED_SIZE_BINARY;
        if (binary) {
            value = static_cast<const arrow::BaseBinaryScalar&>(scalar).value->ToString();
        }
        TString description;
        bool hex = false;
        if (binary) {
            for (const unsigned char c : value) {
                if (c < 32 || c >= 127) { hex = true; break; }
            }
        }
        if (hex) {
            static constexpr char digits[] = "0123456789ABCDEF";
            description = "X'";
            for (const unsigned char c : value) {
                description += digits[c >> 4];
                description += digits[c & 15];
                if (description.size() > MaxStatsDescriptionBytes) { break; }
            }
            description += '\'';
        } else if (binary || type == arrow::Type::STRING || type == arrow::Type::LARGE_STRING) {
            description = "'";
            for (char c : value) {
                description += c;
                if (c == '\'') { description += '\''; }
                if (description.size() > MaxStatsDescriptionBytes) { break; }
            }
            description += '\'';
        } else {
            description = TString(value.data(), value.size());
        }
        return BoundStatsDescription(description, nodeId);
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
        else if (function == "TCompareKernel{Equals}" || function == "Equals") { infix = " = "; }
        else if (function == "TCompareKernel{NotEquals}" || function == "NotEquals") { infix = " <> "; }
        else if (function == "TCompareKernel{Greater}" || function == "Greater") { infix = " > "; }
        else if (function == "TCompareKernel{GreaterOrEqual}" || function == "GreaterOrEqual") { infix = " >= "; }
        else if (function == "TCompareKernel{Less}" || function == "Less") { infix = " < "; }
        else if (function == "TCompareKernel{LessOrEqual}" || function == "LessOrEqual") { infix = " <= "; }
        else if (function == "Add") { infix = " + "; }
        else if (function == "Sub") { infix = " - "; }
        else if (function == "Mul") { infix = " * "; }
        else if (function == "Div") { infix = " / "; }
        else if (function == "Mod") { infix = " % "; }
        if (arguments.size() < 2 || (arguments.size() != 2 && function != "And" && function != "Or")) { infix = {}; }
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
