#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>

#include <util/string/builder.h>
#include <expected>
#include <numeric>
#include <ranges>

namespace NKikimr::NSchemeShard::NOlap {
    inline bool CheckLimits(const TSchemeLimits& limits, TOlapStoreInfo::TPtr alterData, TString& errStr) {
        for (auto& [_, preset]: alterData->SchemaPresets) {
            ui64 columnCount = preset.GetColumns().GetColumns().size();
            if (columnCount > limits.MaxColumnTableColumns) {
                errStr = TStringBuilder()
                    << "Too many columns"
                    << ". new: " << columnCount
                    << ". Limit: " << limits.MaxColumnTableColumns;
                return false;
            }
        }
        return true;
    }

    inline std::expected<void, TString>
    CheckColumnType(const ::NKikimrSchemeOp::TOlapColumnDescription &column, const TAppData *appData) {

        if ((column.GetType() == "Datetime64" || column.GetType() == "Timestamp64" || column.GetType() == "Interval64") &&
            !appData->FeatureFlags.GetEnableTableDatetime64()) {
            return std::unexpected(std::format(
                "Type '{}' specified for column '{}', but support for new date/time 64 types is disabled (EnableTableDatetime64 feature flag is off)",
                column.GetType().data(), column.GetName().data()));
        }

        if (column.GetType().StartsWith("Decimal") &&
            !appData->FeatureFlags.GetEnableParameterizedDecimal()) {
            return std::unexpected(std::format(
                "Type '{}' specified for column '{}', but support for parametrized decimal is disabled (EnableParameterizedDecimal feature flag is off)",
                column.GetType().data(), column.GetName().data()));
        }
        return {};
    }

    template <typename T>
    inline std::expected<void, TString> CheckColumns(const T &columns, const TAppData *appData) {
        std::expected<void, TString> init{};

        return std::accumulate(columns.begin(), columns.end(), init,
            [&](const auto &accumulator, const auto &column) {
                return accumulator.and_then([&] {
                    return CheckColumnType(column, appData);
                });
            });
    }
}
