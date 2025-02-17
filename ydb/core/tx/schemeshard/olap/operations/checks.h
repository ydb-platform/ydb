#pragma once

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
}


