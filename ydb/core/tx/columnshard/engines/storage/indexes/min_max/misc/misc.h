#pragma once
#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {
    inline const TString kMinMaxClassName = "MIN_MAX";

    TString IncorrectDataColumnsErrorMessage(const auto& dataColumns) {
        AFL_VERIFY(!dataColumns.empty());
        return TStringBuilder() << "Local min_max index does not require data columns (COVER in YQL), but got "
            << dataColumns.size() << " columns: [" << JoinStrings(dataColumns.begin(), dataColumns.end(), ", ") << "]";

    }

    TString IncorrectIndexColumnsErrorMessage(const auto& indexColumns) {
        AFL_VERIFY(indexColumns.size() != 1);

        return TStringBuilder() << "Local min_max index can only be applied to exactly one column (one column in the ON (...) YQL statement), but tried to apply it to "
            << indexColumns.size() << " columns: [" << JoinStrings(indexColumns.begin(), indexColumns.end(), ", ") << "]";

    }

    inline const TString DisabledForRowTablesErrorMessage = "Local min_max index is supported only for column tables";

    inline TString UnknownIndexColumnNameErrorMessage(TStringBuf columnName) {
        return TStringBuilder() << "Tried to apply min_max index to unknown column '" << columnName << "'";
    }

    inline const TString FeatureFlagDisabledErrorMessage = "Local min_max index is disabled with EnableCsMinMaxIndex feature flag";


}
