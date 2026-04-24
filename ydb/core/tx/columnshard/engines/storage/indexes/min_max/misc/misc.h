#pragma once
#include <optional>
#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {
    inline const TString kMinMaxClassName = "MIN_MAX";

    TString IncorrectDataColumnsErrorMessage(const auto& dataColumns) {
        AFL_VERIFY(!dataColumns.empty());
        return TStringBuilder() << "Local min_max does't need Data columns(COVER from yql), but got "
            << dataColumns.size() << " of these columns: [" << JoinStrings(dataColumns.begin(), dataColumns.end(), ", ") << "]";

    }

    TString IncorrectIndexColumnsErrorMessage(const auto& indexColumns) {
        AFL_VERIFY(indexColumns.size() != 1);

        return TStringBuilder() << "Local min_max is applied to 1 column only(exactly 1 column in ON (...) yql statement), tried to apply to "
            << indexColumns.size() << " columns: [" << JoinStrings(indexColumns.begin(), indexColumns.end(), ", ") << "]";

    }

    inline TString DisabledForRowTablesErrorMessage = "Local min_max index is supported only for column tables";

    inline TString UnknownIndexColumnNameErrorMessage(TStringBuf columnName) {
        return TStringBuilder() << "Tried to apply min_max index to unknown column '" << columnName << "'";
    }

    inline TString FeatureFlagDisabledErrorMessage = "Local min_max index is disabled with EnableCsMinMaxIndex feature flag";


}
