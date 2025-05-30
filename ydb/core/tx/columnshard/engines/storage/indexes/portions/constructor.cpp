#include "constructor.h"

namespace NKikimr::NOlap::NIndexes {

TConclusionStatus TColumnIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("column_name")) {
        return TConclusionStatus::Fail("column_name have to be in bloom filter features");
    }
    if (!jsonInfo["column_name"].GetString(&ColumnName)) {
        return TConclusionStatus::Fail("column_name have to be string");
    }
    if (!ColumnName) {
        return TConclusionStatus::Fail("column_name cannot contains empty strings");
    }
    {
        auto conclusion = DataExtractor.DeserializeFromJson(jsonInfo["data_extractor"]);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NIndexes
