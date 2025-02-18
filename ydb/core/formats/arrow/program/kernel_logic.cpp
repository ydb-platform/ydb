#include "kernel_logic.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

#include <ydb/library/formats/arrow/accessor/composite/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<bool> TGetJsonPath::DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
    const std::shared_ptr<TAccessorsCollection>& resources) const {
    if (input.size() != 2) {
        return TConclusionStatus::Fail("incorrect parameters count (2 expected) for json path extraction");
    }
    if (output.size() != 1) {
        return TConclusionStatus::Fail("incorrect output count (1 expected) for json path extraction");
    }
    auto jsonPathScalar = resources->GetConstantScalarOptional(input[1].GetColumnId());
    if (!jsonPathScalar) {
        return TConclusionStatus::Fail("no data for json path (cannot find parameter)");
    }
    if (jsonPathScalar->type->id() != arrow::utf8()->id()) {
        return TConclusionStatus::Fail("incorrect json path (have to be utf8)");
    }
    const auto buffer = std::static_pointer_cast<arrow::StringScalar>(jsonPathScalar)->value;
    std::string_view svPath((const char*)buffer->data(), buffer->size());
    if (!svPath.starts_with("$.") || svPath.size() == 2) {
        return TConclusionStatus::Fail("incorrect path format: have to be as '$.**...**'");
    }
    svPath = svPath.substr(2);

    auto accJson = resources->GetAccessorOptional(input.front().GetColumnId());
    if (!accJson) {
        return TConclusionStatus::Fail("incorrect accessor for first json path argument (" + ::ToString(input.front().GetColumnId()) + ")");
    }
    if (accJson->GetType() == IChunkedArray::EType::CompositeChunkedArray) {
        auto composite = std::static_pointer_cast<NAccessor::TCompositeChunkedArray>(accJson);
        NAccessor::TCompositeChunkedArray::TBuilder builder(arrow::utf8());
        for (auto&& i : composite->GetChunks()) {
            if (i->GetType() != IChunkedArray::EType::SubColumnsArray) {
                return false;
            }
            builder.AddChunk(ExtractArray(i, svPath));
        }
        resources->AddVerified(output.front().GetColumnId(), builder.Finish());
        return true;
    }
    if (accJson->GetType() != IChunkedArray::EType::SubColumnsArray) {
        return false;
    }
    resources->AddVerified(output.front().GetColumnId(), ExtractArray(accJson, svPath));
    return true;
}

std::shared_ptr<NKikimr::NArrow::NSSA::IChunkedArray> TGetJsonPath::ExtractArray(
    const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const {
    AFL_VERIFY(jsonAcc->GetType() == IChunkedArray::EType::SubColumnsArray);
    auto accJsonArray = std::static_pointer_cast<NAccessor::TSubColumnsArray>(jsonAcc);
    auto accResult = accJsonArray->GetColumnsData().GetPathAccessor(svPath);
    if (accResult) {
        return accResult;
    }
    return accJsonArray->GetOthersData().GetPathAccessor(svPath, jsonAcc->GetRecordsCount());
}

}   // namespace NKikimr::NArrow::NSSA
