#include "kernel_logic.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<bool> TGetJsonPath::DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
    const std::shared_ptr<TAccessorsCollection>& resources) const {
    auto description = BuildDescription(input, resources);
    if (description.IsFail()) {
        return description;
    }
    if (output.size() != 1) {
        return TConclusionStatus::Fail("incorrect output count (1 expected) for json path extraction");
    }
    auto accJson = description->GetInputAccessor();
    if (!accJson) {
        return TConclusionStatus::Fail("incorrect accessor for first json path argument (" + ::ToString(input.front().GetColumnId()) + ")");
    }
    if (accJson->GetType() == IChunkedArray::EType::CompositeChunkedArray) {
        auto composite = std::static_pointer_cast<NAccessor::TCompositeChunkedArray>(accJson);
        NAccessor::TCompositeChunkedArray::TBuilder builder(arrow::utf8());
        for (auto&& i : composite->GetChunks()) {
            if (i->GetType() != IChunkedArray::EType::SubColumnsArray && i->GetType() != IChunkedArray::EType::SubColumnsPartialArray) {
                return false;
            }
            builder.AddChunk(ExtractArray(i, svPath));
        }
        resources->AddVerified(output.front().GetColumnId(), builder.Finish());
        return true;
    }
    if (accJson->GetType() != IChunkedArray::EType::SubColumnsArray && accJson->GetType() != IChunkedArray::EType::SubColumnsPartialArray) {
        return false;
    }
    resources->AddVerified(output.front().GetColumnId(), ExtractArray(accJson, svPath));
    return true;
}

std::shared_ptr<IChunkedArray> TGetJsonPath::ExtractArray(
    const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const {
    if (jsonAcc->GetType() == IChunkedArray::EType::SubColumnsArray) {
        auto accJsonArray = std::static_pointer_cast<NAccessor::TSubColumnsArray>(jsonAcc);
        return accJsonArray->GetPathAccessor(svPath, jsonAcc->GetRecordsCount());
    } else {
        AFL_VERIFY(jsonAcc->GetType() == IChunkedArray::EType::SubColumnsPartialArray);
        auto accJsonArray = std::static_pointer_cast<NAccessor::TSubColumnsPartialArray>(jsonAcc);
        return accJsonArray->GetPathAccessor(svPath, jsonAcc->GetRecordsCount());
    }
}

std::optional<TFetchingInfo> TGetJsonPath::BuildFetchTask(
    const ui32 columnId, const std::shared_ptr<TAccessorsCollection>& resources) const {
    resources->
}

}   // namespace NKikimr::NArrow::NSSA
