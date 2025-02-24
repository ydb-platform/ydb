#include "kernel_logic.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/partial.h>

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
            builder.AddChunk(ExtractArray(i, description->GetJsonPath()));
        }
        resources->AddVerified(output.front().GetColumnId(), builder.Finish());
        return true;
    }
    if (accJson->GetType() != IChunkedArray::EType::SubColumnsArray && accJson->GetType() != IChunkedArray::EType::SubColumnsPartialArray) {
        return false;
    }
    resources->AddVerified(output.front().GetColumnId(), ExtractArray(accJson, description->GetJsonPath()));
    return true;
}

std::shared_ptr<IChunkedArray> TGetJsonPath::ExtractArray(const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const {
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
    const ui32 columnId, const std::vector<TColumnChainInfo>& input, const std::shared_ptr<TAccessorsCollection>& resources) const {
    AFL_VERIFY(input.size() == 2 && input.front().GetColumnId() == columnId);
    auto description = BuildDescription(input, resources).DetachResult();
    const std::vector<TString> subColumns = { TString(description.GetJsonPath().data(), description.GetJsonPath().size()) };
//    if (!description.GetInputAccessor()) {
//        return TFetchingInfo::BuildFullRestore();
//    } else {
//        return {};
//    }
    if (!description.GetInputAccessor()) {
        return TFetchingInfo::BuildSubColumnsRestore(subColumns);
    } else if (description.GetInputAccessor()->GetType() == NAccessor::IChunkedArray::EType::SubColumnsArray) {
        return std::nullopt;
    } else if (description.GetInputAccessor()->GetType() == NAccessor::IChunkedArray::EType::SubColumnsPartialArray) {
        auto arr = std::static_pointer_cast<NAccessor::TSubColumnsPartialArray>(description.GetInputAccessor());
        if (arr->NeedFetch(description.GetJsonPath())) {
            return TFetchingInfo::BuildSubColumnsRestore(subColumns);
        }
        return std::nullopt;
    } else if (description.GetInputAccessor()->GetType() == NAccessor::IChunkedArray::EType::CompositeChunkedArray) {
        auto arr = std::static_pointer_cast<NAccessor::TCompositeChunkedArray>(description.GetInputAccessor());
        std::optional<bool> needRestore;
        for (auto it = NAccessor::TCompositeChunkedArray::BuildIterator(arr); it.IsValid(); it.Next()) {
            bool needRestoreLocal = false;
            if (it.GetArray()->GetType() == NAccessor::IChunkedArray::EType::SubColumnsArray) {
                needRestoreLocal = false;
            } else if (it.GetArray()->GetType() == NAccessor::IChunkedArray::EType::SubColumnsPartialArray) {
                auto arr = std::static_pointer_cast<NAccessor::TSubColumnsPartialArray>(it.GetArray());
                needRestoreLocal = arr->NeedFetch(description.GetJsonPath());
            } else {
                needRestoreLocal = false;
            }
            if (!needRestore) {
                needRestore = needRestoreLocal;
            } else {
                AFL_VERIFY(needRestoreLocal == needRestore);
            }
        }
        AFL_VERIFY(!!needRestore);
        if (*needRestore) {
            return TFetchingInfo::BuildSubColumnsRestore(subColumns);
        }
    }
    return std::nullopt;
}

}   // namespace NKikimr::NArrow::NSSA
