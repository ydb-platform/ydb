#include "kernel_logic.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
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
    NAccessor::TCompositeChunkedArray::TBuilder builder = MakeCompositeBuilder();
    const std::optional<bool> applied =
        NAccessor::TCompositeChunkedArray::VisitDataOwners<bool>(accJson, [&](const std::shared_ptr<NAccessor::IChunkedArray>& arr) {
            if (arr->GetType() != IChunkedArray::EType::SubColumnsArray && arr->GetType() != IChunkedArray::EType::SubColumnsPartialArray) {
                return std::optional<bool>(false);
            }
            builder.AddChunk(ExtractArray(arr, description->GetJsonPath()));
            return std::optional<bool>();
        });
    if (applied && !*applied) {
        return false;
    }
    resources->AddVerified(output.front().GetColumnId(), builder.Finish(), false);
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

std::optional<TFetchingInfo> TGetJsonPath::BuildFetchTask(const ui32 columnId, const NAccessor::IChunkedArray::EType arrType,
    const std::vector<TColumnChainInfo>& input, const std::shared_ptr<TAccessorsCollection>& resources) const {
    if (arrType != NAccessor::IChunkedArray::EType::SubColumnsArray) {
        return TFetchingInfo::BuildFullRestore(false);
    }
    AFL_VERIFY(input.size() == 2 && input.front().GetColumnId() == columnId);
    auto description = BuildDescription(input, resources).DetachResult();
    const std::vector<TString> subColumns = { TString(description.GetJsonPath().data(), description.GetJsonPath().size()) };
    if (!description.GetInputAccessor()) {
        return TFetchingInfo::BuildSubColumnsRestore(subColumns);
    }

    std::optional<bool> hasSubColumns;
    return NAccessor::TCompositeChunkedArray::VisitDataOwners<TFetchingInfo>(
        description.GetInputAccessor(), [&](const std::shared_ptr<NAccessor::IChunkedArray>& arr) {
            if (arr->GetType() == NAccessor::IChunkedArray::EType::SubColumnsPartialArray) {
                AFL_VERIFY(!hasSubColumns || *hasSubColumns);
                hasSubColumns = true;
                auto scArr = std::static_pointer_cast<NAccessor::TSubColumnsPartialArray>(arr);
                if (scArr->NeedFetch(description.GetJsonPath())) {
                    return std::optional<TFetchingInfo>(TFetchingInfo::BuildSubColumnsRestore(subColumns));
                }
            } else {
                AFL_VERIFY(arr->GetType() == NAccessor::IChunkedArray::EType::SubColumnsArray);
                AFL_VERIFY(!hasSubColumns || !*hasSubColumns);
                hasSubColumns = false;
            }
            return std::optional<TFetchingInfo>();
        });
}

NAccessor::TCompositeChunkedArray::TBuilder TGetJsonPath::MakeCompositeBuilder() const {
    return NAccessor::TCompositeChunkedArray::TBuilder(arrow::utf8());
}

std::shared_ptr<IChunkedArray> TExistsJsonPath::ExtractArray(
    const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const {
    auto arr = TBase::ExtractArray(jsonAcc, svPath);
    auto chunkedArray = arr->GetChunkedArray();
    auto builder = NArrow::MakeBuilder(arrow::uint8(), arr->GetRecordsCount());
    for (auto&& i : chunkedArray->chunks()) {
        for (ui32 idx = 0; idx < i->length(); ++idx) {
            NArrow::Append<arrow::UInt8Type>(*builder, i->IsNull(idx) ? 0 : 1);
        }
    }
    return std::make_shared<NAccessor::TTrivialArray>(FinishBuilder(std::move(builder)));
}

NAccessor::TCompositeChunkedArray::TBuilder TExistsJsonPath::MakeCompositeBuilder() const {
    return NAccessor::TCompositeChunkedArray::TBuilder(arrow::uint8());
}

}   // namespace NKikimr::NArrow::NSSA
