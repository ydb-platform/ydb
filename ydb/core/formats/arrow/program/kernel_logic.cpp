#include "kernel_logic.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/partial.h>

#include <yql/essentials/core/arrow_kernels/request/request.h>

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

TString TSimpleKernelLogic::SignalDescription() const {
    if (GetYqlOperationId()) {
        return ::ToString((NYql::TKernelRequestBuilder::EBinaryOp)*GetYqlOperationId());
    } else {
        return "UNKNOWN";
    }
}

bool TSimpleKernelLogic::IsBoolInResult() const {
    if (GetYqlOperationId()) {
        switch ((NYql::TKernelRequestBuilder::EBinaryOp)*GetYqlOperationId()) {
            case NYql::TKernelRequestBuilder::EBinaryOp::And:
            case NYql::TKernelRequestBuilder::EBinaryOp::Or:
            case NYql::TKernelRequestBuilder::EBinaryOp::Xor:
                return true;
            case NYql::TKernelRequestBuilder::EBinaryOp::Add:
            case NYql::TKernelRequestBuilder::EBinaryOp::Sub:
            case NYql::TKernelRequestBuilder::EBinaryOp::Mul:
            case NYql::TKernelRequestBuilder::EBinaryOp::Div:
            case NYql::TKernelRequestBuilder::EBinaryOp::Mod:
            case NYql::TKernelRequestBuilder::EBinaryOp::Coalesce:
                return false;

            case NYql::TKernelRequestBuilder::EBinaryOp::StartsWith:
            case NYql::TKernelRequestBuilder::EBinaryOp::EndsWith:
            case NYql::TKernelRequestBuilder::EBinaryOp::StringContains:

            case NYql::TKernelRequestBuilder::EBinaryOp::Equals:
            case NYql::TKernelRequestBuilder::EBinaryOp::NotEquals:
            case NYql::TKernelRequestBuilder::EBinaryOp::Less:
            case NYql::TKernelRequestBuilder::EBinaryOp::LessOrEqual:
            case NYql::TKernelRequestBuilder::EBinaryOp::Greater:
            case NYql::TKernelRequestBuilder::EBinaryOp::GreaterOrEqual:
                return true;
        }
    } else {
        return false;
    }
}

NJson::TJsonValue TSimpleKernelLogic::DoDebugJson() const {
    if (GetYqlOperationId()) {
        return ::ToString((NYql::TKernelRequestBuilder::EBinaryOp)*GetYqlOperationId());
    } else {
        return NJson::JSON_NULL;
    }
}

NJson::TJsonValue IKernelLogic::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("class_name", GetClassName());
    if (YqlOperationId) {
        result.InsertValue("operation_id", ::ToString((NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId));
    }
    auto details = DoDebugJson();
    if (details.IsDefined()) {
        result.InsertValue("details", std::move(details));
    }
    return result;
}

}   // namespace NKikimr::NArrow::NSSA
