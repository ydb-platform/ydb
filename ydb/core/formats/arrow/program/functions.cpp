#include "functions.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NSSA {
TConclusion<arrow::Datum> TInternalFunction::Call(
    const TExecFunctionContext& context, const std::shared_ptr<TAccessorsCollection>& resources) const {
    auto funcNames = GetRegistryFunctionNames();

    auto argumentsReader = resources->GetArguments(TColumnChainInfo::ExtractColumnIds(context.GetColumns()), NeedConcatenation);
    TAccessorsCollection::TChunksMerger merger;
    while (auto arguments = argumentsReader.ReadNext()) {
        arrow::Result<arrow::Datum> result = arrow::Status::UnknownError<std::string>("unknown function");
        for (const auto& funcName : funcNames) {
            if (GetContext() && GetContext()->func_registry()->GetFunction(funcName).ok()) {
                result = arrow::compute::CallFunction(funcName, *arguments, FunctionOptions.get(), GetContext());
            } else {
                result = arrow::compute::CallFunction(funcName, *arguments, FunctionOptions.get());
            }

            if (result.ok() && funcName == "count"sv) {
                result = result->scalar()->CastTo(std::make_shared<arrow::UInt64Type>());
            }
            if (result.ok()) {
                auto prepareStatus = PrepareResult(std::move(*result));
                if (prepareStatus.IsFail()) {
                    return prepareStatus;
                }
                result = prepareStatus.DetachResult();
                break;
            }
        }
        if (result.ok()) {
            merger.AddChunk(*result);
        } else {
            return TConclusionStatus::Fail(result.status().message());
        }
    }
    return merger.Execute();
}

}   // namespace NKikimr::NArrow::NSSA
