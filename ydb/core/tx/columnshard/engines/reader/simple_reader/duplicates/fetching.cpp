#include "fetching.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

void TColumnFetchingContext::StartAllocation(const std::shared_ptr<TColumnFetchingContext>& context) {
    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>("DUPLICATES");
    request->AddPortion(context->GetPortion());
    request->SetColumnIds({ context->GetCommonContext().GetResultSchema()->GetColumnIds().begin(),
        context->GetCommonContext().GetResultSchema()->GetColumnIds().end() });
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(context));

    context->GetCommonContext().GetDataAccessorsManager()->AskData(request);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
