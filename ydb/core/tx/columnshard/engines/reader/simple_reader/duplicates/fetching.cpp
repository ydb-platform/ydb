#include "fetching.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TColumnFetchingContext::StartAllocation(const std::shared_ptr<TColumnFetchingContext>& context) {
    std::shared_ptr<TPortionDataSource> source = std::dynamic_pointer_cast<TPortionDataSource>(context->GetSource());
    AFL_VERIFY(source)("source", source->DebugJson())("reason", "fetching_not_implemented");

    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>("DUPLICATES");
    request->AddPortion(source->GetPortionInfoPtr());
    request->SetColumnIds({ context->GetResultSchema()->GetColumnIds().begin(), context->GetResultSchema()->GetColumnIds().end() });
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(context));

    context->GetSource()->GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
