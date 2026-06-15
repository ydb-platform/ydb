#include "filterable.h"

namespace NKikimr::NArrow {

ui32 TGeneralContainerFilterable::GetRecordsCount() const {
    return Container ? Container->GetRecordsCount() : 0;
}

void TGeneralContainerFilterable::ApplyEmpty() {
    Container = Container->BuildEmptySame();
}

void TGeneralContainerFilterable::ApplyArrowFilter(const TColumnFilter& filter) {
    Container = std::make_shared<TGeneralContainer>(ApplyArrowFilterToTable(Container->BuildTableVerified(), filter));
}

void TGeneralContainerFilterable::ApplySlicesFilter(TColumnFilter::TSlicesIterator slices) {
    Container = std::make_shared<TGeneralContainer>(ApplySlicesToTable(Container->BuildTableVerified(), slices));
}

std::shared_ptr<TGeneralContainer> ApplyFilter(
    const TColumnFilter& filter, const std::shared_ptr<TGeneralContainer>& container, const TColumnFilter::TApplyContext& context) {
    auto wrapper = std::make_shared<TGeneralContainerFilterable>(container);
    std::shared_ptr<TColumnFilter::IFilterable> filterable = wrapper;
    filter.Apply(filterable, context);
    return wrapper->GetData();
}

}   // namespace NKikimr::NArrow
