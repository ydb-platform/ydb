#pragma once
#include <ydb/core/formats/arrow/container/container.h>
#include <ydb/core/formats/arrow/filter/filter.h>

#include <memory>

namespace NKikimr::NArrow {

// IFilterable implementation for TGeneralContainer. Lives separately from the `filter`
// library so that `filter` does not depend on `container`.
class TGeneralContainerFilterable: public TColumnFilter::IFilterable {
private:
    std::shared_ptr<TGeneralContainer> Container;

public:
    explicit TGeneralContainerFilterable(const std::shared_ptr<TGeneralContainer>& container)
        : Container(container) {
    }

    const std::shared_ptr<TGeneralContainer>& GetData() const {
        return Container;
    }

    ui32 GetRecordsCount() const override;
    void ApplyArrowFilter(const TColumnFilter& filter) override;
    void ApplySlicesFilter(TColumnFilter::TSlicesIterator slices) override;
    void ApplyEmpty() override;
};

// Applies `filter` to `container` (wrapping it into TGeneralContainerFilterable) and returns the filtered container.
[[nodiscard]] std::shared_ptr<TGeneralContainer> ApplyFilter(const TColumnFilter& filter,
    const std::shared_ptr<TGeneralContainer>& container,
    const TColumnFilter::TApplyContext& context = Default<TColumnFilter::TApplyContext>());

}   // namespace NKikimr::NArrow
