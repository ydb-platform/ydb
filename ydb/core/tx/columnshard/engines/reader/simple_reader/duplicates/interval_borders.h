
#pragma once

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/context.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TFindIntervalBorders: public NConveyor::ITask {
private:
    THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> DataByPortion;
    const std::shared_ptr<THashMap<ui64, std::shared_ptr<TPortionInfo>>> Portions;
    const std::shared_ptr<TInternalFilterConstructor> Context;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;
    TActorId Owner;

private:
    std::vector<TPortionsSlice> FindForSource(const NArrow::TSimpleRow& keyStart, const NArrow::TSimpleRow& keyEnd, const std::shared_ptr<arrow::Schema>& schema);

    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "FIND_INTERVAL_BORDERS";
    }

    const std::shared_ptr<TPortionInfo>& GetPortionVerified(const ui64 portionId) const {
        const auto* portion = Portions->FindPtr(portionId);
        AFL_VERIFY(portion)("portion", portionId);
        return *portion;
    }

public:
    TFindIntervalBorders(THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>&& dataByPortion,
        const std::shared_ptr<THashMap<ui64, std::shared_ptr<TPortionInfo>>>& portions,
        const std::shared_ptr<TInternalFilterConstructor>& context,
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard,
        const TActorId& owner)
        : DataByPortion(std::move(dataByPortion))
        , Portions(portions)
        , Context(context)
        , AllocationGuard(std::move(allocationGuard))
        , Owner(owner) {
    }
};

} // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering