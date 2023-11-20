#pragma once
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public ICSController {
private:
    YDB_READONLY(TAtomicCounter, FilteredRecordsCount, 0);
    YDB_READONLY(TAtomicCounter, Compactions, 0);
    YDB_ACCESSOR(std::optional<TDuration>, GuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<ui64>, GuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OptimizerFreshnessCheckDuration, TDuration::Zero());
    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;
protected:
    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) override;
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) override;
    virtual TDuration GetGuaranteeIndexationInterval(const TDuration defaultValue) const override {
        return GuaranteeIndexationInterval.value_or(defaultValue);
    }
    virtual ui64 GetGuaranteeIndexationStartBytesLimit(const ui64 defaultValue) const override {
        return GuaranteeIndexationStartBytesLimit.value_or(defaultValue);
    }
    virtual TDuration GetOptimizerFreshnessCheckDuration(const TDuration defaultValue) const override {
        return OptimizerFreshnessCheckDuration.value_or(defaultValue);
    }
    virtual EOptimizerCompactionWeightControl GetCompactionControl() const override {
        return CompactionControl;
    }

public:
    void SetCompactionControl(const EOptimizerCompactionWeightControl value) {
        CompactionControl = value;
    }

    bool HasPKSortingOnly() const;
    bool HasCompactions() const {
        return Compactions.Val();
    }
};

}
