#pragma once
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public ICSController {
private:
    YDB_READONLY(TAtomicCounter, FilteredRecordsCount, 0);
    YDB_READONLY(TAtomicCounter, Compactions, 0);
    YDB_READONLY(TAtomicCounter, Indexations, 0);
    YDB_READONLY(TAtomicCounter, IndexesSkippingOnSelect, 0);
    YDB_READONLY(TAtomicCounter, IndexesApprovedOnSelect, 0);
    YDB_READONLY(TAtomicCounter, IndexesSkippedNoData, 0);
    YDB_ACCESSOR(std::optional<TDuration>, GuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<TDuration>, PeriodicWakeupActivationPeriod, std::nullopt);
    YDB_ACCESSOR(std::optional<TDuration>, StatsReportInterval, std::nullopt);
    YDB_ACCESSOR(std::optional<ui64>, GuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OptimizerFreshnessCheckDuration, TDuration::Zero());
    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;
protected:
    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) override;
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) override;
    virtual bool DoOnWriteIndexComplete(const ui64 /*tabletId*/, const TString& /*changeClassName*/) override;
    virtual TDuration GetGuaranteeIndexationInterval(const TDuration defaultValue) const override {
        return GuaranteeIndexationInterval.value_or(defaultValue);
    }
    TDuration GetPeriodicWakeupActivationPeriod(const TDuration defaultValue) const override {
        return PeriodicWakeupActivationPeriod.value_or(defaultValue);
    }
    TDuration GetStatsReportInterval(const TDuration defaultValue) const override {
        return StatsReportInterval.value_or(defaultValue);
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
    virtual void OnIndexSelectProcessed(const std::optional<bool> result) override {
        if (!result) {
            IndexesSkippedNoData.Inc();
        } else if (*result) {
            IndexesApprovedOnSelect.Inc();
        } else {
            IndexesSkippingOnSelect.Inc();
        }
    }
    void SetCompactionControl(const EOptimizerCompactionWeightControl value) {
        CompactionControl = value;
    }

    bool HasPKSortingOnly() const;
    bool HasCompactions() const {
        return Compactions.Val();
    }
};

}
