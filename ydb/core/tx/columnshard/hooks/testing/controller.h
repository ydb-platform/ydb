#pragma once
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public ICSController {
private:
    YDB_READONLY(TAtomicCounter, FilteredRecordsCount, 0);
    YDB_READONLY(TAtomicCounter, Compactions, 0);
protected:
    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) override;
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) override;

public:
    bool HasPKSortingOnly() const;
    bool HasCompactions() const {
        return Compactions.Val();
    }
};

}
