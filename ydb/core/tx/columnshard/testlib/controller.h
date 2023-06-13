#pragma once
#include <ydb/core/testlib/controllers/abstract.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public ICSController {
private:
    YDB_READONLY(ui32, SortingWithLimit, 0);
    YDB_READONLY(ui32, AnySorting, 0);
protected:
    virtual bool DoOnSortingPolicy(std::shared_ptr<NOlap::NIndexedReader::IOrderPolicy> policy);
public:
    bool HasPKSortingOnly() const {
        return SortingWithLimit && !AnySorting;
    }
};

}
