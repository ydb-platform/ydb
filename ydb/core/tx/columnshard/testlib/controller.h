#pragma once
#include <ydb/core/testlib/controllers/abstract.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public ICSController {
private:
    YDB_READONLY(TAtomicCounter, SortingWithLimit, 0);
    YDB_READONLY(TAtomicCounter, AnySorting, 0);
protected:
    virtual bool DoOnSortingPolicy(std::shared_ptr<NOlap::NIndexedReader::IOrderPolicy> policy);
public:
    bool HasPKSortingOnly() const;
};

}
