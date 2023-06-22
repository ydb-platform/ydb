#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/generic/singleton.h>
#include <util/generic/refcount.h>
#include <memory>

namespace NKikimr::NOlap::NIndexedReader {
class IOrderPolicy;
}
namespace NKikimr::NOlap {
class TColumnEngineChanges;
}
namespace arrow {
class RecordBatch;
}

namespace NKikimr::NYDBTest {

class ICSController {
private:
    YDB_READONLY(TAtomicCounter, OnSortingPolicyCounter, 0);
protected:
    virtual bool DoOnSortingPolicy(std::shared_ptr<NOlap::NIndexedReader::IOrderPolicy> /*policy*/) {
        return true;
    }
    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& /*batch*/) {
        return true;
    }
    virtual bool DoOnStartCompaction(const std::shared_ptr<NOlap::TColumnEngineChanges>& /*changes*/) {
        return true;
    }
public:
    using TPtr = std::shared_ptr<ICSController>;
    virtual ~ICSController() = default;
    bool OnSortingPolicy(std::shared_ptr<NOlap::NIndexedReader::IOrderPolicy> policy) {
        OnSortingPolicyCounter.Inc();
        return DoOnSortingPolicy(policy);
    }
    bool OnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
        return DoOnAfterFilterAssembling(batch);
    }
    bool OnStartCompaction(const std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
        return DoOnStartCompaction(changes);
    }
};

class TControllers {
private:
    ICSController::TPtr CSController = std::make_shared<ICSController>();
public:
    template <class T, class... Types>
    static std::shared_ptr<T> RegisterCSController(Types... args) {
        auto result = std::make_shared<T>(args...);
        Singleton<TControllers>()->CSController = result;
        return result;
    }

    static ICSController::TPtr GetColumnShardController() {
        return Singleton<TControllers>()->CSController;
    }
};

}
