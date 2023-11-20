#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/services/metadata/abstract/fetcher.h>
#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/library/accessor/accessor.h>
#include <util/generic/singleton.h>
#include <util/generic/refcount.h>
#include <util/datetime/base.h>
#include <memory>

namespace NKikimr::NOlap::NIndexedReader {
class IOrderPolicy;
}

namespace NKikimr::NColumnShard {
class TTiersManager;
}

namespace NKikimr::NOlap {
class TColumnEngineChanges;
}
namespace arrow {
class RecordBatch;
}

namespace NKikimr::NYDBTest {

enum class EOptimizerCompactionWeightControl {
    Disable,
    Default,
    Force
};

class ILocalDBModifier {
public:
    using TPtr = std::shared_ptr<ILocalDBModifier>;

    virtual ~ILocalDBModifier() {}

    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const = 0;
};

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
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& /*changes*/) {
        return true;
    }
    virtual bool DoOnWriteIndexComplete(const ui64 /*tabletId*/, const TString& /*changeClassName*/) {
        return true;
    }
    virtual bool DoOnWriteIndexStart(const ui64 /*tabletId*/, const TString& /*changeClassName*/) {
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
    bool OnWriteIndexComplete(const ui64 tabletId, const TString& changeClassName) {
        return DoOnWriteIndexComplete(tabletId, changeClassName);
    }
    bool OnWriteIndexStart(const ui64 tabletId, const TString& changeClassName) {
        return DoOnWriteIndexStart(tabletId, changeClassName);
    }
    bool OnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
        return DoOnStartCompaction(changes);
    }
    virtual EOptimizerCompactionWeightControl GetCompactionControl() const {
        return EOptimizerCompactionWeightControl::Force;
    }
    virtual TDuration GetTTLDefaultWaitingDuration(const TDuration defaultValue) const {
        return defaultValue;
    }
    virtual TDuration GetGuaranteeIndexationInterval(const TDuration defaultValue) const {
        return defaultValue;
    }
    virtual ui64 GetGuaranteeIndexationStartBytesLimit(const ui64 defaultValue) const {
        return defaultValue;
    }
    virtual TDuration GetOptimizerFreshnessCheckDuration(const TDuration defaultValue) const {
        return defaultValue;
    }

    virtual void OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& /*tiers*/) {
    }

    virtual ILocalDBModifier::TPtr BuildLocalBaseModifier() const {
        return nullptr;
    }

    virtual NMetadata::NFetcher::ISnapshot::TPtr GetFallbackTiersSnapshot() const {
        static std::shared_ptr<NColumnShard::NTiers::TConfigsSnapshot> result = std::make_shared<NColumnShard::NTiers::TConfigsSnapshot>(TInstant::Now());
        return result;
    }
};

class TControllers {
private:
    ICSController::TPtr CSController = std::make_shared<ICSController>();
public:
    template <class TController>
    class TGuard: TNonCopyable {
    private:
        std::shared_ptr<TController> Controller;
    public:
        TGuard(std::shared_ptr<TController> controller)
            : Controller(controller)
        {
            Y_ABORT_UNLESS(Controller);
        }

        TController* operator->() {
            return Controller.get();
        }

        ~TGuard() {
            Singleton<TControllers>()->CSController = std::make_shared<ICSController>();
        }
    };

    template <class T, class... Types>
    static TGuard<T> RegisterCSControllerGuard(Types... args) {
        auto result = std::make_shared<T>(args...);
        Singleton<TControllers>()->CSController = result;
        return result;
    }

    static ICSController::TPtr GetColumnShardController() {
        return Singleton<TControllers>()->CSController;
    }
};

}
