#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/services/metadata/abstract/fetcher.h>
#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/library/accessor/accessor.h>
#include <util/generic/singleton.h>
#include <util/generic/refcount.h>
#include <util/datetime/base.h>
#include <memory>

namespace NKikimr::NColumnShard {
class TTiersManager;
class TColumnShard;
}

namespace NKikimr::NOlap {
class TColumnEngineChanges;
class IBlobsGCAction;
namespace NStatistics {
class TOperatorContainer;
}
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
protected:
    virtual void DoOnTabletInitCompleted(const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
        return;
    }
    virtual void DoOnTabletStopped(const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
        return;
    }
    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& /*batch*/) {
        return true;
    }
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& /*changes*/) {
        return true;
    }
    virtual bool DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& /*changes*/, const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
        return true;
    }
    virtual bool DoOnWriteIndexStart(const ui64 /*tabletId*/, const TString& /*changeClassName*/) {
        return true;
    }
    virtual void DoOnAfterSharingSessionsManagerStart(const NColumnShard::TColumnShard& /*shard*/) {
    }
    virtual void DoOnAfterGCAction(const NColumnShard::TColumnShard& /*shard*/, const NOlap::IBlobsGCAction& /*action*/) {
    }
    virtual void DoOnDataSharingFinished(const ui64 /*tabletId*/, const TString& /*sessionId*/) {
    }
    virtual void DoOnDataSharingStarted(const ui64 /*tabletId*/, const TString & /*sessionId*/) {
    }

public:
    using TPtr = std::shared_ptr<ICSController>;
    virtual ~ICSController() = default;
    void OnDataSharingFinished(const ui64 tabletId, const TString& sessionId) {
        return DoOnDataSharingFinished(tabletId, sessionId);
    }
    void OnDataSharingStarted(const ui64 tabletId, const TString& sessionId) {
        return DoOnDataSharingStarted(tabletId, sessionId);
    }
    virtual void OnStatisticsUsage(const NOlap::NStatistics::TOperatorContainer& /*statOperator*/) {
        
    }
    virtual void OnMaxValueUsage() {
    }
    void OnTabletInitCompleted(const NColumnShard::TColumnShard& shard) {
        DoOnTabletInitCompleted(shard);
    }

    void OnTabletStopped(const NColumnShard::TColumnShard& shard) {
        DoOnTabletStopped(shard);
    }

    void OnAfterGCAction(const NColumnShard::TColumnShard& shard, const NOlap::IBlobsGCAction& action) {
        DoOnAfterGCAction(shard, action);
    }

    bool OnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
        return DoOnAfterFilterAssembling(batch);
    }
    bool OnWriteIndexComplete(const NOlap::TColumnEngineChanges& changes, const NColumnShard::TColumnShard& shard) {
        return DoOnWriteIndexComplete(changes, shard);
    }
    void OnAfterSharingSessionsManagerStart(const NColumnShard::TColumnShard& shard) {
        return DoOnAfterSharingSessionsManagerStart(shard);
    }
    bool OnWriteIndexStart(const ui64 tabletId, const TString& changeClassName) {
        return DoOnWriteIndexStart(tabletId, changeClassName);
    }
    bool OnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
        return DoOnStartCompaction(changes);
    }
    virtual void OnIndexSelectProcessed(const std::optional<bool> /*result*/) {
    }
    virtual TDuration GetReadTimeoutClean(const TDuration def) {
        return def;
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
    virtual TDuration GetPeriodicWakeupActivationPeriod(const TDuration defaultValue) const {
        return defaultValue;
    }
    virtual TDuration GetStatsReportInterval(const TDuration defaultValue) const {
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

    virtual void OnTieringUpdate(NMetadata::NFetcher::ISnapshot::TPtr /* snapshotExt */) {
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
