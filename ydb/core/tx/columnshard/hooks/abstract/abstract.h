#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>

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
class TPortionInfo;
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
public:
    enum class EBackground {
        Indexation,
        Compaction,
        TTL,
        Cleanup,
        GC
    };
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
    virtual bool DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& /*changes*/, const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
        return true;
    }
    virtual bool DoOnWriteIndexStart(const ui64 /*tabletId*/, NOlap::TColumnEngineChanges& /*change*/) {
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
    virtual bool IsBackgroundEnabled(const EBackground /*id*/) const {
        return true;
    }

    using TPtr = std::shared_ptr<ICSController>;
    virtual ~ICSController() = default;

    virtual void OnSelectShardingFilter() {
    
    }

    virtual TDuration GetCompactionActualizationLag(const TDuration def) const {
        return def;
    }

    virtual NColumnShard::TBlobPutResult::TPtr OverrideBlobPutResultOnCompaction(const NColumnShard::TBlobPutResult::TPtr original, const NOlap::TWriteActionsCollection& /*actions*/) const {
        return original;
    }

    virtual TDuration GetRemovedPortionLivetime(const TDuration def) const {
        return def;
    }

    virtual ui64 GetReduceMemoryIntervalLimit(const ui64 def) const {
        return def;
    }
    virtual ui64 GetRejectMemoryIntervalLimit(const ui64 def) const {
        return def;
    }
    virtual bool NeedForceCompactionBacketsConstruction() const {
        return false;
    }
    virtual ui64 GetSmallPortionSizeDetector(const ui64 def) const {
        return def;
    }
    virtual void OnExportFinished() {

    }
    virtual void OnActualizationRefreshScheme() {

    }
    virtual void OnActualizationRefreshTiering() {

    }
    virtual void AddPortionForActualizer(const i32 /*portionsCount*/) {

    }

    void OnDataSharingFinished(const ui64 tabletId, const TString& sessionId) {
        return DoOnDataSharingFinished(tabletId, sessionId);
    }
    void OnDataSharingStarted(const ui64 tabletId, const TString& sessionId) {
        return DoOnDataSharingStarted(tabletId, sessionId);
    }
    virtual void OnStatisticsUsage(const NOlap::NStatistics::TOperatorContainer& /*statOperator*/) {

    }
    virtual void OnPortionActualization(const NOlap::TPortionInfo& /*info*/) {

    }
    virtual void OnMaxValueUsage() {
    }

    virtual TDuration GetLagForCompactionBeforeTierings(const TDuration def) const {
        return def;
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
    bool OnWriteIndexStart(const ui64 tabletId, NOlap::TColumnEngineChanges& change) {
        return DoOnWriteIndexStart(tabletId, change);
    }
    virtual void OnIndexSelectProcessed(const std::optional<bool> /*result*/) {
    }
    virtual TDuration GetReadTimeoutClean(const TDuration def) {
        return def;
    }
    virtual EOptimizerCompactionWeightControl GetCompactionControl() const {
        return EOptimizerCompactionWeightControl::Default;
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

    virtual ILocalDBModifier::TPtr BuildLocalBaseModifier() const {
        return nullptr;
    }

    virtual NMetadata::NFetcher::ISnapshot::TPtr GetFallbackTiersSnapshot() const {
        static std::shared_ptr<NColumnShard::NTiers::TConfigsSnapshot> result = std::make_shared<NColumnShard::NTiers::TConfigsSnapshot>(TInstant::Now());
        return result;
    }

    virtual void OnSwitchToWork(const ui64 tabletId) {
        Y_UNUSED(tabletId);
    }

    virtual void OnCleanupActors(const ui64 tabletId) {
        Y_UNUSED(tabletId);
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

    template <class T>
    static T* GetControllerAs() {
        auto controller = Singleton<TControllers>()->CSController;
        return dynamic_cast<T*>(controller.get());
    }
};

}
