#pragma once
#include "events.h"
#include "request.h"

#include "abstract/collector.h"

#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TAccessorSignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    const NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
    const NMonitoring::TDynamicCounters::TCounterPtr FetchingCount;
    const NMonitoring::TDynamicCounters::TCounterPtr AskNew;
    const NMonitoring::TDynamicCounters::TCounterPtr AskDuplication;
    const NMonitoring::TDynamicCounters::TCounterPtr ResultFromCache;
    const NMonitoring::TDynamicCounters::TCounterPtr ResultAskDirectly;

    TAccessorSignals()
        : TBase("AccessorsFetching")
        , QueueSize(TBase::GetValue("Queue/Count"))
        , FetchingCount(TBase::GetValue("Fetching/Count"))
        , AskNew(TBase::GetDeriviative("Ask/Fault/Count"))
        , AskDuplication(TBase::GetDeriviative("Ask/Duplication/Count"))
        , ResultFromCache(TBase::GetDeriviative("ResultFromCache/Count"))
        , ResultAskDirectly(TBase::GetDeriviative("ResultAskDirectly/Count")) {
    }
};

class IDataAccessorsManager {
private:
    virtual void DoAskData(const TTabletId tabletId, const std::shared_ptr<TDataAccessorsRequest>& request) = 0;
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const TTabletId tabletId, const bool update) = 0;
    virtual void DoUnregisterController(const TTabletId tabletId, const TInternalPathId pathId) = 0;
    virtual void DoAddPortion(const TTabletId tabletId, const TPortionDataAccessor& accessor) = 0;
    virtual void DoRemovePortion(const TTabletId tabletId, const TPortionInfo::TConstPtr& portion) = 0;
    const NActors::TActorId TabletActorId;

public:
    const NActors::TActorId& GetTabletActorId() const {
        return TabletActorId;
    }

    IDataAccessorsManager(const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId) {
    }

    virtual ~IDataAccessorsManager() = default;

    void AddPortion(const TTabletId tabletId, const TPortionDataAccessor& accessor) {
        DoAddPortion(tabletId, accessor);
    }
    void RemovePortion(const TTabletId tabletId, const TPortionInfo::TConstPtr& portion) {
        DoRemovePortion(tabletId, portion);
    }
    void AskData(const TTabletId tabletId, const std::shared_ptr<TDataAccessorsRequest>& request) {
        AFL_VERIFY(request);
        AFL_VERIFY(request->HasSubscriber());
        return DoAskData(tabletId, request);
    }
    void RegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const TTabletId tabletId, const bool update) {
        AFL_VERIFY(controller);
        return DoRegisterController(std::move(controller), tabletId, update);
    }
    void UnregisterController(const TTabletId tabletId, const TInternalPathId pathId) {
        return DoUnregisterController(tabletId, pathId);
    }
};

class TDataAccessorsManagerContainer: public NBackgroundTasks::TControlInterfaceContainer<IDataAccessorsManager> {
private:
    using TBase = NBackgroundTasks::TControlInterfaceContainer<IDataAccessorsManager>;

public:
    using TBase::TBase;
};

class TActorAccessorsManager: public IDataAccessorsManager {
private:
    using TBase = IDataAccessorsManager;
    const NActors::TActorId ActorId;
    std::shared_ptr<NDataAccessorControl::IAccessorCallback> AccessorsCallback;
    virtual void DoAskData(const TTabletId tabletId, const std::shared_ptr<TDataAccessorsRequest>& request) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAskServiceDataAccessors>(tabletId, request));
    }
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const TTabletId tabletId, const bool update) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvRegisterController>(std::move(controller), tabletId, update));
    }
    virtual void DoUnregisterController(TTabletId tabletId, const TInternalPathId pathId) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvUnregisterController>(tabletId, pathId));
    }
    virtual void DoAddPortion(const TTabletId tabletId, const TPortionDataAccessor& accessor) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAddPortion>(tabletId, accessor));
    }
    virtual void DoRemovePortion(const TTabletId tabletId, const TPortionInfo::TConstPtr& portion) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvRemovePortion>(tabletId, portion));
    }

public:
    TActorAccessorsManager(const NActors::TActorId& actorId, const NActors::TActorId& tabletActorId)
        : TBase(tabletActorId)
        , ActorId(actorId)
        , AccessorsCallback(std::make_shared<TActorAccessorsCallback>(ActorId)) {
        AFL_VERIFY(!!tabletActorId);
    }
};

class TLocalManager: public IDataAccessorsManager {
private:
    using TBase = IDataAccessorsManager;
    using TManagerKey = std::pair<TTabletId, TInternalPathId>;

    static TManagerKey makeManagerKey(TTabletId tabletId, const TInternalPathId pathId) {
        return std::make_pair(tabletId, pathId);
    }
    THashMap<TManagerKey, std::unique_ptr<IGranuleDataAccessor>> Managers;
    THashMap<ui64, std::vector<std::shared_ptr<TDataAccessorsRequest>>> RequestsByPortion;
    TAccessorSignals Counters;
    const std::shared_ptr<IAccessorCallback> AccessorCallback;

    class TPortionToAsk {
    private:
        TPortionInfo::TConstPtr Portion;
        YDB_READONLY_DEF(std::shared_ptr<const TAtomicCounter>, AbortionFlag);

    public:
        TPortionToAsk(const TPortionInfo::TConstPtr& portion, const std::shared_ptr<const TAtomicCounter>& abortionFlag)
            : Portion(portion)
            , AbortionFlag(abortionFlag) {
        }

        TPortionInfo::TConstPtr ExtractPortion() {
            return std::move(Portion);
        }
    };

    std::deque<TPortionToAsk> PortionsAsk;
    TPositiveControlInteger PortionsAskInFlight;

    void DrainQueue(const TTabletId tabletId);

    virtual void DoAskData(TTabletId tabletId, const std::shared_ptr<TDataAccessorsRequest>& request) override;
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, TTabletId tabletId, const bool update) override;
    virtual void DoUnregisterController(TTabletId tabletId, const TInternalPathId pathId) override {
        AFL_VERIFY(Managers.erase(makeManagerKey(tabletId, pathId)));
    }
    virtual void DoAddPortion(const TTabletId tabletId, const TPortionDataAccessor& accessor) override;
    virtual void DoRemovePortion(TTabletId tabletId, const TPortionInfo::TConstPtr& portionInfo) override {
        auto it = Managers.find(makeManagerKey(tabletId, portionInfo->GetPathId()));
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions({}, { portionInfo->GetPortionId() });
    }

public:
    class TTestingCallback: public IAccessorCallback {
    private:
        std::weak_ptr<TLocalManager> Manager;
        virtual void OnAccessorsFetched(TTabletId tabletId, std::vector<TPortionDataAccessor>&& accessors) override {
            auto mImpl = Manager.lock();
            if (!mImpl) {
                return;
            }
            for (auto&& i : accessors) {
                mImpl->AddPortion(tabletId, i);
            }
        }

    public:
        explicit TTestingCallback() {}
        void InitManager(const std::weak_ptr<TLocalManager>& manager) {
            Manager = manager;
        }
    };

    static std::shared_ptr<TLocalManager> BuildForTests() {
        auto callback = std::make_shared<TTestingCallback>();
        std::shared_ptr<TLocalManager> result = std::make_shared<TLocalManager>(callback);
        callback->InitManager(result);
        return result;
    }

    explicit TLocalManager(const std::shared_ptr<IAccessorCallback>& callback)
        : TBase(NActors::TActorId())
        , AccessorCallback(callback) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
