#pragma once
#include "events.h"
#include "request.h"

#include "abstract/collector.h"

#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class IDataAccessorsManager {
private:
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) = 0;
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) = 0;
    virtual void DoUnregisterController(const ui64 pathId) = 0;
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) = 0;
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) = 0;
    const NActors::TActorId TabletActorId;

public:
    const NActors::TActorId& GetTabletActorId() const {
        return TabletActorId;
    }

    IDataAccessorsManager(const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId) {
    }

    virtual ~IDataAccessorsManager() = default;

    void AddPortion(const TPortionDataAccessor& accessor) {
        DoAddPortion(accessor);
    }
    void RemovePortion(const TPortionInfo::TConstPtr& portion) {
        DoRemovePortion(portion);
    }
    void AskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
        AFL_VERIFY(request);
        AFL_VERIFY(request->HasSubscriber());
        return DoAskData(request);
    }
    void RegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) {
        AFL_VERIFY(controller);
        return DoRegisterController(std::move(controller), update);
    }
    void UnregisterController(const ui64 pathId) {
        return DoUnregisterController(pathId);
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
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAskServiceDataAccessors>(request));
    }
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvRegisterController>(std::move(controller), update));
    }
    virtual void DoUnregisterController(const ui64 pathId) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvUnregisterController>(pathId));
    }
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAddPortion>(accessor));
    }
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvRemovePortion>(portion));
    }

public:
    TActorAccessorsManager(const NActors::TActorId& actorId, const NActors::TActorId& tabletActorId)
        : TBase(tabletActorId)
        , ActorId(actorId) 
        , AccessorsCallback(std::make_shared<TActorAccessorsCallback>(ActorId))
    {

        AFL_VERIFY(!!tabletActorId);
    }
};

class TLocalManager: public IDataAccessorsManager {
private:
    using TBase = IDataAccessorsManager;
    THashMap<ui64, std::unique_ptr<IGranuleDataAccessor>> Managers;
    THashMap<ui64, std::vector<std::shared_ptr<TDataAccessorsRequest>>> RequestsByPortion;
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
    ui64 PortionsAskInFlight = 0;

    void DrainQueue();

    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override;
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) override;
    virtual void DoUnregisterController(const ui64 pathId) override {
        AFL_VERIFY(Managers.erase(pathId));
    }
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) override;
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portionInfo) override {
        auto it = Managers.find(portionInfo->GetPathId());
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions({}, { portionInfo->GetPortionId() });
    }

public:
    class TTestingCallback: public IAccessorCallback {
    private:
        std::weak_ptr<TLocalManager> Manager;
        virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) override {
            auto mImpl = Manager.lock();
            if (!mImpl) {
                return;
            }
            for (auto&& i : accessors) {
                mImpl->AddPortion(i);
            }
        }

    public:
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

    TLocalManager(const std::shared_ptr<IAccessorCallback>& callback)
        : TBase(NActors::TActorId())
        , AccessorCallback(callback)
    {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
