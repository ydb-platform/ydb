#pragma once
#include "controller.h"
#include "events.h"
#include "request.h"

#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class IDataAccessorsManager {
private:
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) = 0;
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller) = 0;
    virtual void DoUnregisterController(const ui64 pathId) = 0;
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) = 0;
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) = 0;

public:
    virtual ~IDataAccessorsManager() = default;

    void AddPortion(const TPortionDataAccessor& accessor) {
        DoAddPortion(accessor);
    }
    void RemovePortion(const TPortionInfo::TConstPtr& portion) {
        DoRemovePortion(portion);
    }
    void AskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
        AFL_VERIFY(request);
        return DoAskData(request);
    }
    void RegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller) {
        AFL_VERIFY(controller);
        return DoRegisterController(std::move(controller));
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
    const NActors::TActorId ActorId;
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvAskDataAccessors>(request));
    }
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller) override {
        NActors::TActivationContext::Send(ActorId, std::make_unique<TEvRegisterController>(std::move(controller)));
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
    TActorAccessorsManager(const NActors::TActorId& actorId)
        : ActorId(actorId) {
    }
};

class TLocalManager: public IDataAccessorsManager {
private:
    THashMap<ui64, std::unique_ptr<IGranuleDataAccessor>> Managers;

    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override {
        for (auto&& i : request->GetPathIds()) {
            auto it = Managers.find(i);
            if (it == Managers.end()) {
                request->AddData(i, TConclusionStatus::Fail("incorrect pathId"));
            } else {
                it->second->AskData(request);
            }
        }
    }
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller) override {
        AFL_VERIFY(Managers.emplace(controller->GetPathId(), std::move(controller)).second);
    }
    virtual void DoUnregisterController(const ui64 pathId) override {
        AFL_VERIFY(Managers.erase(pathId));
    }
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) override {
        auto it = Managers.find(accessor.GetPortionInfo().GetPathId());
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions( { accessor }, {} );
    }
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portionInfo) override {
        auto it = Managers.find(portionInfo->GetPathId());
        AFL_VERIFY(it != Managers.end());
        it->second->ModifyPortions({}, { portionInfo->GetPortionId() });
    }

public:
    TLocalManager() = default;
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
