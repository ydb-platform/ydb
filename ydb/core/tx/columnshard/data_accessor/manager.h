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

class TStatAddress {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY(ui64, Version, 0);
    YDB_READONLY(ui32, EntityId, 0);

public:
    TStatAddress(const ui64 pathId, const ui64 version, const ui32 entityId)
        : PathId(pathId)
        , Version(version)
        , EntityId(entityId)
    {

    }

    bool operator==(const TStatAddress& item) const {
        return PathId == item.PathId && Version == item.Version && EntityId == item.EntityId;
    }
};

class TEntityInfo: public TStatAddress {
private:
    using TBase = TStatAddress;
    YDB_READONLY(ui64, BlobBytes, 0);
    YDB_READONLY(ui64, RawBytes, 0);

public:
    TEntityInfo(const ui64 pathId, const ui64 version, const ui32 entityId, const ui64 blobBytes, const ui64 rawBytes)
        : TBase(pathId, version, entityId)
        , BlobBytes(blobBytes)
        , RawBytes(rawBytes) {
    }
};

class TEntityStatInfo {
private:
    YDB_READONLY(ui64, BlobBytes, 0);
    YDB_READONLY(ui64, RawBytes, 0);
    YDB_READONLY(ui64, Count, 0);

public:
    TEntityStatInfo() = default;

    void AddInfo(const TEntityInfo& info) {
        BlobBytes += info.GetBlobBytes();
        RawBytes += info.GetRawBytes();
        ++Count;
    }

    void RemoveInfo(const TEntityInfo& info) {
        AFL_VERIFY(BlobBytes >= info.GetBlobBytes());
        AFL_VERIFY(RawBytes >= info.GetRawBytes());
        BlobBytes -= info.GetBlobBytes();
        RawBytes -= info.GetRawBytes();
        AFL_VERIFY(Count);
        --Count;
    }
};

class TEntitiesStatInfo {
private:
    THashMap<ui32, TEntityStatInfo> Entities;
    TEntityStatInfo Global;

public:
    const TEntityStatInfo& GetGlobal() const {
        return Global;
    }

    void AddInfo(const TEntityInfo& info) {
        Entities[info.GetEntityId()].AddInfo(info);
        Global.AddInfo(info);
    }
    void RemoveInfo(const TEntityInfo& info) {
        Entities[info.GetEntityId()].RemoveInfo(info);
        Global.RemoveInfo(info);
    }
};

class TVersionsStatInfo {
private:
    THashMap<ui64, TEntitiesStatInfo> Versions;
    TEntityStatInfo Global;

public:
    const TEntityStatInfo& GetGlobal() const {
        return Global;
    }

    void AddInfo(const TEntityInfo& info) {
        Versions[info.GetVersion()].AddInfo(info);
        Global.AddInfo(info);
    }
    void RemoveInfo(const TEntityInfo& info) {
        auto itVersion = Versions.find(info.GetVersion());
        AFL_VERIFY(itVersion != Versions.end());
        itVersion->second.RemoveInfo(info);
        if (itVersion->second.GetGlobal().GetCount() == 0) {
            Versions.erase(itVersion);
        }
        Global.RemoveInfo(info);
    }
};

class TPathesStatInfo {
private:
    THashMap<ui64, TVersionsStatInfo> Pathes;

public:
    void AddInfo(const TEntityInfo& info) {
        Pathes[info.GetPathId()].AddInfo(info);
    }
    void RemoveInfo(const TEntityInfo& info) {
        auto it = Pathes.find(info.GetVersion());
        AFL_VERIFY(it != Pathes.end());
        it->second.RemoveInfo(info);
        if (it->second.GetGlobal().GetCount() == 0) {
            Pathes.erase(it);
        }
    }
};

class TLocalManager: public IDataAccessorsManager {
private:
    THashMap<ui64, std::unique_ptr<IGranuleDataAccessor>> Managers;
    TPathesStatInfo CompactedStatsInfo;
    TPathesStatInfo InsertedStatsInfo;

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

        TPathesStatInfo* info = nullptr;
        if (accessor.GetPortionInfo().GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
            info = &InsertedStatsInfo;
        } else {
            info = &CompactedStatsInfo;
        }
        ui64 correctVersion = 0;
        for (auto&& i : accessor.GetRecords()) {
            info->AddInfo(TEntityInfo(accessor.GetPortionInfo().GetPathId(), i.GetEntityId(),
                accessor.GetPortionInfo().GetSchemaVersionVerified() | correctVersion, accessor.GetColumnBlobBytes({ i.GetEntityId() }),
                accessor.GetColumnRawBytes({ i.GetEntityId() })));
        }
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
