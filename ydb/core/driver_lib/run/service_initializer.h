#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/base/appdata.h>

#include <util/generic/list.h>
#include <util/generic/ptr.h>

namespace NKikimr {

/**
 * Storage for global objects that must survive as long as the actor system
 */
class IGlobalObjectStorage {
protected:
    ~IGlobalObjectStorage() = default;

public:
    virtual void AddGlobalObject(std::shared_ptr<void> object) = 0;
};

struct IAppDataInitializer : public virtual TThrRefBase {
    virtual void Initialize(NKikimr::TAppData* appData) = 0;

};


class TAppDataInitializersList : public IAppDataInitializer {

    TList<TIntrusivePtr<IAppDataInitializer> > AppDataInitializersList;

public:
    void AddAppDataInitializer(TIntrusivePtr<IAppDataInitializer> appDataInitializer);

    virtual void Initialize(NKikimr::TAppData* appData) override;
};


struct IServiceInitializer: public virtual TThrRefBase {
    virtual void InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) = 0;
};


class TServiceInitializersList : public IServiceInitializer {

    TList<TIntrusivePtr<IServiceInitializer> > ServiceInitializersList;

public:
    void AddServiceInitializer(TIntrusivePtr<IServiceInitializer> serviceInitializer);

    virtual void InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) override;
};

} // namespace NKikimr
