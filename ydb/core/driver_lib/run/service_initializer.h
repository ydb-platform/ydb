#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <ydb/core/base/appdata.h>

#include <util/generic/list.h>
#include <util/generic/ptr.h>

namespace NKikimr {

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
