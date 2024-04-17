#pragma once
#include "defs.h"
#include "tenant_runtime.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/console_tenants_manager.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

struct TSlotRequest {
    TString Type;
    TString Zone;
    ui64 Count;
};

struct TSlotState {
    ui64 Required = 0;
    ui64 Allocated = 0;
};

struct TPoolAllocation {
    TPoolAllocation(const TString &type = "", ui32 size = 0, ui32 allocated = 0)
        : PoolType(type)
        , PoolSize(size)
        , Allocated(allocated)
    {
    }

    TString PoolType;
    ui32 PoolSize;
    ui32 Allocated;
};

struct TUnitRegistration {
    TUnitRegistration(const TString &host = "", ui32 port = 0, const TString &kind = "")
        : Host(host)
        , Port(port)
        , Kind(kind)
    {
    }

    TString Host;
    ui32 Port;
    TString Kind;
};

inline void CollectSlots(TVector<TSlotRequest> &)
{
}

template <typename ...Ts>
void CollectSlots(TVector<TSlotRequest> &requests,
                  const TString &type,
                  const TString &zone,
                  ui64 count,
                  Ts... args)
{
    requests.push_back({type, zone, count});
    CollectSlots(requests, args...);
}

inline void CollectSlots(THashMap<std::pair<TString, TString>, TSlotState> &)
{
}

template <typename ...Ts>
void CollectSlots(THashMap<std::pair<TString, TString>, TSlotState> &slots,
                  const TString &type,
                  const TString &zone,
                  ui64 required,
                  ui64 allocated,
                  Ts... args)
{
    auto &slot = slots[std::make_pair(type, zone)];
    slot.Required += required;
    slot.Allocated += allocated;
    CollectSlots(slots, args...);
}

struct TCreateTenantRequest {
    using TSelf = TCreateTenantRequest;
    using TAttrsCont = TVector<std::pair<TString, TString>>;
    using TPoolsCont = TVector<TPoolAllocation>;
    using TSlotsCont = TVector<TSlotRequest>;

    enum class EType {
        Unspecified,
        Common,
        Shared,
        Serverless,
    };

    // All types
    TString Path;
    EType Type;
    TAttrsCont Attrs;
    Ydb::Cms::DatabaseQuotas DatabaseQuotas;
    // Common & Shared
    TPoolsCont Pools;
    TSlotsCont Slots;
    // Serverless
    TString SharedDbPath;
    ui32 PlanResolution = 0;

    TCreateTenantRequest() = delete;

    explicit TCreateTenantRequest(const TString& path, EType type = EType::Unspecified)
        : Path(path)
        , Type(type)
    {
    }

    TSelf& WithAttrs(const TAttrsCont& attrs) {
        Attrs = attrs;
        return *this;
    }

    TSelf& WithDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& quotas) {
        DatabaseQuotas = quotas;
        return *this;
    }

    TSelf& WithDatabaseQuotas(const TString& quotas) {
        Ydb::Cms::DatabaseQuotas parsedQuotas;
        UNIT_ASSERT_C(NProtoBuf::TextFormat::ParseFromString(quotas, &parsedQuotas), quotas);
        DatabaseQuotas = std::move(parsedQuotas);
        return *this;
    }

    TSelf& WithPools(const TPoolsCont& pools) {
        if (Type == EType::Unspecified) {
            Type = EType::Common;
        }

        UNIT_ASSERT(Type == EType::Common || Type == EType::Shared);
        Pools = pools;
        return *this;
    }

    TSelf& WithSlots(const TSlotsCont& slots) {
        if (Type == EType::Unspecified) {
            Type = EType::Common;
        }

        UNIT_ASSERT(Type == EType::Common || Type == EType::Shared);
        Slots = slots;
        return *this;
    }

    TSelf& WithSlots(const TString& type, const TString& zone, ui64 count) {
        if (Type == EType::Unspecified) {
            Type = EType::Common;
        }

        UNIT_ASSERT(Type == EType::Common || Type == EType::Shared);
        Slots.push_back({type, zone, count});
        return *this;
    }

    TSelf& WithSharedDbPath(const TString& path) {
        if (Type == EType::Unspecified) {
            Type = EType::Serverless;
        }

        UNIT_ASSERT(Type == EType::Serverless);
        SharedDbPath = path;
        return *this;
    }

    TSelf& WithPlanResolution(ui32 planResolution) {
        PlanResolution = planResolution;
        return *this;
    }
};

inline bool CompareState(THashMap<std::pair<TString, TString>, TSlotState> slots,
                         THashMap<TString, TPoolAllocation> pools,
                         THashMap<std::pair<TString, ui32>, TUnitRegistration> registrations,
                         const Ydb::Cms::GetDatabaseStatusResult &status, bool shared = false)
{
    const auto& resources = shared ? status.required_shared_resources() : status.required_resources();

    for (auto &unit : resources.computational_units()) {
        auto key = std::make_pair(unit.unit_kind(), unit.availability_zone());
        auto count = unit.count();
        if (!slots.contains(key))
            return false;
        if (slots[key].Required != count)
            return false;
        slots[key].Required = 0;
    }

    for (auto &unit : resources.storage_units()) {
        auto key = unit.unit_kind();
        auto size = unit.count();
        if (!pools.contains(key))
            return false;
        if (pools[key].PoolSize != size)
            return false;
        pools[key].PoolSize = 0;
    }

    for (auto &unit : status.allocated_resources().computational_units()) {
        auto key = std::make_pair(unit.unit_kind(), unit.availability_zone());
        auto count = unit.count();
        if (!slots.contains(key))
            return false;
        if (slots[key].Allocated != count)
            return false;
        slots[key].Allocated = 0;
    }

    for (auto &unit : status.allocated_resources().storage_units()) {
        auto key = unit.unit_kind();
        auto size = unit.count();
        if (!pools.contains(key))
            return false;
        if (pools[key].Allocated != size)
            return false;
        pools[key].Allocated = 0;
    }

    for (auto &unit : status.registered_resources()) {
        auto key = std::make_pair(unit.host(), unit.port());
        if (!registrations.contains(key))
            return false;
        if (registrations.at(key).Kind != unit.unit_kind())
            return false;
        registrations.erase(key);
    }

    for (auto &pr : slots) {
        if (pr.second.Required || pr.second.Allocated)
            return false;
    }

    for (auto &pr : pools) {
        if (pr.second.PoolSize || pr.second.Allocated)
            return false;
    }

    if (registrations.size())
        return false;

    return true;
}

template <typename ...Ts>
inline void CheckTenantStatus(TTenantTestRuntime &runtime, const TString &path, bool shared,
                       Ydb::StatusIds::StatusCode code,
                       Ydb::Cms::GetDatabaseStatusResult::State state,
                       TVector<TPoolAllocation> poolTypes,
                       TVector<TUnitRegistration> unitRegistrations,
                       Ts... args)
{
    THashMap<std::pair<TString, TString>, TSlotState> slots;
    CollectSlots(slots, args...);

    THashMap<TString, TPoolAllocation> pools;
    for (auto &pool : poolTypes)
        pools[pool.PoolType] = pool;

    THashMap<std::pair<TString, ui32>, TUnitRegistration> registrations;
    for (auto &reg : unitRegistrations)
        registrations[std::make_pair(reg.Host, reg.Port)] = reg;

    bool ok = false;
    while (!ok) {
        auto *event = new NConsole::TEvConsole::TEvGetTenantStatusRequest;
        event->Record.MutableRequest()->set_path(path);

        TAutoPtr<IEventHandle> handle;
        runtime.SendToConsole(event);
        auto reply = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvGetTenantStatusResponse>(handle);
        auto &operation = reply->Record.GetResponse().operation();
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), code);
        if (code != Ydb::StatusIds::SUCCESS)
            return;

        Ydb::Cms::GetDatabaseStatusResult status;
        UNIT_ASSERT(operation.result().UnpackTo(&status));

        UNIT_ASSERT_VALUES_EQUAL(status.path(), CanonizePath(path));

        ok = status.state() == state && CompareState(slots, pools, registrations, status, shared);
        if (!ok) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvRetryAllocateResources);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvSubdomainFailed);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvSubdomainCreated);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvSubdomainReady);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvSubdomainRemoved);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvPoolAllocated);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvPoolFailed);
            options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvPoolDeleted);
            runtime.DispatchEvents(options, TDuration::Seconds(10));
        }
    }
}

template <typename ...Ts>
inline void CheckTenantStatus(TTenantTestRuntime &runtime, const TString &path,
                       Ydb::StatusIds::StatusCode code,
                       Ydb::Cms::GetDatabaseStatusResult::State state,
                       TVector<TPoolAllocation> poolTypes,
                       TVector<TUnitRegistration> unitRegistrations,
                       Ts... args)
{
    CheckTenantStatus(runtime, path, false, code, state, poolTypes, unitRegistrations, args...);
}


inline void CheckCreateTenant(TTenantTestRuntime &runtime,
                       const TString &token,
                       Ydb::StatusIds::StatusCode code,
                       const TCreateTenantRequest &request)
{
    using EType = TCreateTenantRequest::EType;

    auto *event = new NConsole::TEvConsole::TEvCreateTenantRequest;
    event->Record.MutableRequest()->set_path(request.Path);

    auto *resources = request.Type == EType::Shared
        ? event->Record.MutableRequest()->mutable_shared_resources()
        : event->Record.MutableRequest()->mutable_resources();

    for (auto &req : request.Slots) {
        auto &unit = *resources->add_computational_units();
        unit.set_unit_kind(req.Type);
        unit.set_availability_zone(req.Zone);
        unit.set_count(req.Count);
    }

    for (auto &pool : request.Pools) {
        auto &unit = *resources->add_storage_units();
        unit.set_unit_kind(pool.PoolType);
        unit.set_count(pool.PoolSize);
    }

    if (token)
        event->Record.SetUserToken(token);

    for (const auto& [key, value] : request.Attrs) {
        (*event->Record.MutableRequest()->mutable_attributes())[key] = value;
    }

    if (request.Type == EType::Serverless) {
        event->Record.MutableRequest()->mutable_serverless_resources()->set_shared_database_path(request.SharedDbPath);
    }

    if (request.PlanResolution) {
        event->Record.MutableRequest()->mutable_options()->set_plan_resolution(request.PlanResolution);
    }
    
    event->Record.MutableRequest()->mutable_database_quotas()->CopyFrom(request.DatabaseQuotas);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvCreateTenantResponse>(handle);
    auto &operation = reply->Record.GetResponse().operation();

    if (operation.ready()) {
        UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), code, operation.DebugString());
    } else {
        TString id = operation.id();
        auto *request = new NConsole::TEvConsole::TEvNotifyOperationCompletionRequest;
        request->Record.MutableRequest()->set_id(id);

        runtime.SendToConsole(request);
        TAutoPtr<IEventHandle> handle;
        auto replies = runtime.GrabEdgeEventsRethrow<NConsole::TEvConsole::TEvNotifyOperationCompletionResponse,
                                                     NConsole::TEvConsole::TEvOperationCompletionNotification>(handle);
        auto *resp = std::get<1>(replies);
        if (!resp) {
            UNIT_ASSERT(!std::get<0>(replies)->Record.GetResponse().operation().ready());
            resp = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvOperationCompletionNotification>(handle);
        }

        UNIT_ASSERT(resp->Record.GetResponse().operation().ready());
        UNIT_ASSERT_VALUES_EQUAL(resp->Record.GetResponse().operation().status(), code);
    }
}

inline void CheckCreateTenant(TTenantTestRuntime &runtime,
                       Ydb::StatusIds::StatusCode code,
                       const TCreateTenantRequest &request)
{
    CheckCreateTenant(runtime, "", code, request);
}

template <typename ...Ts>
void CheckCreateTenant(TTenantTestRuntime &runtime,
                       const TString &path,
                       const TString &token,
                       Ydb::StatusIds::StatusCode code,
                       TVector<TPoolAllocation> pools,
                       const TVector<std::pair<TString, TString>> &attrs,
                       Ts... args)
{
    TVector<TSlotRequest> slots;
    CollectSlots(slots, args...);

    CheckCreateTenant(runtime, token, code,
        TCreateTenantRequest(path)
            .WithSlots(slots)
            .WithPools(pools)
            .WithAttrs(attrs));
}

template <typename ...Ts>
void CheckCreateTenant(TTenantTestRuntime &runtime,
                       const TString &path,
                       Ydb::StatusIds::StatusCode code,
                       TVector<TPoolAllocation> pools,
                       const TVector<std::pair<TString, TString>> &attrs,
                       Ts... args)
{
    CheckCreateTenant(runtime, path, "", code, pools, attrs, args...);
}

template <typename ...Ts>
void CheckCreateTenant(TTenantTestRuntime &runtime,
                       const TString &path,
                       const TString &token,
                       Ydb::StatusIds::StatusCode code,
                       TVector<TPoolAllocation> pools,
                       Ts... args)
{
    CheckCreateTenant(runtime, path, token, code, pools, {}, args...);
}

template <typename ...Ts>
void CheckCreateTenant(TTenantTestRuntime &runtime,
                       const TString &path,
                       Ydb::StatusIds::StatusCode code,
                       TVector<TPoolAllocation> pools,
                       Ts... args)
{
    CheckCreateTenant(runtime, path, "", code, pools, {}, args...);
}

inline void CheckRemoveTenant(TTenantTestRuntime &runtime,
                              const TString &path,
                              const TString &token,
                              Ydb::StatusIds::StatusCode code)
{
    auto *event = new NConsole::TEvConsole::TEvRemoveTenantRequest;
    event->Record.MutableRequest()->set_path(path);
    if (token)
        event->Record.SetUserToken(token);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvRemoveTenantResponse>(handle);
    auto &operation = reply->Record.GetResponse().operation();

    if (operation.ready()) {
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), code);
    } else {
        TString id = operation.id();
        for (;;) {
            auto check = new NConsole::TEvConsole::TEvGetOperationRequest;
            check->Record.MutableRequest()->set_id(id);
            runtime.SendToConsole(check);
            auto checkReply = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvGetOperationResponse>(handle);
            auto &checkOperation = checkReply->Record.GetResponse().operation();
            if (checkOperation.ready()) {
                UNIT_ASSERT_VALUES_EQUAL(checkOperation.status(), code);
                break;
            }

            TDispatchOptions options;
            runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        }
    }
}

inline void CheckRemoveTenant(TTenantTestRuntime &runtime,
                              const TString &path,
                              Ydb::StatusIds::StatusCode code)
{
    CheckRemoveTenant(runtime, path, "", code);
}

inline void WaitTenantStatus(TTenantTestRuntime &runtime,
                             const TString &path,
                             TVector<Ydb::Cms::GetDatabaseStatusResult::State> expected)
{
    for (;;) {
        auto *req = new NConsole::TEvConsole::TEvGetTenantStatusRequest;
        req->Record.MutableRequest()->set_path(path);
        runtime.SendToConsole(req);

        auto ev = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvGetTenantStatusResponse>(runtime.Sender);
        auto &operation = ev->Get()->Record.GetResponse().operation();
        UNIT_ASSERT_C(operation.status() == Ydb::StatusIds::SUCCESS,
                "Unexpected status " << operation.status() << " for tenant " << path);

        Ydb::Cms::GetDatabaseStatusResult result;
        UNIT_ASSERT_C(operation.result().UnpackTo(&result),
                "Failed to unpack status result for tenant " << path);

        if (std::find(expected.begin(), expected.end(), result.state()) != expected.end())
            break;

        TDispatchOptions options;
        runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
    }
}

inline void WaitTenantRunning(TTenantTestRuntime &runtime, const TString &path) {
    WaitTenantStatus(runtime, path, { Ydb::Cms::GetDatabaseStatusResult::RUNNING });
}

} // namespace NKikimr
