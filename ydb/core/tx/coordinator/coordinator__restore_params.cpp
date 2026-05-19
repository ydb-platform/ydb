#include "coordinator_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COORDINATOR

namespace NKikimr::NFlatTxCoordinator {

class TTxCoordinator::TRestoreProcessingParamsActor
    : public TActorBootstrapped<TRestoreProcessingParamsActor>
{
public:
    TRestoreProcessingParamsActor(
            const TActorId& owner,
            ui64 tabletId,
            const TPathId& tenantPathId,
            ui64 version)
        : Owner(owner)
        , TabletId(tabletId)
        , TenantPathId(tenantPathId)
        , Version(version)
    { }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::COORDINATOR_RESTORE_PROCESSING_PARAMS;
    }

    void Bootstrap() {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(TenantPathId));
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        TActorBootstrapped::PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable, Handle);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
        const auto* msg = ev->Get();
        const auto& domainDescription = msg->Result->GetPathDescription().GetDomainDescription();
        if (!domainDescription.HasProcessingParams()) {
            // Wait for description with processing params
            YDB_LOG_WARN("ignoring update for path without processing params",
                {"Coordinator", TabletId},
                {"TenantPathId", TenantPathId},
                {"Path", msg->Path});
            return;
        }

        const auto& params = domainDescription.GetProcessingParams();
        if (params.GetVersion() < Version) {
            // Wait until the expected version is published
            YDB_LOG_WARN("ignoring update for path with processing params version, waiting for version",
                {"Coordinator", TabletId},
                {"TenantPathId", TenantPathId},
                {"Path", msg->Path},
                {"GetVersion", params.GetVersion()},
                {"Version", Version});
            return;
        }

        // Make sure params we found includes our own tablet
        bool found = false;
        for (ui64 coordinatorId : params.GetCoordinators()) {
            if (coordinatorId == TabletId) {
                found = true;
                break;
            }
        }

        if (!found) {
            // Ignore suspicious TenantPathId that points to some other subdomain
            YDB_LOG_WARN("ignoring suspicious update for path with processing params version that don't have in coordinators list",
                {"Coordinator", TabletId},
                {"TenantPathId", TenantPathId},
                {"Path", msg->Path},
                {"GetVersion", params.GetVersion()},
                {"TabletId", TabletId});
            return PassAway();
        }

        Send(Owner, new TEvPrivate::TEvRestoredProcessingParams(params));
        PassAway();
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr&) {
        // ignore
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::TPtr&) {
        // ignore
    }

private:
    const TActorId Owner;
    const ui64 TabletId;
    const TPathId TenantPathId;
    const ui64 Version;
};

bool TTxCoordinator::IsTabletInStaticDomain(TAppData* appData) {
    for (auto domainCoordinatorId: appData->DomainsInfo->GetDomain()->Coordinators) {
        if (TabletID() == domainCoordinatorId) {
            return true;
        }
    }

    return false;
}

void TTxCoordinator::RestoreProcessingParams(const TActorContext& ctx) {
    TAppData* appData = AppData(ctx);
    if (IsTabletInStaticDomain(appData)) {
        YDB_LOG_CTX_INFO(ctx, "restoring static processing params",
            {"Coordinator", TabletID()});
        DoConfiguration(*CreateDomainConfigurationFromStatic(appData), ctx);
        return;
    }

    if (RestoreProcessingParamsActor) {
        // Shouldn't really happen, but handle gracefully just in case
        ctx.Send(RestoreProcessingParamsActor, new TEvents::TEvPoison);
    }

    auto tenantPathId = Info()->TenantPathId;
    YDB_LOG_CTX_INFO(ctx, "resolving missing processing params version from tenant",
        {"Coordinator", TabletID()},
        {"Version", Config.Version},
        {"tenantPathId", tenantPathId});
    RestoreProcessingParamsActor = Register(new TRestoreProcessingParamsActor(SelfId(), TabletID(), Info()->TenantPathId, Config.Version));
}

void TTxCoordinator::Handle(TEvPrivate::TEvRestoredProcessingParams::TPtr& ev, const TActorContext& ctx) {
    if (ev->Sender != RestoreProcessingParamsActor) {
        // Wait for the latest update
        return;
    }

    // Note: restored params may be newer than those previously presisted
    auto& params = ev->Get()->Config;
    YDB_LOG_CTX_INFO(ctx, "applying discovered processing params version",
        {"Coordinator", TabletID()},
        {"GetVersion", params.GetVersion()});
    RestoreProcessingParamsActor = { };
    DoConfiguration(TEvSubDomain::TEvConfigure(std::move(params)), ctx);
}

} // namespace NKikimr::NFlatTxCoordinator
