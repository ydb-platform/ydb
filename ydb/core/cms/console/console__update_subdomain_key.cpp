#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

class TTenantsManager::TTxUpdateSubDomainKey : public TTransactionBase<TTenantsManager> {
public:
    TTxUpdateSubDomainKey(TTenantsManager *self,
                        const TString &path,
                        ui64 schemeShardId,
                        ui64 pathId,
                        TActorId worker)
        : TBase(self)
        , Path(path)
        , SchemeShardId(schemeShardId)
        , PathId(pathId)
        , Worker(worker)
    {
        Y_ABORT_UNLESS(SchemeShardId && PathId);
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxUpdateSubDomainKey for tenant " << Path
                    << " schemeshardid=" << SchemeShardId
                    << " pathid=" << PathId);

        Tenant = Self->GetTenant(Path);
        if (!Tenant) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxUpdateSubDomainKey cannot find tenant " << Path);
            return true;
        }

        // We are probably already removing this tenant.
        if (Tenant->IsRemoving()) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxUpdateSubDomainKey found tenant " << Path
                        << " in wrong state " << Tenant->State);
            Tenant = nullptr;
            return true;
        }

        Self->DbUpdateTenantSubdomain(Tenant, SchemeShardId, PathId, txc, ctx);
        if (Tenant->State == TTenant::CREATING_SUBDOMAIN)
            Self->DbUpdateTenantState(Tenant, TTenant::CONFIGURING_SUBDOMAIN, txc, ctx);

        for (auto &pr : Tenant->StoragePools) {
            auto config = pr.second->Config;
            TStoragePool::SetScopeId(config, SchemeShardId, PathId);
            Self->DbUpdatePoolConfig(Tenant, pr.second, config, txc, ctx);

            if (pr.second->State == TStoragePool::ALLOCATED)
                Self->DbUpdatePoolState(Tenant, pr.second, TStoragePool::NOT_UPDATED, txc, ctx);
        }

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxUpdateSubDomainKey complete for " << Path);

        if (Tenant) {
            if (Tenant->DomainId) {
                Self->TenantIdToName.erase(Tenant->DomainId);
            }

            Tenant->DomainId = TDomainId(SchemeShardId, PathId);

            Y_ABORT_UNLESS(!Self->TenantIdToName.contains(Tenant->DomainId));
            Self->TenantIdToName[Tenant->DomainId] = Tenant->Path;

            if (Tenant->Worker == Worker)
                Tenant->Worker = TActorId();

            for (auto &pr : Tenant->StoragePools) {
                pr.second->SetScopeId(SchemeShardId, PathId);
                if (pr.second->State == TStoragePool::ALLOCATED)
                    pr.second->State = TStoragePool::NOT_UPDATED;
            }

            if (Tenant->State == TTenant::CREATING_SUBDOMAIN)
                Self->ChangeTenantState(Tenant, TTenant::CONFIGURING_SUBDOMAIN, ctx);

            Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    TString Path;
    ui64 SchemeShardId;
    ui64 PathId;
    TActorId Worker;
};

ITransaction *TTenantsManager::CreateTxUpdateSubDomainKey(const TString &path,
                                                        ui64 schemeShardId,
                                                        ui64 pathId,
                                                        TActorId worker)
{
    return new TTxUpdateSubDomainKey(this, path, schemeShardId, pathId, worker);
}

} // namespace NKikimr::NConsole
