#include "console_tenants_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_TENANTS

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
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateSubDomainKey for tenant",
            {"Path", Path},
            {"schemeshardid", SchemeShardId},
            {"pathid", PathId});

        Tenant = Self->GetTenant(Path);
        if (!Tenant) {
            YDB_LOG_CTX_ERROR(ctx, "TTxUpdateSubDomainKey cannot find tenant",
                {"Path", Path});
            return true;
        }

        // We are probably already removing this tenant.
        if (Tenant->IsRemoving()) {
            YDB_LOG_CTX_ERROR(ctx, "TTxUpdateSubDomainKey found tenant in wrong state",
                {"Path", Path},
                {"State", Tenant->State});
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateSubDomainKey complete for",
            {"Path", Path});

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
