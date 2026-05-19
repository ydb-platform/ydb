#include "console_tenants_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_TENANTS

namespace NKikimr::NConsole {

class TTenantsManager::TTxUpdateTenantState : public TTransactionBase<TTenantsManager> {
public:
    TTxUpdateTenantState(TTenantsManager *self,
                         const TString &path,
                         TTenant::EState state,
                         TActorId worker)
        : TBase(self)
        , Path(path)
        , State(state)
        , PrevState(state)
        , Worker(worker)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateTenantState for tenant to",
            {"Path", Path},
            {"State", State});

        Tenant = Self->GetTenant(Path);
        if (!Tenant) {
            YDB_LOG_CTX_ERROR(ctx, "TTxUpdateTenantState cannot find tenant",
                {"Path", Path});
            return true;
        }

        PrevState = Tenant->State;
        if (Tenant->State == State)
            return true;

        Self->DbUpdateTenantState(Tenant, State, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateTenantState complete for",
            {"Path", Path});

        if (Tenant && PrevState != State) {
            if (Tenant->Worker == Worker)
                Tenant->Worker = TActorId();
            Self->ChangeTenantState(Tenant, State, ctx);
            Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    TString Path;
    TTenant::EState State;
    TTenant::EState PrevState;
    TActorId Worker;
};

ITransaction *TTenantsManager::CreateTxUpdateTenantState(const TString &path,
                                                         TTenant::EState state,
                                                         TActorId worker)
{
    return new TTxUpdateTenantState(this, path, state, worker);
}

} // namespace NKikimr::NConsole
