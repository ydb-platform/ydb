#include "console_tenants_manager.h"
#include "console_impl.h"

namespace NKikimr::NConsole {

class TTenantsManager::TTxUpdateTenantPoolConfig : public TTransactionBase<TTenantsManager> {
public:
    TTxUpdateTenantPoolConfig(TEvConsole::TEvUpdateTenantPoolConfig::TPtr ev, TTenantsManager *self)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code, const TString &error, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "Cannot update tenant pool config: " << error);

        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_status(code);
        auto issue = operation.add_issues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        Ydb::StatusIds::StatusCode code;
        TString error;

        auto &rec = Request->Get()->Record;
        auto &token = rec.GetUserToken();
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "TTxUpdateTenantPoolConfig: "
                    << rec.ShortDebugString());

        Response.Reset(new TEvConsole::TEvGetTenantStatusResponse);
        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(true);

        if (!Self->CheckAccess(token, code, error, ctx))
            return Error(code, error, ctx);

        auto path = CanonizePath(rec.GetTenant());
        auto tenant = Self->GetTenant(path);
        if (!tenant)
            return Error(Ydb::StatusIds::NOT_FOUND,
                         Sprintf("Database '%s' doesn't exist", path.data()), ctx);

        if (!tenant->IsRunning() && !tenant->IsConfiguring())
            return Error(Ydb::StatusIds::UNAVAILABLE,
                         Sprintf("Database '%s' is busy", path.data()), ctx);

        auto kind = rec.GetPoolType();
        auto pool = tenant->StoragePools.find(kind);
        if (pool == tenant->StoragePools.end())
            return Error(Ydb::StatusIds::NOT_FOUND,
                         Sprintf("Pool of kind '%s' doesn't exist", kind.data()), ctx);

        auto &config = *rec.MutableConfig();
        pool->second->AllocatedNumGroups = config.GetNumGroups();
        pool->second->Config.Swap(&config);
        Self->DbUpdatePoolConfig(tenant, pool->second, pool->second->Config, txc, ctx);

        Ydb::Cms::GetDatabaseStatusResult result;
        Self->FillTenantStatus(tenant, result);
        operation.set_status(Ydb::StatusIds::SUCCESS);
        operation.mutable_result()->PackFrom(result);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "TTxUpdateTenantPoolConfig Complete");

        Y_ABORT_UNLESS(Response);
        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS, "Send: " << Response->ToString());
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvUpdateTenantPoolConfig::TPtr Request;
    THolder<TEvConsole::TEvGetTenantStatusResponse> Response;
};

ITransaction *TTenantsManager::CreateTxUpdateTenantPoolConfig(TEvConsole::TEvUpdateTenantPoolConfig::TPtr &ev)
{
    return new TTxUpdateTenantPoolConfig(ev, this);
}

} // namespace NKikimr::NConsole
