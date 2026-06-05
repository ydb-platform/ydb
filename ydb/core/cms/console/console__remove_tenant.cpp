#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

using namespace NOperationId;

class TTenantsManager::TTxRemoveTenant : public TTransactionBase<TTenantsManager> {
public:
    TTxRemoveTenant(TEvConsole::TEvRemoveTenantRequest::TPtr ev, TTenantsManager *self)
        : TBase(self)
        , Path(CanonizePath(ev->Get()->Record.GetRequest().path()))
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code, const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "Cannot remove tenant: " << error);

        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(true);
        operation.set_status(code);
        auto issue = operation.add_issues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);

        Tenant = nullptr;

        return true;
    }

    void FillTenantResponse()
    {
        Y_ABORT_UNLESS(Tenant);
        Ydb::TOperationId id = Self->MakeOperationId(Tenant, TTenant::REMOVE);
        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(false);
        operation.set_id(ProtoToString(id));
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        Ydb::StatusIds::StatusCode code;
        TString error;

        auto &rec = Request->Get()->Record.GetRequest();
        auto &token = Request->Get()->Record.GetUserToken();
        auto &peer = Request->Get()->Record.GetPeerName();

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "TTxRemoveTenant: "
                    << Request->Get()->Record.ShortDebugString());

        Response = new TEvConsole::TEvRemoveTenantResponse;

        if (!Self->CheckAccess(token, code, error, ctx))
            return Error(code, error, ctx);

        auto path = CanonizePath(rec.path());
        Tenant = Self->GetTenant(path);
        if (!Tenant)
            return Error(Ydb::StatusIds::NOT_FOUND,
                         Sprintf("Database '%s' doesn't exist", path.data()), ctx);

        if (Tenant->IsRemoving()) {
            FillTenantResponse();
            // Set null to avoid removal actions trigger.
            Tenant = nullptr;
            return true;
        } else if (!Tenant->IsConfiguring() && !Tenant->IsRunning()) {
            return Error(Ydb::StatusIds::UNAVAILABLE,
                         Sprintf("Database '%s' is busy", path.data()), ctx);
        } else if (Tenant->HostedTenants) {
            return Error(Ydb::StatusIds::PRECONDITION_FAILED,
                         Sprintf("Database '%s' has serverless databases. Remove all of them first", path.data()), ctx);
        }

        Tenant->TxId = ctx.Now().GetValue();
        FillTenantResponse();

        Self->DbUpdateTenantState(Tenant, TTenant::REMOVING_SUBDOMAIN, txc, ctx);
        Self->DbUpdateTenantUserToken(Tenant, token, txc, ctx);
        Self->DbUpdateTenantPeerName(Tenant, peer, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "TTxRemoveTenant Complete");

        Y_ABORT_UNLESS(Response);
        if (Response->Record.GetResponse().operation().status())
            Self->Counters.Inc(Response->Record.GetResponse().operation().status(),
                               COUNTER_REMOVE_RESPONSES);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS, "Send: " << Response->ToString());
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);

        if (Tenant) {
            if (Tenant->IsCreating())
                Self->SendTenantNotifications(Tenant, TTenant::CREATE,
                                              Ydb::StatusIds::ABORTED, ctx);

            Self->ChangeTenantState(Tenant, TTenant::REMOVING_SUBDOMAIN, ctx);
            Tenant->UserToken = NACLib::TUserToken(Request->Get()->Record.GetUserToken());
            Tenant->Worker = TActorId();

            Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TString Path;
    TEvConsole::TEvRemoveTenantRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvRemoveTenantResponse> Response;
    TTenant::TPtr Tenant;
};

ITransaction *TTenantsManager::CreateTxRemoveTenant(TEvConsole::TEvRemoveTenantRequest::TPtr &ev)
{
    return new TTxRemoveTenant(ev, this);
}

} // namespace NKikimr::NConsole
