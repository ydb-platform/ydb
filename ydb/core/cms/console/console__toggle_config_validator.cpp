#include "console_impl.h"
#include "console_configs_manager.h"
#include "console_tenants_manager.h"

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/cms/console/validators/registry.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

namespace NKikimr::NConsole {

class TConfigsManager::TTxToggleConfigValidator : public TTransactionBase<TConfigsManager> {
public:
    TTxToggleConfigValidator(TEvConsole::TEvToggleConfigValidatorRequest::TPtr ev,
                             TConfigsManager *self)
        : TBase(self)
        , Request(std::move(ev))
        , Modify(false)
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code, const TString &error,
               const TActorContext &ctx)
    {
        YDB_LOG_DEBUG_CTX(ctx, "Cannot toggle",
            {"validator", error});

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(error);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &rec = Request->Get()->Record;
        YDB_LOG_DEBUG_CTX(ctx, "Dump txToggleConfigValidator",
            {"txToggleConfigValidator", rec});

        Response = MakeHolder<TEvConsole::TEvToggleConfigValidatorResponse>();

        const TString &name = rec.GetName();
        bool disable = rec.GetDisable();
        auto registry = TValidatorsRegistry::Instance();

        if (!registry->GetValidator(name))
            return Error(Ydb::StatusIds::NOT_FOUND, "Unknown validator: " + rec.GetName(), ctx);

        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        if (disable) {
            if (Self->DisabledValidators.contains(name))
                return true;

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::DisabledValidators>().Key(rec.GetName()).Update();

            YDB_LOG_DEBUG_CTX(ctx, "Add disabled validator to local database",
                {"name", rec.GetName()});
        } else {
            if (!Self->DisabledValidators.contains(name))
                return true;

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::DisabledValidators>().Key(rec.GetName()).Delete();

            YDB_LOG_DEBUG_CTX(ctx, "Remove disabled validator from local database",
                {"name", rec.GetName()});
        }

        Modify = true;

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TConsole::TTxToggleConfigValidator Complete");

        if (Modify) {
            auto &rec = Request->Get()->Record;
            auto registry = TValidatorsRegistry::Instance();

            if (rec.GetDisable()) {
                registry->DisableValidator(rec.GetName());
                Self->DisabledValidators.insert(rec.GetName());

                YDB_LOG_DEBUG_CTX(ctx, "Disable validator",
                    {"request", rec.GetName()});
            } else {
                registry->EnableValidator(rec.GetName());
                Self->DisabledValidators.erase(rec.GetName());

                YDB_LOG_DEBUG_CTX(ctx, "Enable validator",
                    {"request", rec.GetName()});
            }
        }

        Y_ABORT_UNLESS(Response);
        YDB_LOG_TRACE_CTX(ctx, "Dump send",
            {"send", Response->ToString()});
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvToggleConfigValidatorRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvToggleConfigValidatorResponse> Response;
    bool Modify;
};

ITransaction *TConfigsManager::CreateTxToggleConfigValidator(TEvConsole::TEvToggleConfigValidatorRequest::TPtr &ev)
{
    return new TTxToggleConfigValidator(ev, this);
}

} // namespace NKikimr::NConsole
