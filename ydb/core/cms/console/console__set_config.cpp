#include "console_impl.h"
#include "console_configs_manager.h"
#include "console_tenants_manager.h"

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/util/config_index.h>

namespace NKikimr::NConsole {

class TConsole::TTxSetConfig : public TTransactionBase<TConsole> {
public:
    TTxSetConfig(TEvConsole::TEvSetConfigRequest::TPtr ev, TConsole *self)
        : TBase(self)
        , Request(std::move(ev))
        , ModifyConfig(false)
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code, const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS, "Cannot set config: " << error);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(error);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &rec = Request->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::CMS, "TConsole::TTxSetConfig: " << rec.ShortDebugString());

        Response = new TEvConsole::TEvSetConfigResponse;

        if (rec.GetMerge() == NKikimrConsole::TConfigItem::OVERWRITE) {
            NewConfig = rec.GetConfig();
        } else if (rec.GetMerge() == NKikimrConsole::TConfigItem::MERGE) {
            NewConfig = Self->Config;
            NewConfig.MergeFrom(rec.GetConfig());
        } else if (rec.GetMerge() == NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED) {
            NewConfig = Self->Config;
            MergeMessageOverwriteRepeated(NewConfig, *rec.MutableConfig());
        } else {
            return Error(Ydb::StatusIds::BAD_REQUEST,
                         "Unsupported merge strategy", ctx);
        }

        Ydb::StatusIds::StatusCode code;
        TString error;
        if (!Self->TenantsManager->CheckTenantsConfig(NewConfig.GetTenantsConfig(), code, error))
            return Error(code, error, ctx);
        if (!Self->ConfigsManager->CheckConfig(NewConfig.GetConfigsConfig(), code, error))
            return Error(code, error, ctx);

        ModifyConfig = true;

        // Modify state.
        NIceDb::TNiceDb db(txc.DB);
        TString config;
        Y_PROTOBUF_SUPPRESS_NODISCARD NewConfig.SerializeToString(&config);
        db.Table<Schema::Config>().Key(ConfigKeyConfig)
            .Update(NIceDb::TUpdate<Schema::Config::Value>(config));

        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TConsole::TTxSetConfig Complete");

        if (ModifyConfig)
            Self->LoadConfigFromProto(NewConfig);

        Y_ABORT_UNLESS(Response);
        LOG_TRACE_S(ctx, NKikimrServices::CMS, "Send: " << Response->ToString());
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvSetConfigRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvSetConfigResponse> Response;
    NKikimrConsole::TConfig NewConfig;
    bool ModifyConfig;
};

ITransaction *TConsole::CreateTxSetConfig(TEvConsole::TEvSetConfigRequest::TPtr &ev)
{
    return new TTxSetConfig(ev, this);
}

} // namespace NKikimr::NConsole
