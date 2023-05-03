#include "console_configs_manager.h"
#include "console_configs_provider.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxDropYamlConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxDropYamlConfig(TConfigsManager *self,
                       TEvConsole::TEvDropConfigRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &) override
    {
        auto &req = Request->Get()->Record;

        NIceDb::TNiceDb db(txc.DB);

        Y_UNUSED(Modify);
        Y_UNUSED(Error);

        try {
            Version = req.GetRequest().version();
            auto cluster = req.GetRequest().cluster();

            if (Version == 0) {
                ythrow yexception() << "Invalid version";
            }

            if (Self->ClusterName != cluster) {
                ythrow yexception() << "ClusterName mismatch";
            }

            if (Version != Self->YamlVersion) {
                ythrow yexception() << "Version mismatch";
            }
        } catch (const yexception& ex) {
            Response = MakeHolder<TEvConsole::TEvDropConfigResponse>();
            auto *op = Response->Record.MutableResponse()->mutable_operation();
            op->set_status(Ydb::StatusIds::BAD_REQUEST);
            op->set_ready(true);
            auto *issue = op->add_issues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(ex.what());
            Error = true;
            return true;
        }

        if (!Self->YamlDropped) {
            Modify = true;

            db.Table<Schema::YamlConfig>().Key(Version)
                .Update<Schema::YamlConfig::Dropped>(true);
        }

        Response = MakeHolder<TEvConsole::TEvDropConfigResponse>();
        auto *op = Response->Record.MutableResponse()->mutable_operation();
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->set_ready(true);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxDropYamlConfig Complete");

        ctx.Send(Request->Sender, Response.Release());

        if (!Error && Modify) {
            Self->YamlDropped = true;

            Self->VolatileYamlConfigs.clear();

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>("");
            ctx.Send(Self->ConfigsProvider, resp.Release());
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvDropConfigRequest::TPtr Request;
    THolder<TEvConsole::TEvDropConfigResponse> Response;
    bool Error = false;
    bool Modify = false;
    ui32 Version;
};

ITransaction *TConfigsManager::CreateTxDropYamlConfig(TEvConsole::TEvDropConfigRequest::TPtr &ev)
{
    return new TTxDropYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
