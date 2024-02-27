#include "console_configs_manager.h"
#include "console_configs_provider.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

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

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &req = Request->Get()->Record;

        NIceDb::TNiceDb db(txc.DB);

        try {
            Version = req.GetRequest().identity().version();
            auto cluster = req.GetRequest().identity().cluster();

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
            Error = true;

            auto ev = MakeHolder<TEvConsole::TEvGenericError>();
            ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
            auto *issue = ev->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(ex.what());
            Response = MakeHolder<NActors::IEventHandle>(Request->Sender, ctx.SelfID, ev.Release());
            return true;
        }

        if (!Self->YamlDropped) {
            Modify = true;

            db.Table<Schema::YamlConfig>().Key(Version).Delete();
        }

        Response = MakeHolder<NActors::IEventHandle>(Request->Sender, ctx.SelfID, new TEvConsole::TEvDropConfigResponse());

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxDropYamlConfig Complete");

        ctx.Send(Response.Release());

        if (!Error && Modify) {
            Self->YamlVersion = 0;
            Self->YamlConfig.clear();
            Self->YamlDropped = true;

            Self->VolatileYamlConfigs.clear();

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>("");
            ctx.Send(Self->ConfigsProvider, resp.Release());
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvDropConfigRequest::TPtr Request;
    THolder<NActors::IEventHandle> Response;
    bool Error = false;
    bool Modify = false;
    ui32 Version;
};

ITransaction *TConfigsManager::CreateTxDropYamlConfig(TEvConsole::TEvDropConfigRequest::TPtr &ev)
{
    return new TTxDropYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
