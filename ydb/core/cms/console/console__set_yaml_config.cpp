#include "console_configs_manager.h"
#include "console_configs_provider.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxSetYamlConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxSetYamlConfig(TConfigsManager *self,
                       TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &req = Request->Get()->Record;

        NIceDb::TNiceDb db(txc.DB);

        auto config = req.GetRequest().config();

        try {
            UpdatedConfig = NYamlConfig::ReplaceMetadata(config, NYamlConfig::TMetadata{
                    .Version = Self->YamlVersion + 1,
                    .Cluster = Self->ClusterName,
                });

            if (UpdatedConfig != Self->YamlConfig || Self->YamlDropped) {
                Modify = true;

                auto tree = NFyaml::TDocument::Parse(UpdatedConfig);
                auto resolved = NYamlConfig::ResolveAll(tree);

                for (auto& [_, config] : resolved.Configs) {
                    auto cfg = NYamlConfig::YamlToProto(config.second);
                }

                if (!req.GetRequest().dry_run()) {
                    db.Table<Schema::YamlConfig>().Key(Self->YamlVersion + 1)
                        .Update<Schema::YamlConfig::Config>(UpdatedConfig)
                        // set config dropped by default to support rollback to previous versions
                        // where new config layout is not supported
                        // it will lead to ignoring config from new versions
                        .Update<Schema::YamlConfig::Dropped>(true);

                    /* Later we shift this boundary to support rollback and history */
                    db.Table<Schema::YamlConfig>().Key(Self->YamlVersion)
                        .Delete();
                }
            }

            Response = MakeHolder<NActors::IEventHandle>(Request->Sender, ctx.SelfID, new TEvConsole::TEvSetYamlConfigResponse());
        } catch (const yexception& ex) {
            Error = true;

            auto ev = MakeHolder<TEvConsole::TEvGenericError>();
            ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
            auto *issue = ev->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(ex.what());
            Response = MakeHolder<NActors::IEventHandle>(Request->Sender, ctx.SelfID, ev.Release());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxSetYamlConfig Complete");

        auto &req = Request->Get()->Record;

        ctx.Send(Response.Release());

        if (!Error && Modify && !req.GetRequest().dry_run()) {
            Self->YamlVersion = Self->YamlVersion + 1;
            Self->YamlConfig = UpdatedConfig;
            Self->YamlDropped = false;

            Self->VolatileYamlConfigs.clear();

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(Self->YamlConfig);
            ctx.Send(Self->ConfigsProvider, resp.Release());
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvSetYamlConfigRequest::TPtr Request;
    THolder<NActors::IEventHandle> Response;
    bool Error = false;
    bool Modify = false;
    TString UpdatedConfig;
};

ITransaction *TConfigsManager::CreateTxSetYamlConfig(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
{
    return new TTxSetYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
