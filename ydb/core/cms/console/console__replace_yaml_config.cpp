#include "console_configs_manager.h"
#include "console_configs_provider.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxReplaceYamlConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxReplaceYamlConfig(TConfigsManager *self,
                       TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
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
            auto metadata = NYamlConfig::GetMetadata(config);
            Cluster = metadata.Cluster.value_or(TString("unknown"));
            Version = metadata.Version.value_or(0);
            UpdatedConfig = NYamlConfig::ReplaceMetadata(config, NYamlConfig::TMetadata{
                    .Version = Version + 1,
                    .Cluster = Cluster,
                });

            if (UpdatedConfig != Self->YamlConfig || Self->YamlDropped) {
                Modify = true;

                auto tree = NFyaml::TDocument::Parse(UpdatedConfig);
                auto resolved = NYamlConfig::ResolveAll(tree);

                if (Self->ClusterName != Cluster) {
                    ythrow yexception() << "ClusterName mismatch";
                }

                if (Version != Self->YamlVersion) {
                    ythrow yexception() << "Version mismatch";
                }

                for (auto& [_, config] : resolved.Configs) {
                    auto cfg = NYamlConfig::YamlToProto(config.second);
                }

                if (!req.GetRequest().dry_run()) {
                    db.Table<Schema::YamlConfig>().Key(Version + 1)
                        .Update<Schema::YamlConfig::Config>(UpdatedConfig)
                        // set config dropped by default to support rollback to previous versions
                        // where new config layout is not supported
                        // it will lead to ignoring config from new versions
                        .Update<Schema::YamlConfig::Dropped>(true);

                    /* Later we shift this boundary to support rollback and history */
                    db.Table<Schema::YamlConfig>().Key(Version)
                        .Delete();
                }
            }

            Response = MakeHolder<NActors::IEventHandle>(Request->Sender, ctx.SelfID, new TEvConsole::TEvReplaceYamlConfigResponse());
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
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxReplaceYamlConfig Complete");

        auto &req = Request->Get()->Record;

        ctx.Send(Response.Release());

        if (!Error && Modify && !req.GetRequest().dry_run()) {
            Self->YamlVersion = Version + 1;
            Self->YamlConfig = UpdatedConfig;
            Self->YamlDropped = false;

            Self->VolatileYamlConfigs.clear();

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(Self->YamlConfig);
            ctx.Send(Self->ConfigsProvider, resp.Release());
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvReplaceYamlConfigRequest::TPtr Request;
    THolder<NActors::IEventHandle> Response;
    bool Error = false;
    bool Modify = false;
    ui32 Version;
    TString Cluster;
    TString UpdatedConfig;
};

ITransaction *TConfigsManager::CreateTxReplaceYamlConfig(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
