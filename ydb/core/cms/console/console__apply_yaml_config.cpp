#include "console_configs_manager.h"
#include "console_configs_provider.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxApplyYamlConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxApplyYamlConfig(TConfigsManager *self,
                       TEvConsole::TEvApplyConfigRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &) override
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

                db.Table<Schema::YamlConfig>().Key(Version + 1)
                    .Update<Schema::YamlConfig::Config>(UpdatedConfig)
                    .Update<Schema::YamlConfig::Dropped>(false);

                /* Later we shift this boundary to support rollback and history */
                db.Table<Schema::YamlConfig>().Key(Version)
                    .Delete();
            }

            Response = MakeHolder<TEvConsole::TEvApplyConfigResponse>();
            auto *op = Response->Record.MutableResponse()->mutable_operation();
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->set_ready(true);
        } catch (const yexception& ex) {
            Error = true;

            Response = MakeHolder<TEvConsole::TEvApplyConfigResponse>();
            auto *op = Response->Record.MutableResponse()->mutable_operation();
            op->set_status(Ydb::StatusIds::BAD_REQUEST);
            op->set_ready(true);
            auto *issue = op->add_issues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(ex.what());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxApplyYamlConfig Complete");

        ctx.Send(Request->Sender, Response.Release());

        if (!Error && Modify) {
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
    TEvConsole::TEvApplyConfigRequest::TPtr Request;
    THolder<TEvConsole::TEvApplyConfigResponse> Response;
    bool Error = false;
    bool Modify = false;
    ui32 Version;
    TString Cluster;
    TString UpdatedConfig;
};

ITransaction *TConfigsManager::CreateTxApplyYamlConfig(TEvConsole::TEvApplyConfigRequest::TPtr &ev)
{
    return new TTxApplyYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
