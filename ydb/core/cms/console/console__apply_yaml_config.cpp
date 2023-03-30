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

        if (config != Self->YamlConfig) {
            Modify = true;

            try {
                auto parser = NFyaml::TParser::Create(config.Detach());
                auto header = parser.NextDocument();
                auto cluster = header->Root().Map()["cluster"].Scalar();
                Version = TryFromString<ui64>(header->Root().Map()["version"].Scalar())
                    .GetOrElse(0);

                if (Version == 0) {
                    ythrow yexception() << "Invalid version";
                }

                auto tree = parser.NextDocument();
                auto resolved = NYamlConfig::ResolveAll(*tree);

                if (Self->ClusterName != cluster) {
                    ythrow yexception() << "ClusterName mismatch";
                }

                if (Version != Self->YamlVersion + 1) {
                    ythrow yexception() << "Version mismatch";
                }

                for (auto& [_, config] : resolved.Configs) {
                    auto cfg = NYamlConfig::YamlToProto(config.second);
                }
            } catch (const yexception& ex) {
                Response = MakeHolder<TEvConsole::TEvApplyConfigResponse>();
                auto *op = Response->Record.MutableResponse()->mutable_operation();
                op->set_status(Ydb::StatusIds::BAD_REQUEST);
                op->set_ready(true);
                auto *issue = op->add_issues();
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(ex.what());
                Error = true;
                return true;
            }

            db.Table<Schema::YamlConfig>().Key(Version)
                .Update<Schema::YamlConfig::Config>(config);

            /* Later we shift this boundary to support rollback and history */
            db.Table<Schema::YamlConfig>().Key(Version - 1)
                .Delete();
        }

        Response = MakeHolder<TEvConsole::TEvApplyConfigResponse>();
        auto *op = Response->Record.MutableResponse()->mutable_operation();
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->set_ready(true);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxApplyYamlConfig Complete");

        auto &req = Request->Get()->Record;

        ctx.Send(Request->Sender, Response.Release());

        if (!Error && Modify) {
            Self->YamlVersion = Version;
            Self->YamlConfig = req.GetRequest().config();

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
};

ITransaction *TConfigsManager::CreateTxApplyYamlConfig(TEvConsole::TEvApplyConfigRequest::TPtr &ev)
{
    return new TTxApplyYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
