#include "console_configs_manager.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxGetYamlConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxGetYamlConfig(TConfigsManager *self,
                     TEvConsole::TEvGetAllConfigsRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
        , IngressDatabase(Request->Get()->Record.HasIngressDatabase() ? TMaybe<TString>{Request->Get()->Record.GetIngressDatabase()} : TMaybe<TString>{})
    {
    }

    bool Execute(TTransactionContext &, const TActorContext &) override
    {
        Response = MakeHolder<TEvConsole::TEvGetAllConfigsResponse>();

        if (IngressDatabase
            // treat root (domain) database as cluster for backward compatibility
            && *IngressDatabase != Self->DomainName && *IngressDatabase != ("/" + Self->DomainName))
        {
            if (Self->DatabaseYamlConfigs.contains(*IngressDatabase)) {
                auto& identity = *Response->Record.MutableResponse()->add_identity();
                identity.set_database(*IngressDatabase);
                identity.set_version(Self->DatabaseYamlConfigs[*IngressDatabase].Version);
                Response->Record.MutableResponse()->add_config(Self->DatabaseYamlConfigs[*IngressDatabase].Config);
            }

            return true;
        }

        auto& identity = *Response->Record.MutableResponse()->add_identity();
        identity.set_cluster(Self->ClusterName);
        identity.set_version(Self->YamlVersion);
        Response->Record.MutableResponse()->add_config(Self->MainYamlConfig);

        for (const auto& [database, config] : Self->DatabaseYamlConfigs) {
            Response->Record.MutableResponse()->add_identity()->set_database(database);
            Response->Record.MutableResponse()->add_identity()->set_version(config.Version);
            Response->Record.MutableResponse()->add_config(config.Config);
        }

        for (auto &[id, cfg] : Self->VolatileYamlConfigs) {
            auto *config = Response->Record.MutableResponse()->add_volatile_configs();
            config->set_id(id);
            config->set_config(cfg);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxGetYamlConfig Complete");

        ctx.Send(Request->Sender, Response.Release());

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvGetAllConfigsRequest::TPtr Request;
    THolder<TEvConsole::TEvGetAllConfigsResponse> Response;
    TMaybe<TString> IngressDatabase;
};

ITransaction *TConfigsManager::CreateTxGetYamlConfig(TEvConsole::TEvGetAllConfigsRequest::TPtr &ev)
{
    return new TTxGetYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
