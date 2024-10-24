#include "console_configs_manager.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxGetYamlConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxGetYamlConfig(TConfigsManager *self,
                     NEvConsole::TEvGetAllConfigsRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &, const TActorContext &) override
    {
        Response = MakeHolder<NEvConsole::TEvGetAllConfigsResponse>();

        Response->Record.MutableResponse()->mutable_identity()->set_cluster(Self->ClusterName);
        Response->Record.MutableResponse()->mutable_identity()->set_version(Self->YamlVersion);
        Response->Record.MutableResponse()->set_config(Self->YamlConfig);

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
    NEvConsole::TEvGetAllConfigsRequest::TPtr Request;
    THolder<NEvConsole::TEvGetAllConfigsResponse> Response;
};

ITransaction *TConfigsManager::CreateTxGetYamlConfig(NEvConsole::TEvGetAllConfigsRequest::TPtr &ev)
{
    return new TTxGetYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
