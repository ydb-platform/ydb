#include "console_configs_manager.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxGetYamlMetadata : public TTransactionBase<TConfigsManager> {
public:
    TTxGetYamlMetadata(TConfigsManager *self,
                       TEvConsole::TEvGetAllMetadataRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &, const TActorContext &) override
    {
        Response = MakeHolder<TEvConsole::TEvGetAllMetadataResponse>();

        if (Self->YamlConfig) {
            auto doc = NFyaml::TDocument::Parse(Self->YamlConfig);

            TStringStream metadata;
            metadata << doc.Root().Map().at("metadata");

            Response->Record.MutableResponse()->set_metadata(metadata.Str());

            for (auto &[id, cfg] : Self->VolatileYamlConfigs) {
                auto *config = Response->Record.MutableResponse()->add_volatile_configs();
                metadata.clear();
                auto doc = NFyaml::TDocument::Parse(cfg);
                metadata << doc.Root().Map().at("metadata");
                config->set_id(id);
                config->set_metadata(metadata.Str());
            }

        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxGetYamlMetadata Complete");

        ctx.Send(Request->Sender, Response.Release());

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvGetAllMetadataRequest::TPtr Request;
    THolder<TEvConsole::TEvGetAllMetadataResponse> Response;
};

ITransaction *TConfigsManager::CreateTxGetYamlMetadata(TEvConsole::TEvGetAllMetadataRequest::TPtr &ev)
{
    return new TTxGetYamlMetadata(this, ev);
}

} // namespace NKikimr::NConsole
