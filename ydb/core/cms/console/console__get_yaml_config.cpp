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
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &) override
    {
        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::YamlConfig>()
            .Reverse()
            .Select<Schema::YamlConfig::TColumns>();

        if (!rowset.IsReady())
            return false;

        Response = MakeHolder<TEvConsole::TEvGetAllConfigsResponse>();
        auto *op = Response->Record.MutableResponse()->mutable_operation();
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->set_ready(true);

        Response->Record.MutableResponse()->set_cluster(Self->ClusterName);
        Response->Record.MutableResponse()->set_version(Self->YamlVersion);

        if (!rowset.EndOfSet()) {
            bool dropped = rowset.template GetValue<Schema::YamlConfig::Dropped>();
            if (!dropped) {
                auto config = rowset.template GetValue<Schema::YamlConfig::Config>();
                Response->Record.MutableResponse()->set_config(config);

                for (auto &[id, cfg] : Self->VolatileYamlConfigs) {
                    auto *config = Response->Record.MutableResponse()->add_volatile_configs();
                    config->set_id(id);
                    config->set_config(cfg);
                }
            }
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
};

ITransaction *TConfigsManager::CreateTxGetYamlConfig(TEvConsole::TEvGetAllConfigsRequest::TPtr &ev)
{
    return new TTxGetYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
