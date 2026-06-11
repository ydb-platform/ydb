#include "console_configs_manager.h"
#include "console_impl.h"
#include "console_tenants_manager.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NConsole {

class TConsole::TTxLoadState : public TTransactionBase<TConsole> {
public:
    TTxLoadState(TConsole *self)
        : TBase(self)
    {
    }

    template <typename T>
    bool IsReady(T &t)
    {
        return t.IsReady();
    }

    template <typename T, typename ...Ts>
    bool IsReady(T &t, Ts &...args)
    {
        return t.IsReady() && IsReady(args...);
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TConsole::TTxLoadState Execute");

        NIceDb::TNiceDb db(txc.DB);
        auto configRow = db.Table<Schema::Config>().Key(ConfigKeyConfig).Select<Schema::Config::Value>();

        if (!db.Precharge<Schema>())
            return false;

        Self->ClearState();

        if (!configRow.IsReady())
            return false;

        if (configRow.IsValid()) {
            auto configString = configRow.GetValue<Schema::Config::Value>();
            NKikimrConsole::TConfig config;
            Y_PROTOBUF_SUPPRESS_NODISCARD config.ParseFromArray(configString.data(), configString.size());
            Self->LoadConfigFromProto(config);

            YDB_LOG_DEBUG_CTX(ctx, "Loaded",
                {"config", config.DebugString()});
        } else {
            YDB_LOG_DEBUG_CTX(ctx, "Using default config");

            Self->LoadConfigFromProto(NKikimrConsole::TConfig());
        }

        if (!Self->TenantsManager->DbLoadState(txc, ctx))
            return false;

        if (!Self->ConfigsManager->DbLoadState(txc, ctx))
            return false;

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TConsole::TTxLoadState Complete");

        Self->Become(&TConsole::StateWork);
        Self->SignalTabletActive(ctx);

        ctx.Send(Self->TenantsManager->SelfId(), new TTenantsManager::TEvPrivate::TEvStateLoaded);
        ctx.Send(Self->ConfigsManager->SelfId(), new TConfigsManager::TEvPrivate::TEvStateLoaded);
        Self->ProcessEnqueuedEvents(ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
};

ITransaction *TConsole::CreateTxLoadState()
{
    return new TTxLoadState(this);
}

} // namespace NKikimr::NConsole
