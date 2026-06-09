#include "console_configs_manager.h"

namespace NKikimr::NConsole {

class TConfigsManager::TTxLogCleanup : public TTransactionBase<TConfigsManager> {
public:
    TTxLogCleanup(TConfigsManager *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTxLogCleanup Execute");

        const ui32 maxConsoleLogEntries = 25000;

        return Self->Logger.DbCleanupLog(maxConsoleLogEntries, txc, ctx);
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS,
                  "TTxLogCleanup Complete");

        Self->ScheduleLogCleanup(ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }
};

ITransaction *TConfigsManager::CreateTxLogCleanup()
{
    return new TTxLogCleanup(this);
}

} // NKikimr::NConsole
