#include "console_configs_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

namespace NKikimr::NConsole {

class TConfigsManager::TTxLogCleanup : public TTransactionBase<TConfigsManager> {
public:
    TTxLogCleanup(TConfigsManager *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        YDB_LOG_CTX_DEBUG(ctx, "TTxLogCleanup Execute");

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
