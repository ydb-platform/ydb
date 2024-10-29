#include "console_configs_manager.h"

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxGetLogTail : public TTransactionBase<TConfigsManager> {
public:
    TTxGetLogTail(TConfigsManager *self,
                  NEvConsole::TEvGetLogTailRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &req = Request->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTxGetLogTail Execute " << req.ShortDebugString());

        TVector<NKikimrConsole::TLogRecord> records;
        if (!Self->Logger.DbLoadLogTail(req.GetLogFilter(), records, txc))
            return false;

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTxGetLogTail found " << records.size()
                    << " matching log records");

        Response = MakeHolder<NEvConsole::TEvGetLogTailResponse>();
        auto &rec = Response->Record;
        rec.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        for (auto it = records.rbegin(); it != records.rend(); ++it) {
            auto &entry = *rec.AddLogRecords();
            entry.Swap(&*it);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxGetLogTail Complete");

        ctx.Send(Request->Sender, Response.Release());

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    NEvConsole::TEvGetLogTailRequest::TPtr Request;
    THolder<NEvConsole::TEvGetLogTailResponse> Response;
};

ITransaction *TConfigsManager::CreateTxGetLogTail(NEvConsole::TEvGetLogTailRequest::TPtr &ev)
{
    return new TTxGetLogTail(this, ev);
}

} // namespace NKikimr::NConsole
