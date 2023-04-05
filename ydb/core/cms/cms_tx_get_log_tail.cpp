#include "cms_impl.h"
#include "scheme.h"

namespace NKikimr::NCms {

using namespace NKikimrCms;

class TCms::TTxGetLogTail : public TTransactionBase<TCms> {
public:
    TTxGetLogTail(TCms *self,
                  TEvCms::TEvGetLogTailRequest::TPtr &ev)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    TTxType GetTxType() const override { return TXTYPE_GET_LOG_TAIL; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        auto &req = Request->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "TTxGetLogTail Execute " << req.ShortDebugString());

        TVector<NKikimrCms::TLogRecord> records;
        if (!Self->Logger.DbLoadLogTail(req.GetLogFilter(), records, txc))
            return false;

        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "TTxGetLogTail found " << records.size()
                    << " matching log records");

        Response = MakeHolder<TEvCms::TEvGetLogTailResponse>();
        auto &rec = Response->Record;
        rec.MutableStatus()->SetCode(TStatus::OK);
        for (auto it = records.rbegin(); it != records.rend(); ++it) {
            auto &entry = *rec.AddLogRecords();
            entry.Swap(&*it);
            if (req.GetTextFormat() != TEXT_FORMAT_NONE)
                entry.SetMessage(Self->Logger.GetLogMessage(entry, req.GetTextFormat()));
            if (!req.GetIncludeData())
                entry.ClearData();
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxGetLogTail Complete");

        ctx.Send(Request->Sender, Response.Release());
    }

private:
    TEvCms::TEvGetLogTailRequest::TPtr Request;
    THolder<TEvCms::TEvGetLogTailResponse> Response;
};

ITransaction *TCms::CreateTxGetLogTail(TEvCms::TEvGetLogTailRequest::TPtr &ev) {
    return new TTxGetLogTail(this, ev);
}

} // namespace NKikimr::NCms
