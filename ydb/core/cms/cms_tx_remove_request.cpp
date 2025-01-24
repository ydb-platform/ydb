#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxRemoveRequest : public TTransactionBase<TCms> {
public:
    TTxRemoveRequest(TCms *self, const TString &id, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp)
        : TBase(self)
        , Request(std::move(req))
        , Response(resp)
        , Id(id)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_REQUEST; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemoveRequest Execute");

        auto it = Self->State->ScheduledRequests.find(Id);
        if (it != Self->State->ScheduledRequests.end()) {
            auto& request = it->second.Request;
            if (request.GetEvictVDisks()) {
                for (const auto &action : it->second.Request.GetActions()) {
                    auto ret = Self->ResetHostMarkers(action.GetHost(), txc, ctx);
                    std::move(ret.begin(), ret.end(), std::back_inserter(HostUpdateMarkers));
                }
            }

            if (request.GetDecomissionPDisk()) {
                for (const auto &action : it->second.Request.GetActions()) {
                    for (auto &device : action.GetDevices()) {
                        auto ret = Self->ResetPDiskMarkers(TPDiskInfo::NameToId(device), txc, ctx);
                        std::move(ret.begin(), ret.end(), std::back_inserter(PDiskUpdateMarkers));
                    }
                }
            }
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Request>().Key(Id).Delete();
        Self->State->ScheduledRequests.erase(Id);

        Self->AuditLog(ctx, TStringBuilder() << "Remove request"
            << ": id# " << Id
            << ", reason# " << (Request ? "explicit remove" : "scheduled cleanup"));

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemoveRequest Complete");

        if (Response) {
            Y_ABORT_UNLESS(Request);
            Self->Reply(Request.Get(), Response, ctx);
        }

        Self->RemoveEmptyTasks(ctx);
        Self->SentinelUpdateMarkers(std::move(HostUpdateMarkers), std::move(PDiskUpdateMarkers), ctx);
    }

private:
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
    TString Id;
    TVector<TEvSentinel::TEvUpdateMarkers::THostMarkers> HostUpdateMarkers;
    TVector<TEvSentinel::TEvUpdateMarkers::TPDiskMarkers> PDiskUpdateMarkers;
};

ITransaction *TCms::CreateTxRemoveRequest(const TString &id, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp) {
    return new TTxRemoveRequest(this, id, std::move(req), std::move(resp));
}

} // NKikimr::NCms
