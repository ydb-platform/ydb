#include "tablet_impl.h"

#include <util/string/builder.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxQuoterResourceDescribe : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvDescribeQuoterResources Record;

    THolder<TEvKesus::TEvDescribeQuoterResourcesResult> Reply;

    TTxQuoterResourceDescribe(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvDescribeQuoterResources& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
        , Reply(MakeHolder<TEvKesus::TEvDescribeQuoterResourcesResult>())
    {
        Reply->Record.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
    }

    TTxType GetTxType() const override { return TXTYPE_QUOTER_RESOURCE_DESCRIBE; }

    template <class TRepeatedField>
    static TString FormatResourcesToDescribe(const TRepeatedField& list) {
        TStringBuilder ret;
        ret << "[";
        for (int i = 0; i < list.size(); ++i) {
            if (i > 0) {
                ret << ", ";
            }
            ret << list.Get(i);
        }
        ret << "]";
        return std::move(ret);
    }

    TString FormatIdsToDescribe() const {
        return FormatResourcesToDescribe(Record.GetResourceIds());
    }

    TString FormatPathsToDescribe() const {
        return FormatResourcesToDescribe(Record.GetResourcePaths());
    }

    void AddToResult(const TQuoterResourceTree* resource) {
        Y_ABORT_UNLESS(resource);
        *Reply->Record.AddResources() = resource->GetProps();
    }

    bool NeedToDescribeAll() const {
        return Record.ResourcePathsSize() == 0 && Record.ResourceIdsSize() == 0;
    }

    void WalkResource(const TQuoterResourceTree* resource) {
        AddToResult(resource);
        for (const TQuoterResourceTree* child : resource->GetChildren()) {
            WalkResource(child);
        }
    }

    void ProcessResource(const TQuoterResourceTree* resource) {
        if (Record.GetRecursive()) {
            WalkResource(resource);
        } else {
            AddToResult(resource);
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceDescribe::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", ids=" << FormatIdsToDescribe()
                << ", paths=" << FormatPathsToDescribe() << ", recursive=" << Record.GetRecursive() << ")");

        if (NeedToDescribeAll()) {
            for (auto&& [path, resource] : Self->QuoterResources.GetAllResources()) {
                if (Record.GetRecursive() || resource->GetParentId() == 0) {
                    AddToResult(resource);
                }
            }
        } else {
            for (ui64 id : Record.GetResourceIds()) {
                const TQuoterResourceTree* resource = Self->QuoterResources.FindId(id);
                if (!resource) {
                    Reply = MakeHolder<TEvKesus::TEvDescribeQuoterResourcesResult>(
                        Ydb::StatusIds::NOT_FOUND,
                        TStringBuilder() << "Resource with id " << id << " doesn't exist.");
                    return true;
                }
                ProcessResource(resource);
            }
            for (const TString& path : Record.GetResourcePaths()) {
                const TQuoterResourceTree* resource = Self->QuoterResources.FindPath(path);
                if (!resource) {
                    Reply = MakeHolder<TEvKesus::TEvDescribeQuoterResourcesResult>(
                        Ydb::StatusIds::NOT_FOUND,
                        TStringBuilder() << "Resource with path \"" << path << "\" doesn't exist.");
                    return true;
                }
                ProcessResource(resource);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceDescribe::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, std::move(Reply), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDescribeQuoterResources::TPtr& ev) {
    Execute(new TTxQuoterResourceDescribe(this, ev->Sender, ev->Cookie, ev->Get()->Record), TActivationContext::AsActorContext());
}

}
}
