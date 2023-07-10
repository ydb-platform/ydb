#include "action.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/string/ascii.h>
#include <util/string/vector.h>
#include <util/string/cast.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

class TListUsersActor
    : public TActionActor<TListUsersActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TListUsersActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ListUsers, std::move(cb))
    {
    }

private:
    void PassAway() override {
        TActionActor<TListUsersActor>::PassAway();
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableListUsers()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        auto proxy = MakeTxProxyID();

        auto ev = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        ev->Record.MutableDescribePath()->SetPath(Cfg().GetRoot());

        RLOG_SQS_TRACE("TListUsersActor generate request."
                       << ". Proxy actor: " << proxy
                       << ". TEvNavigate: " << ev->Record.ShortDebugString());

        Send(proxy, ev.Release());
    }

    TString DoGetQueueName() const override {
        return TString();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleDescribeSchemeResult);
        }
    }

    void HandleDescribeSchemeResult(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        const auto& desc = record.GetPathDescription();

        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            RLOG_SQS_CRIT("No error handler at TListUsersActor in HandleDescribeSchemeResult"
                               << ", got msg: " << record.ShortDebugString());
            // status, reason might be useful.
            // The request doesn't have to be seccessfull all the time. StatusNotAvailable for example could occur
        }

        const TString prefix = Request().GetUserNamePrefix();

        for (const auto& child : desc.children()) {
            if (child.GetPathType() == NKikimrSchemeOp::EPathTypeDir) {
                if (prefix.empty() || AsciiHasPrefix(child.GetName(), prefix)) {
                    Response_.MutableListUsers()->AddUserNames(child.GetName());
                }
            }
        }

        SendReplyAndDie();
    }

    const TListUsersRequest& Request() const {
        return SourceSqsRequest_.GetListUsers();
    }
};

IActor* CreateListUsersActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TListUsersActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
