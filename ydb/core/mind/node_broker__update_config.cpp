#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/protos/counters_node_broker.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NODE_BROKER

namespace NKikimr {
namespace NNodeBroker {

class TNodeBroker::TTxUpdateConfig : public TTransactionBase<TNodeBroker> {
public:
    TTxUpdateConfig(TNodeBroker *self,
                    TEvConsole::TEvConfigNotificationRequest::TPtr notification)
        : TBase(self)
        , Notification(std::move(notification))
        , Config(Notification->Get()->GetConfig().GetNodeBrokerConfig())
        , Modify(false)
    {
    }

    TTxUpdateConfig(TNodeBroker *self,
                    TEvNodeBroker::TEvSetConfigRequest::TPtr request)
        : TBase(self)
        , Request(std::move(request))
        , Config(Request->Get()->Record.GetConfig())
        , Modify(false)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_UPDATE_CONFIG; }

    bool ProcessNotification(const TActorContext &ctx)
    {
        auto &rec = Notification->Get()->Record;

        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateConfig Execute",
            {"ShortDebugString", rec.ShortDebugString()});

        if (!google::protobuf::util::MessageDifferencer::Equals(Config, Self->Dirty.Config))
            Modify = true;

        auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
        Response = new IEventHandle(Notification->Sender, Self->SelfId(), resp.Release(),
                                    0, Notification->Cookie);

        return true;
    }

    bool ProcessRequest(const TActorContext &ctx)
    {
        auto &rec = Request->Get()->Record;

        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateConfig Execute",
            {"ShortDebugString", rec.ShortDebugString()});

        if (!google::protobuf::util::MessageDifferencer::Equals(Config, Self->Dirty.Config))
            Modify = true;

        auto resp = MakeHolder<TEvNodeBroker::TEvSetConfigResponse>();
        resp->Record.MutableStatus()->SetCode(NKikimrNodeBroker::TStatus::OK);
        Response = new IEventHandle(Request->Sender, Self->SelfId(), resp.Release(),
                                    0, Request->Cookie);

        return true;
    }

    bool Execute(TTransactionContext &txc,
                 const TActorContext &ctx) override
    {
        if (Notification && !ProcessNotification(ctx))
            return true;

        if (Request && !ProcessRequest(ctx))
            return true;

        if (Modify) {
            Self->Dirty.DbUpdateConfig(Config, txc);
            Self->Dirty.LoadConfigFromProto(Config);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxUpdateConfig Complete");

        if (Modify)
            Self->Committed.LoadConfigFromProto(Config);

        if (Response) {
            YDB_LOG_CTX_TRACE(ctx, "TTxUpdateConfig reply",
                {"with", Response->ToString()});
            ctx.Send(Response);
        }

        Self->UpdateCommittedStateCounters();
    }

private:
    TEvConsole::TEvConfigNotificationRequest::TPtr Notification;
    TEvNodeBroker::TEvSetConfigRequest::TPtr Request;
    TAutoPtr<IEventHandle> Response;
    const NKikimrNodeBroker::TConfig &Config;
    bool Modify;
};

ITransaction *TNodeBroker::CreateTxUpdateConfig(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    return new TTxUpdateConfig(this, std::move(ev));
}

ITransaction *TNodeBroker::CreateTxUpdateConfig(TEvNodeBroker::TEvSetConfigRequest::TPtr &ev)
{
    return new TTxUpdateConfig(this, std::move(ev));
}

} // NNodeBroker
} // NKikimr
