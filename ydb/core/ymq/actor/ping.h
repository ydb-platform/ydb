#pragma once
#include "defs.h"
#include "actor.h"
#include "log.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSQS {

class TPingActor
    : public TActorBootstrapped<TPingActor>
{
public:
    TPingActor(THolder<IPingReplyCallback> cb, const TString& requestId)
        : Callback_(std::move(cb))
        , RequestId_(requestId)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_PING_ACTOR;
    }

    void Bootstrap() {
        RLOG_SQS_DEBUG("Send reply to ping");
        Callback_->DoSendReply();
        PassAway();
    }

private:
    THolder<IPingReplyCallback> Callback_;
    const TString RequestId_;
};

} // namespace NKikimr::NSQS
