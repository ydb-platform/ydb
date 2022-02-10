#include "retention.h"
#include "cfg.h"
#include "log.h"
#include "executor.h"

#include <ydb/public/lib/value/value.h> 
#include <ydb/core/base/appdata.h> 
#include <ydb/core/ymq/base/debug_info.h> 

#include <library/cpp/actors/core/hfunc.h>
#include <util/random/random.h>

namespace NKikimr::NSQS {

TRetentionActor::TRetentionActor(const TQueuePath& queuePath, const TActorId& queueLeader)
    : QueuePath_(queuePath)
    , RequestId_(CreateGuidAsString())
    , QueueLeader_(queueLeader)
{
    DebugInfo->QueueRetentionActors.emplace(TStringBuilder() << TLogQueueName(QueuePath_), this);
}

TRetentionActor::~TRetentionActor() {
    DebugInfo->QueueRetentionActors.EraseKeyValue(TStringBuilder() << TLogQueueName(QueuePath_), this);
}

void TRetentionActor::Bootstrap() {
    RLOG_SQS_INFO("Bootstrap retention actor for queue " << TLogQueueName(QueuePath_));
    Become(&TThis::StateFunc);
    Schedule(RandomRetentionPeriod(), new TEvWakeup());
}

TDuration TRetentionActor::RandomRetentionPeriod() const {
    const TDuration minPeriod = TDuration::MilliSeconds(Cfg().GetMinMessageRetentionPeriodMs());
    return minPeriod +
        TDuration::MilliSeconds(RandomNumber<ui32>(minPeriod.MilliSeconds() / 2));
}

void TRetentionActor::SetRetentionBoundary() {
    auto onExecuted = [this](const TSqsEvents::TEvExecuted::TRecord& ev) {
        ui32 status = ev.GetStatus();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            using NKikimr::NClient::TValue;
            const TValue val(TValue::Create(ev.GetExecutionEngineEvaluatedResponse()));
            const TValue list(val["result"]);

            for (size_t i = 0; i < list.Size(); ++i) {
                auto req = MakeHolder<TSqsEvents::TEvPurgeQueue>();
                req->QueuePath = QueuePath_;
                req->Boundary = TInstant::MilliSeconds(ui64(list[i]["RetentionBoundary"]));
                req->Shard = ui64(list[i]["Shard"]);

                RLOG_SQS_INFO("Set retention boundary for queue " << TLogQueueName(QueuePath_, req->Shard) << " to " << req->Boundary.MilliSeconds() << " (" << req->Boundary << ")");

                Send(QueueLeader_, std::move(req));
            }
        } else {
            RLOG_SQS_ERROR("Failed to set retention boundary for queue " << TLogQueueName(QueuePath_));
        }

        Schedule(RandomRetentionPeriod(), new TEvWakeup());
    };

    TExecutorBuilder(SelfId(), RequestId_)
        .User(QueuePath_.UserName)
        .Queue(QueuePath_.QueueName)
        .QueueLeader(QueueLeader_)
        .QueryId(SET_RETENTION_ID)
        .RetryOnTimeout()
        .OnExecuted(onExecuted)
        .Params()
            .Uint64("NOW", Now().MilliSeconds())
            .Bool("PURGE", false)
        .ParentBuilder().Start();

    RLOG_SQS_TRACE("Executing retention request for queue " << TLogQueueName(QueuePath_));
}

void TRetentionActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    RLOG_SQS_DEBUG("Handle executed in retention actor for queue " << TLogQueueName(QueuePath_));
    ev->Get()->Call();
}

void TRetentionActor::HandlePoisonPill(TEvPoisonPill::TPtr&) {
    RLOG_SQS_DEBUG("Handle poison pill in retention actor for queue " << TLogQueueName(QueuePath_));
    PassAway();
}

void TRetentionActor::HandleWakeup() {
    RLOG_SQS_DEBUG("Handle wakeup in retention actor for queue " << TLogQueueName(QueuePath_));
    SetRetentionBoundary();
}

STATEFN(TRetentionActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        cFunc(TEvWakeup::EventType, HandleWakeup);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        hFunc(TEvPoisonPill, HandlePoisonPill);
    }
}

} // namespace NKikimr::NSQS
