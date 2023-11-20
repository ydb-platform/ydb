#include "action.h"
#include "attributes_md5.h"
#include "cfg.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "params.h"

#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/proto/records.pb.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TReceiveMessageActor
    : public TActionActor<TReceiveMessageActor>
{
public:
    static constexpr bool NeedQueueAttributes() {
        return true;
    }

    TReceiveMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ReceiveMessage, std::move(cb))
    {
    }

private:
    TDuration GetVisibilityTimeout() const {
        if (Request().HasVisibilityTimeout()) {
            return TDuration::Seconds(Request().GetVisibilityTimeout());
        } else {
            return QueueAttributes_->VisibilityTimeout;
        }
    }

    TInstant WaitDeadline() const {
        return StartTs_ + WaitTime_;
    }

    bool MaybeScheduleWait() {
        const TInstant waitDeadline = WaitDeadline();
        const TInstant now = TActivationContext::Now();
        const TDuration timeLeft = now < waitDeadline ? waitDeadline - now : TDuration::Zero();
        if (timeLeft >= TDuration::MilliSeconds(Cfg().GetMinTimeLeftForReceiveMessageWaitMs())) {
            const TDuration waitStep = Min(TDuration::Seconds(1), waitDeadline - now);
            this->Schedule(waitStep, new TEvWakeup());
            TotalWaitDuration_ += waitStep;
            RLOG_SQS_DEBUG("Schedule wait for " << waitStep.MilliSeconds() << "ms");
            Retried_ = true;
            return true;
        } else {
            return false;
        }
    }

    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableReceiveMessage(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        const auto& cfg = Cfg();

        if (Request().GetWaitTimeSeconds()
            && cfg.GetMaxWaitTimeoutMs()
            && TDuration::Seconds(Request().GetWaitTimeSeconds()) > TDuration::MilliSeconds(cfg.GetMaxWaitTimeoutMs())) {
            MakeError(Response_.MutableReceiveMessage(), NErrors::INVALID_PARAMETER_VALUE,
                      TStringBuilder() << "WaitTimeSeconds parameter must be less than or equal to "
                           << TDuration::MilliSeconds(cfg.GetMaxWaitTimeoutMs()).Seconds() << " seconds.");
            return false;
        }

        if (Request().GetMaxNumberOfMessages() > cfg.GetMaxNumberOfReceiveMessages()) {
            MakeError(Response_.MutableReceiveMessage(), NErrors::INVALID_PARAMETER_VALUE,
                      TStringBuilder() << "MaxNumberOfMessages parameter must be between 1 and " << cfg.GetMaxNumberOfReceiveMessages()
                           << ", if provided.");
            return false;
        }

        if (Request().HasVisibilityTimeout() && TDuration::Seconds(Request().GetVisibilityTimeout()) > TLimits::MaxVisibilityTimeout) {
            MakeError(Response_.MutableReceiveMessage(), NErrors::INVALID_PARAMETER_VALUE, "VisibilityTimeout parameter must be less than or equal to 12 hours.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableReceiveMessage()->MutableError();
    }

    void InitParams() {
        if (ParamsAreInited_) {
            return;
        }
        ParamsAreInited_ = true;

        if (Request().HasWaitTimeSeconds()) {
            WaitTime_ = TDuration::Seconds(Request().GetWaitTimeSeconds());
        } else {
            WaitTime_ = QueueAttributes_->ReceiveMessageWaitTime;
        }

        if (IsFifoQueue()) {
            if (Request().GetReceiveRequestAttemptId()) {
                ReceiveAttemptId_ = Request().GetReceiveRequestAttemptId();
            } else {
                ReceiveAttemptId_ = CreateGuidAsString();
            }
        }

        MaxMessagesCount_ = ClampVal(static_cast<size_t>(Request().GetMaxNumberOfMessages()), TLimits::MinBatchSize, TLimits::MaxBatchSize);
    }

    void DoAction() override {
        Become(&TThis::StateFunc);
        Y_ABORT_UNLESS(QueueAttributes_.Defined());

        InitParams();

        auto receiveRequest = MakeHolder<TSqsEvents::TEvReceiveMessageBatch>();
        receiveRequest->RequestId = RequestId_;
        receiveRequest->MaxMessagesCount = MaxMessagesCount_;
        receiveRequest->ReceiveAttemptId = ReceiveAttemptId_;
        receiveRequest->VisibilityTimeout = GetVisibilityTimeout();
        if (WaitTime_) {
            receiveRequest->WaitDeadline = WaitDeadline();
        }

        Send(QueueLeader_, std::move(receiveRequest));
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    void DoFinish() override {
        if (!Retried_) {
            const TDuration duration = GetRequestDuration();
            this->Send(
                QueueLeader_,
                new TSqsEvents::TEvLocalCounterChanged(
                    TSqsEvents::TEvLocalCounterChanged::ECounterType::ReceiveMessageImmediateDuration,
                    duration.MilliSeconds()
                )
            );
        }
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvReceiveMessageBatchResponse, HandleReceiveMessageBatchResponse);
        }
    }

    void HandleReceiveMessageBatchResponse(TSqsEvents::TEvReceiveMessageBatchResponse::TPtr& ev) {
        if (ev->Get()->Retried) {
            Retried_ = true;
        }

        if (ev->Get()->Failed) {
            MakeError(Response_.MutableReceiveMessage(), NErrors::INTERNAL_FAILURE);
        } else if (ev->Get()->OverLimit) {
            MakeError(Response_.MutableReceiveMessage(), NErrors::OVER_LIMIT);
        } else {
            if (ev->Get()->Messages.empty()) {
                if (MaybeScheduleWait()) {
                    return;
                } else {
                    this->Send(
                        QueueLeader_,
                        new TSqsEvents::TEvLocalCounterChanged(
                            TSqsEvents::TEvLocalCounterChanged:: ECounterType::ReceiveMessageEmptyCount,
                            1
                        )
                    );
                }
            }

            for (auto& message : ev->Get()->Messages) {
                auto* item = Response_.MutableReceiveMessage()->AddMessages();
                item->SetApproximateFirstReceiveTimestamp(message.FirstReceiveTimestamp.MilliSeconds());
                item->SetApproximateReceiveCount(message.ReceiveCount);
                item->SetMessageId(message.MessageId);
                item->SetData(message.Data);
                item->SetMD5OfMessageBody(MD5::Calc(message.Data));
                item->SetReceiptHandle(EncodeReceiptHandle(message.ReceiptHandle));
                RLOG_SQS_DEBUG("Encoded receipt handle: " << message.ReceiptHandle);
                item->SetSentTimestamp(message.SentTimestamp.MilliSeconds());
                if (message.SenderId) {
                    item->SetSenderId(message.SenderId);
                }

                if (message.MessageAttributes) {
                    TMessageAttributeList attrs;
                    if (attrs.ParseFromString(message.MessageAttributes)) {
                        for (auto& a : *attrs.MutableAttributes()) {
                            item->AddMessageAttributes()->Swap(&a);
                        }
                    }
                    if (item->MessageAttributesSize() > 0) {
                        const TString md5 = CalcMD5OfMessageAttributes(item->GetMessageAttributes());
                        item->SetMD5OfMessageAttributes(md5);
                    }
                }

                if (IsFifoQueue()) {
                    item->SetMessageDeduplicationId(message.MessageDeduplicationId);
                    item->SetMessageGroupId(message.MessageGroupId);
                    item->SetSequenceNumber(message.SequenceNumber);
                }
            }
        }
        SendReplyAndDie();
    }

    bool HandleWakeup(TEvWakeup::TPtr& ev) override {
        if (!TActionActor::HandleWakeup(ev)) {
            DoAction();
        }
        return true;
    }

    TDuration GetRequestWaitDuration() const override {
        return TotalWaitDuration_;
    }

    TString DumpState() override {
        TStringBuilder ret;
        ret << TActionActor::DumpState()
            << " Retried: " << Retried_
            << " WaitTime: " << WaitTime_
            << " ParamsAreInited: " << ParamsAreInited_
            << " MaxMessagesCount: " << MaxMessagesCount_
            << " TotalWaitDuration: " << TotalWaitDuration_
            << " ReceiveAttemptId: " << ReceiveAttemptId_;
        return std::move(ret);
    }

    const TReceiveMessageRequest& Request() const {
        return SourceSqsRequest_.GetReceiveMessage();
    }

private:
    TString ReceiveAttemptId_;
    bool Retried_ = false;
    TDuration WaitTime_ = TDuration::Zero();
    bool ParamsAreInited_ = false;
    size_t MaxMessagesCount_ = 0;
    TDuration TotalWaitDuration_;
};

IActor* CreateReceiveMessageActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TReceiveMessageActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
