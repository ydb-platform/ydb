#pragma once

#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <ydb/core/tx/columnshard/subscriber/abstract/subscriber/subscriber.h>
#include <ydb/core/tx/columnshard/subscriber/events/append_completed/event.h>

#include <ydb/core/statistics/events.h>

#include <ydb/library/actors/interconnect/types.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

#include <memory>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NColumnShard::NSubscriber {

class TAnalyzeSubscriber: public ISubscriber {
private:
    const TActorId OriginalSender;
    const ui64 OriginalCookie;
    const TActorContext& Context;
    std::unique_ptr<NStat::TEvStatistics::TEvAnalyzeTableResponse> ResponseDraft;

public:
    virtual std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::AppendCompleted };
    }

    virtual bool DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard&) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::AppendCompleted);

        auto& responseRecord = ResponseDraft->Record;

        responseRecord.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        return Context.Send(OriginalSender, ResponseDraft.release(), 0, OriginalCookie);
    }

    virtual bool IsFinished() const override {
        return !ResponseDraft;
    }

    TAnalyzeSubscriber(const TActorId& sender,
                       const ui64 cookie,
                       const TActorContext& ctx,
                       std::unique_ptr<NStat::TEvStatistics::TEvAnalyzeTableResponse>&& response)
        : OriginalSender(sender)
        , OriginalCookie(cookie)
        , Context(ctx)
        , ResponseDraft(std::move(response))
    {
    }
};

}
