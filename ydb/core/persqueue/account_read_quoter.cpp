#include "account_read_quoter.h"
#include "event_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/counters.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/join.h>
#include <util/string/vector.h>

namespace NKikimr {
namespace NPQ {

const TDuration UPDATE_COUNTERS_INTERVAL = TDuration::Seconds(5);
const TDuration DO_NOT_QUOTE_AFTER_ERROR_PERIOD = TDuration::Seconds(5);

const TString TAccountReadQuoter::READ_QUOTA_ROOT_PATH = "read-quota";

constexpr NKikimrServices::TActivity::EType TAccountReadQuoter::ActorActivityType() {
    return NKikimrServices::TActivity::PERSQUEUE_ACCOUNT_READ_QUOTER;
}

TAccountReadQuoter::TAccountReadQuoter(
    TActorId tabletActor,
    TActorId recepient,
    ui64 tabletId,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    ui32 partition,
    const TString& user,
    const TTabletCountersBase& counters
)
        : TabletActor(tabletActor)
        , Recepient(recepient)
        , TabletId(tabletId)
        , TopicConverter(topicConverter)
        , Partition(partition)
        , User(user)
        , ConsumerPath(NPersQueue::ConvertOldConsumerName(user))
        , ReadCreditBytes(AppData()->PQConfig.GetQuotingConfig().GetReadCreditBytes())
{
    Counters.Populate(counters);

    auto userParts = SplitString(ConsumerPath, "/"); // account/folder/topic // account is first element

    const TString account = userParts[0];
    userParts[0] = READ_QUOTA_ROOT_PATH; // read-quota/folder/topic

    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    KesusPath = Join("/", quotingConfig.GetQuotersDirectoryPath(), account);
    QuotaResourcePath = JoinSeq("/", userParts);
    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_SPEED_LIMITER,
        LimiterDescription() <<" kesus=" << KesusPath << " resource_path=" << QuotaResourcePath);
}

void TAccountReadQuoter::InitCounters(const TActorContext& ctx) {
    if (CountersInited) {
        return;
    }
    auto counters = AppData(ctx)->Counters;
    if (counters) {
        QuotaWaitCounter.Reset(new TPercentileCounter(
            GetServiceCounters(counters, "pqproxy|consumerReadQuotaWait"),
            NPersQueue::GetLabels(TopicConverter),
            {
                {"Client", User},
                {"ConsumerPath", ConsumerPath},
                {"sensor", "ConsumerReadQuotaWait"}
            },
            "Interval",
            TVector<std::pair<ui64, TString>>{{0, "0ms"}, {1, "1ms"}, {5, "5ms"}, {10, "10ms"},
                {20, "20ms"}, {50, "50ms"}, {100, "100ms"}, {500, "500ms"},
                {1000, "1000ms"}, {2500, "2500ms"}, {5000, "5000ms"}, {10000, "10000ms"}, {9999999, "999999ms"}},
            true
        ));
    }
    CountersInited = true;
}


void TAccountReadQuoter::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateWork);
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);
}

void TAccountReadQuoter::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_SPEED_LIMITER, LimiterDescription() << " killed");
    for (const auto& event : Queue) {
        auto cookie = event.Event->Get()->Cookie;
        ReplyPersQueueError(
            TabletActor, ctx, TabletId, TopicConverter->GetClientsideName(), Partition, Counters, NKikimrServices::PQ_READ_SPEED_LIMITER,
            cookie, NPersQueue::NErrorCode::INITIALIZING,
            TStringBuilder() << "Tablet is restarting, topic " << TopicConverter->GetClientsideName() << " (ReadInfo) cookie " << cookie
        );
    }
    Die(ctx);
}

void TAccountReadQuoter::HandleUpdateCounters(TEvPQ::TEvUpdateCounters::TPtr&, const TActorContext& ctx) {
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);
    ctx.Send(Recepient, new NAccountReadQuoterEvents::TEvCounters(Counters, User));
}

void TAccountReadQuoter::HandleReadQuotaRequest(NAccountReadQuoterEvents::TEvRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_SPEED_LIMITER,
        LimiterDescription() << " quota required for cookie=" << ev->Get()->ReadRequest->Get()->Cookie
    );
    InitCounters(ctx);
    bool hasActualErrors = ctx.Now() - LastReportedErrorTime <= DO_NOT_QUOTE_AFTER_ERROR_PERIOD;
    if ((QuotaRequestInFlight || !InProcessReadRequestCookies.empty()) && !hasActualErrors) {
        Queue.emplace_back(std::move(ev->Get()->ReadRequest), ctx.Now());
    } else {
        ApproveRead(ev->Get()->ReadRequest, ctx.Now(), ctx);
    }
}

void TAccountReadQuoter::HandleReadQuotaConsumed(NAccountReadQuoterEvents::TEvConsumed::TPtr& ev, const TActorContext& ctx) {
    ConsumedBytesInCredit += ev->Get()->ReadBytes;
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_SPEED_LIMITER, LimiterDescription()
        << "consumed read quota " << ev->Get()->ReadBytes
        << " bytes by cookie=" << ev->Get()->ReadRequestCookie
        << ", consumed in credit " << ConsumedBytesInCredit << "/" << ReadCreditBytes
    );
    auto it = InProcessReadRequestCookies.find(ev->Get()->ReadRequestCookie);
    Y_VERIFY(it != InProcessReadRequestCookies.end());
    InProcessReadRequestCookies.erase(it);

    if (!QuotaRequestInFlight) {
        if (ConsumedBytesInCredit >= ReadCreditBytes) {
            Send(MakeQuoterServiceID(),
                new TEvQuota::TEvRequest(
                    TEvQuota::EResourceOperator::And,
                    { TEvQuota::TResourceLeaf(KesusPath, QuotaResourcePath, ConsumedBytesInCredit) },
                    TDuration::Max()),
                0,
                ++CurrentQuotaRequestCookie
            );
            QuotaRequestInFlight = true;
            ConsumedBytesInCredit = 0;
        } else if (!Queue.empty()){
            ApproveRead(std::move(Queue.front().Event), Queue.front().StartWait, ctx);
            Queue.pop_front();
        }
    }
}

void TAccountReadQuoter::HandleClearance(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx) {
    QuotaRequestInFlight = false;
    const ui64 cookie = ev->Cookie;
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_SPEED_LIMITER,
        LimiterDescription() << "Got read quota:" << ev->Get()->Result << ". Cookie: " << cookie
    );

    Y_VERIFY(CurrentQuotaRequestCookie == cookie);
    if (!Queue.empty()) {
        ApproveRead(std::move(Queue.front().Event), Queue.front().StartWait, ctx);
        Queue.pop_front();
    }

    if (Y_UNLIKELY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Success)) {
        Y_VERIFY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Deadline); // We set deadline == inf in quota request.
        if (ctx.Now() - LastReportedErrorTime > TDuration::Minutes(1)) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_READ_SPEED_LIMITER, LimiterDescription() << "Got read quota error: " << ev->Get()->Result);
            LastReportedErrorTime = ctx.Now();
        }
        return;
    }
}

void TAccountReadQuoter::TAccountReadQuoter::ApproveRead(TEvPQ::TEvRead::TPtr ev, TInstant startWait, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_SPEED_LIMITER,
        LimiterDescription() << " approve read for cookie=" << ev->Get()->Cookie
    );
    InProcessReadRequestCookies.insert(ev->Get()->Cookie);

    auto waitTime = ctx.Now() - startWait;
    Send(Recepient, new NAccountReadQuoterEvents::TEvResponse(ev.Release(), waitTime));

    if (QuotaWaitCounter) {
        QuotaWaitCounter->IncFor(waitTime.MilliSeconds());
    }
}

TString TAccountReadQuoter::LimiterDescription() const {
    return TStringBuilder() << "topic=" << TopicConverter->GetClientsideName() << ":" << Partition << " user=" << User << ": ";
}

}// NPQ
}// NKikimr
