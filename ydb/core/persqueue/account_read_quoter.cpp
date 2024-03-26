#include "account_read_quoter.h"
#include "event_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/counters.h>

#include <ydb/library/actors/core/log.h>

#include <util/string/join.h>
#include <util/string/vector.h>

namespace NKikimr {
namespace NPQ {

const TDuration UPDATE_COUNTERS_INTERVAL = TDuration::Seconds(5);
const TDuration DO_NOT_QUOTE_AFTER_ERROR_PERIOD = TDuration::Seconds(5);

const TString TAccountReadQuoter::READ_QUOTA_ROOT_PATH = "read-quota";
const TString TAccountWriteQuoter::WRITE_QUOTA_ROOT_PATH = "write-quota";

TBasicAccountQuoter::TBasicAccountQuoter(
    TActorId tabletActor,
    TActorId recepient,
    ui64 tabletId,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const TPartitionId& partition,
    const TQuoterParams& params,
    ui64 quotaCreditBytes,
    const TTabletCountersBase& counters,
    const TDuration& doNotQuoteAfterErrorPeriod
)
        : KesusPath(params.KesusPath)
        , ResourcePath(params.ResourcePath)
        , TopicConverter(topicConverter)
        , Partition(partition)
        , TabletActor(tabletActor)
        , Recepient(recepient)
        , TabletId(tabletId)
        , CreditBytes(quotaCreditBytes)
        , DoNotQuoteAfterErrorPeriod(doNotQuoteAfterErrorPeriod)
{
    Counters.Populate(counters);
}

void TBasicAccountQuoter::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateWork);
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);
}

void TBasicAccountQuoter::InitCounters(const TActorContext& ctx) {
    if (!CountersInited) {
        InitCountersImpl(ctx);
        CountersInited = true;
    }
}

void TBasicAccountQuoter::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_RATE_LIMITER, LimiterDescription() << " killed");
    for (const auto& event : Queue) {
        auto cookie = event.Request->Get()->Cookie;
        ReplyPersQueueError(
            TabletActor, ctx, TabletId, TopicConverter->GetClientsideName(), Partition, Counters, NKikimrServices::PQ_RATE_LIMITER,
            cookie, NPersQueue::NErrorCode::INITIALIZING,
            TStringBuilder() << "Tablet is restarting, topic " << TopicConverter->GetClientsideName() << " (ReadInfo) cookie " << cookie
        );
    }
    Die(ctx);
}

void TBasicAccountQuoter::HandleUpdateCounters(TEvPQ::TEvUpdateCounters::TPtr&, const TActorContext& ctx) {
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);
    ctx.Send(Recepient, MakeCountersUpdateEvent());
}

void TBasicAccountQuoter::HandleQuotaRequest(NAccountQuoterEvents::TEvRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_RATE_LIMITER,
        LimiterDescription() << ": quota required for cookie=" << ev->Get()->Cookie
    );
    InitCounters(ctx);
    bool hasActualErrors = ctx.Now() - LastReportedErrorTime < DoNotQuoteAfterErrorPeriod;
    if (ResourcePath && (QuotaRequestInFlight || !InProcessQuotaRequestCookies.empty()) && !hasActualErrors) {
        Queue.emplace_back(ev, ctx.Now());
    } else {
        ApproveQuota(ev, ctx.Now(), ctx);
    }
}

void TBasicAccountQuoter::HandleQuotaConsumed(NAccountQuoterEvents::TEvConsumed::TPtr& ev, const TActorContext& ctx) {
    ConsumedBytesInCredit += ev->Get()->BytesConsumed;
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_RATE_LIMITER, LimiterDescription()
        << "consumed quota " << ev->Get()->BytesConsumed
        << " bytes by cookie=" << ev->Get()->RequestCookie
        << ", consumed in credit " << ConsumedBytesInCredit << "/" << CreditBytes
    );
    auto it = InProcessQuotaRequestCookies.find(ev->Get()->RequestCookie);
    Y_ABORT_UNLESS(it != InProcessQuotaRequestCookies.end());
    InProcessQuotaRequestCookies.erase(it);

    if (!QuotaRequestInFlight) {
        if (ConsumedBytesInCredit >= CreditBytes && ResourcePath) {
            TThis::Send(MakeQuoterServiceID(),
                new TEvQuota::TEvRequest(
                    TEvQuota::EResourceOperator::And,
                    { TEvQuota::TResourceLeaf(KesusPath, ResourcePath, ConsumedBytesInCredit) },
                    TDuration::Max()),
                0,
                ++CurrentQuotaRequestCookie
            );
            QuotaRequestInFlight = true;
            ConsumedBytesInCredit = 0;
        } else if (!Queue.empty()){
            ApproveQuota(Queue.front().Request, Queue.front().StartWait, ctx);
            Queue.pop_front();
        }
    }
}

void TBasicAccountQuoter::HandleClearance(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx) {
    QuotaRequestInFlight = false;
    const ui64 cookie = ev->Cookie;
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_RATE_LIMITER,
        LimiterDescription() << "Got quota from Kesus:" << ev->Get()->Result << ". Cookie: " << cookie
    );

    Y_ABORT_UNLESS(CurrentQuotaRequestCookie == cookie);
    if (!Queue.empty()) {
        ApproveQuota(Queue.front().Request, Queue.front().StartWait, ctx);
        Queue.pop_front();
    }

    if (Y_UNLIKELY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Success)) {
        Y_ABORT_UNLESS(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Deadline); // We set deadline == inf in quota request.
        if (ctx.Now() - LastReportedErrorTime > TDuration::Minutes(1)) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_RATE_LIMITER, LimiterDescription() << "Got quota request error: " << ev->Get()->Result);
            LastReportedErrorTime = ctx.Now();
        }
        return;
    }
}

void TBasicAccountQuoter::ApproveQuota(NAccountQuoterEvents::TEvRequest::TPtr& ev, TInstant startWait, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_RATE_LIMITER,
        LimiterDescription() << " approve read for cookie=" << ev->Get()->Cookie
    );
    InProcessQuotaRequestCookies.insert(ev->Get()->Cookie);

    auto waitTime = ctx.Now() - startWait;
    auto released = ev->Release();
    TThis::Send(Recepient, new NAccountQuoterEvents::TEvResponse(std::move(released), waitTime));

    if (QuotaWaitCounter) {
        QuotaWaitCounter->IncFor(waitTime.MilliSeconds());
    }
}

TQuoterParams TAccountReadQuoter::GetQuoterParams(const TString& user) {
    auto consumerPath = NPersQueue::ConvertOldConsumerName(user);
    TQuoterParams ret;
    auto userParts = SplitString(consumerPath, "/"); // account/folder/topic // account is first element

    const TString account = userParts[0];
    userParts[0] = READ_QUOTA_ROOT_PATH; // read-quota/folder/topic

    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    ret.KesusPath = Join("/", quotingConfig.GetQuotersDirectoryPath(), account);
    ret.ResourcePath = JoinSeq("/", userParts);
    return ret;
}

TAccountReadQuoter::TAccountReadQuoter(
    TActorId tabletActor,
    TActorId recepient,
    ui64 tabletId,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const TPartitionId& partition,
    const TString& user,
    const TTabletCountersBase& counters
)
    : TBasicAccountQuoter(tabletActor, recepient, tabletId, topicConverter, partition, GetQuoterParams(user),
            AppData()->PQConfig.GetQuotingConfig().GetReadCreditBytes(), counters, DO_NOT_QUOTE_AFTER_ERROR_PERIOD)
    , User(user)
{
    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_RATE_LIMITER,
        LimiterDescription() <<" kesus=" << KesusPath << " resource_path=" << ResourcePath);
    ConsumerPath = NPersQueue::ConvertOldConsumerName(user);
}


void TAccountReadQuoter::InitCountersImpl(const TActorContext& ctx) {
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
}

THolder<NAccountQuoterEvents::TEvCounters> TAccountReadQuoter::MakeCountersUpdateEvent() {
    return MakeHolder<NAccountQuoterEvents::TEvCounters>(Counters, true, User);
}

TString TAccountReadQuoter::LimiterDescription() const {
    return TStringBuilder() << "topic=" << TopicConverter->GetClientsideName() << ":" << Partition << " user=" << User << ": ";
}


TAccountWriteQuoter::TAccountWriteQuoter(
    TActorId tabletActor,
    TActorId recepient,
    ui64 tabletId,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const TPartitionId& partition,
    const TTabletCountersBase& counters,
    const TActorContext& ctx
)
    : TBasicAccountQuoter(tabletActor, recepient, tabletId, topicConverter, partition,
                          CreateQuoterParams(AppData()->PQConfig, topicConverter, tabletId, ctx),
                          0, counters,
                          TDuration::Zero())
{
}

TQuoterParams TAccountWriteQuoter::CreateQuoterParams(
        const NKikimrPQ::TPQConfig& pqConfig, NPersQueue::TTopicConverterPtr topicConverter,
        ui64 tabletId, const TActorContext& ctx
) {
    TQuoterParams params;
    const auto& quotingConfig = pqConfig.GetQuotingConfig();
    Y_ABORT_UNLESS(quotingConfig.GetTopicWriteQuotaEntityToLimit() != NKikimrPQ::TPQConfig::TQuotingConfig::UNSPECIFIED);
    auto topicPath = topicConverter->GetFederationPath();

    auto topicParts = SplitPath(topicPath); // account/folder/topic // account is first element
    if (topicParts.size() < 2) {
        LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE,
                    "tablet " << tabletId << " topic '" << topicPath << "' Bad topic name. Disable quoting for topic");
        return params;
    }
    topicParts[0] = WRITE_QUOTA_ROOT_PATH; // write-quota/folder/topic
    params.KesusPath = TStringBuilder() << quotingConfig.GetQuotersDirectoryPath() << "/" << topicConverter->GetAccount();
    params.ResourcePath = JoinPath(topicParts);

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "topicWriteQuutaResourcePath '" << params.ResourcePath
                << "' topicWriteQuoterPath '" << params.KesusPath
                << "' account '" << topicConverter->GetAccount()
                << "'"
    );
    return params;
}

void TAccountWriteQuoter::InitCountersImpl(const TActorContext&) {
}

THolder<NAccountQuoterEvents::TEvCounters> TAccountWriteQuoter::MakeCountersUpdateEvent() {
    return MakeHolder<NAccountQuoterEvents::TEvCounters>(Counters, false, TString{});
}

TString TAccountWriteQuoter::LimiterDescription() const {
    return TStringBuilder() << "topic=" << TopicConverter->GetClientsideName() << ":" << Partition << " writeQuoter" << ": ";
}


constexpr NKikimrServices::TActivity::EType TAccountReadQuoter::ActorActivityType() {
    return NKikimrServices::TActivity::PERSQUEUE_ACCOUNT_READ_QUOTER;
}

constexpr NKikimrServices::TActivity::EType TAccountWriteQuoter::ActorActivityType() {
    return NKikimrServices::TActivity::PERSQUEUE_ACCOUNT_WRITE_QUOTER;
}

}// NPQ
}// NKikimr
