#include "tablet_impl.h"
#include "quoter_constants.h"

#include <util/system/datetime.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KESUS_TABLET

namespace NKikimr {
namespace NKesus {

class TKesusTablet::TQuoterResourceSink : public IResourceSink {
public:
    TQuoterResourceSink(const TActorId& actor, TKesusTablet* kesus)
        : Actor(actor)
        , Kesus(kesus)
    {
    }

    void Send(ui64 resourceId, double amount, const NKikimrKesus::TStreamingQuoterResource* props) override {
        Kesus->QuoterResourceSessionsAccumulator.Accumulate(Actor, resourceId, amount, props);
    }

    void Sync(ui64 resourceId, ui32 lastReportId, double available) override {
        Kesus->QuoterResourceSessionsAccumulator.Sync(Actor, resourceId, lastReportId, available);
    }

    void CloseSession(ui64 resourceId, Ydb::StatusIds::StatusCode status, const TString& reason) override {
        THolder<TEvKesus::TEvResourcesAllocated> ev = MakeHolder<TEvKesus::TEvResourcesAllocated>();
        auto* info = ev->Record.AddResourcesInfo();
        info->SetResourceId(resourceId);
        TEvKesus::FillError(info->MutableStateNotification(), status, reason);
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TEvResourcesAllocated",
            {"tabletId", Kesus->TabletID()},
            {"actorId", Actor},
            {"cookie", 0},
            {"ev", ev->Record});
        Kesus->Send(Actor, std::move(ev));
    }

private:
    TActorId Actor;
    TKesusTablet* Kesus;
};

void TKesusTablet::TQuoterResourceSessionsAccumulator::Accumulate(const TActorId& recipient, ui64 resourceId, double amount, const NKikimrKesus::TStreamingQuoterResource* props) {
    TSendInfo& info = SendInfos[recipient];
    if (!info.Event) {
        info.Event = MakeHolder<TEvKesus::TEvResourcesAllocated>();
    }
    auto [indexIt, insertedNew] = info.ResIdIndex.try_emplace(resourceId, info.Event->Record.ResourcesInfoSize());
    NKikimrKesus::TEvResourcesAllocated::TResourceInfo* resInfo = nullptr;
    if (insertedNew) {
        resInfo = info.Event->Record.AddResourcesInfo();
        resInfo->SetResourceId(resourceId);
        resInfo->SetAmount(amount);
        resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
    } else {
        Y_ABORT_UNLESS(indexIt->second < info.Event->Record.ResourcesInfoSize());
        resInfo = info.Event->Record.MutableResourcesInfo(indexIt->second);
        resInfo->SetAmount(resInfo->GetAmount() + amount);
    }

    if (props) {
        *resInfo->MutableEffectiveProps() = *props;
    }
}

void TKesusTablet::TQuoterResourceSessionsAccumulator::Sync(const TActorId& recipient, ui64 resourceId, ui32 lastReportId, double amount) {
    TSendSyncInfo& info = SendSyncInfos[recipient];
    if (!info.Event) {
        info.Event = MakeHolder<TEvKesus::TEvSyncResources>();
    }
    auto [indexIt, insertedNew] = info.ResIdIndex.try_emplace(resourceId, info.Event->Record.ResourcesInfoSize());
    NKikimrKesus::TEvSyncResources::TResourceInfo* resInfo = nullptr;
    if (insertedNew) {
        resInfo = info.Event->Record.AddResourcesInfo();
        resInfo->SetResourceId(resourceId);
        resInfo->SetAvailable(amount);
        resInfo->SetLastReportId(lastReportId);
    } else {
        Y_ABORT_UNLESS(indexIt->second < info.Event->Record.ResourcesInfoSize());
        resInfo = info.Event->Record.MutableResourcesInfo(indexIt->second);
        resInfo->SetAvailable(amount);
        resInfo->SetLastReportId(lastReportId);
    }
}

void TKesusTablet::TQuoterResourceSessionsAccumulator::SendAll(const TActorContext& ctx, ui64 tabletId) {
    for (auto infoIter = SendInfos.begin(), infoEnd = SendInfos.end(); infoIter != infoEnd; ++infoIter) {
        const TActorId& recipientId = infoIter->first;
        auto& info = infoIter->second;
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletId, recipientId, cookie, data",
            {"tabletId", tabletId},
            {"recipientId", recipientId},
            {"cookie", 0},
            {"data", info.Event->Record});
        ctx.Send(recipientId, std::move(info.Event));
    }
    SendInfos.clear();

    for (auto infoIter = SendSyncInfos.begin(), infoEnd = SendSyncInfos.end(); infoIter != infoEnd; ++infoIter) {
        const TActorId& recipientId = infoIter->first;
        auto& info = infoIter->second;
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletId, recipientId, cookie, data",
            {"tabletId", tabletId},
            {"recipientId", recipientId},
            {"cookie", 0},
            {"data", info.Event->Record});
        ctx.Send(recipientId, std::move(info.Event));
    }
    SendSyncInfos.clear();
}

void TKesusTablet::Handle(TEvKesus::TEvSubscribeOnResources::TPtr& ev) {
    THolder<TEvKesus::TEvSubscribeOnResourcesResult> reply = MakeHolder<TEvKesus::TEvSubscribeOnResourcesResult>();
    const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetActorID());
    const TActorId pipeServerId = ev->Recipient;
    const ui32 clientVersion = ev->Get()->Record.GetProtocolVersion();
    reply->Record.MutableResults()->Reserve(ev->Get()->Record.ResourcesSize());
    reply->Record.SetProtocolVersion(NQuoter::KESUS_PROTOCOL_VERSION);
    IResourceSink::TPtr sink = new TQuoterResourceSink(ev->Sender, this);
    const TInstant now = TActivationContext::Now();
    TTickProcessorQueue queue;
    i64 subscriptions = 0;
    i64 unknownSubscriptions = 0;
    for (const NKikimrKesus::TEvSubscribeOnResources::TResourceSubscribeInfo& resource : ev->Get()->Record.GetResources()) {
        NKikimrKesus::TEvSubscribeOnResourcesResult::TResourceSubscribeResult* result = reply->Record.AddResults();
        TQuoterResourceTree* resourceTree = QuoterResources.FindPath(resource.GetResourcePath());
        if (resourceTree) {
            ++subscriptions;
            TQuoterSession* session = QuoterResources.GetOrCreateSession(clientId, clientVersion, resourceTree);
            session->SetResourceSink(sink);
            const NActors::TActorId prevPipeServerId = session->SetPipeServerId(pipeServerId);
            QuoterResources.SetPipeServerId(TQuoterSessionId(clientId, resourceTree->GetResourceId()), prevPipeServerId, pipeServerId);
            session->UpdateConsumptionState(resource.GetStartConsuming(), resource.GetInitialAmount(), queue, now);

            result->MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
            resourceTree->FillSubscribeResult(*result);
            *result->MutableEffectiveProps() = resourceTree->GetEffectiveProps();
        } else {
            ++unknownSubscriptions;
            TEvKesus::FillError(result->MutableError(), Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Resource \"" << resource.GetResourcePath() << "\" doesn't exist.");
        }
    }
    if (subscriptions) {
        *QuoterResources.GetCounters().ResourceSubscriptions += subscriptions;
    }
    if (unknownSubscriptions) {
        *QuoterResources.GetCounters().UnknownResourceSubscriptions += unknownSubscriptions;
    }
    QuoterTickProcessorQueue.Merge(std::move(queue));
    YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletID, #_ev->Sender, cookie, data",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie},
        {"data", reply->Record});
    Send(ev->Sender, std::move(reply), 0, ev->Cookie);

    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Subscribe on quoter resources",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie});

    HandleQuoterTick();
}

void TKesusTablet::Handle(TEvKesus::TEvUpdateConsumptionState::TPtr& ev) {
    THolder<TEvKesus::TEvResourcesAllocated> errors;
    const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetActorID());
    const TInstant now = TActivationContext::Now();
    IResourceSink::TPtr sink = new TQuoterResourceSink(ev->Sender, this);
    TTickProcessorQueue queue;
    i64 consumptionStarts = 0;
    i64 consumptionStops = 0;
    for (const NKikimrKesus::TEvUpdateConsumptionState::TResourceInfo& resource : ev->Get()->Record.GetResourcesInfo()) {
        if (TQuoterSession* session = QuoterResources.FindSession(clientId, resource.GetResourceId())) {
            if (resource.GetConsumeResource()) {
                ++consumptionStarts;
            } else {
                ++consumptionStops;
            }
            session->SetResourceSink(sink);
            session->UpdateConsumptionState(resource.GetConsumeResource(), resource.GetAmount(), queue, now);
        } else {
            if (!errors) {
                errors = MakeHolder<TEvKesus::TEvResourcesAllocated>();
            }
            auto* notification = errors->Record.AddResourcesInfo();
            notification->SetResourceId(resource.GetResourceId());
            TEvKesus::FillError(notification->MutableStateNotification(), Ydb::StatusIds::BAD_SESSION, "No such session exists.");
        }
    }
    if (consumptionStarts) {
        *QuoterResources.GetCounters().ResourceConsumptionStarts += consumptionStarts;
    }
    if (consumptionStops) {
        *QuoterResources.GetCounters().ResourceConsumptionStops += consumptionStops;
    }
    QuoterTickProcessorQueue.Merge(std::move(queue));
    if (errors) {
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletID, #_ev->Sender, cookie, data",
            {"tabletID", TabletID()},
           {"sender", ev->Sender},
            {"cookie", 0},
            {"data", errors->Record});
        Send(ev->Sender, std::move(errors));
    }
    auto ack = MakeHolder<TEvKesus::TEvUpdateConsumptionStateAck>();
    YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletID, #_ev->Sender, cookie, data",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie},
        {"data", ack->Record});
    Send(ev->Sender, std::move(ack), 0, ev->Cookie);

    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Update quoter resources consumption state",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie});

    HandleQuoterTick();
}

void TKesusTablet::Handle(TEvKesus::TEvAccountResources::TPtr& ev) {
    auto ack = MakeHolder<TEvKesus::TEvAccountResourcesAck>();
    const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetActorID());
    const TInstant now = TActivationContext::Now();
    TTickProcessorQueue queue;
    for (const NKikimrKesus::TEvAccountResources::TResourceInfo& resource : ev->Get()->Record.GetResourcesInfo()) {
        auto* result = ack->Record.AddResourcesInfo();
        result->SetResourceId(resource.GetResourceId());
        if (TQuoterSession* session = QuoterResources.FindSession(clientId, resource.GetResourceId())) {
            TInstant accepted = session->Account(
                TInstant::MicroSeconds(resource.GetStartUs()),
                TDuration::MicroSeconds(resource.GetIntervalUs()),
                resource.GetAmount().data(),
                resource.GetAmount().size(),
                queue, now);
            result->SetAcceptedUs(accepted.MicroSeconds());
        } else {
            TEvKesus::FillError(result->MutableStateNotification(), Ydb::StatusIds::BAD_SESSION, "No such session exists.");
        }
    }
    QuoterTickProcessorQueue.Merge(std::move(queue));
    YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletID, #_ev->Sender, cookie, data",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie},
        {"data", ack->Record});
    Send(ev->Sender, std::move(ack), 0, ev->Cookie);

    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Account quoter resources",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie});

    HandleQuoterTick();
}

void TKesusTablet::Handle(TEvKesus::TEvReportResources::TPtr& ev) {
    auto ack = MakeHolder<TEvKesus::TEvReportResourcesAck>();
    const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetActorID());
    const TInstant now = TActivationContext::Now();
    TTickProcessorQueue queue;
    for (const NKikimrKesus::TEvReportResources::TResourceInfo& resource : ev->Get()->Record.GetResourcesInfo()) {
        auto* result = ack->Record.AddResourcesInfo();
        result->SetResourceId(resource.GetResourceId());
        if (TQuoterSession* session = QuoterResources.FindSession(clientId, resource.GetResourceId())) {
            session->ReportConsumed(resource.GetReportId(), resource.GetTotalConsumed(), queue, now);
        } else {
            TEvKesus::FillError(result->MutableStateNotification(), Ydb::StatusIds::BAD_SESSION, "No such session exists.");
        }
    }
    QuoterTickProcessorQueue.Merge(std::move(queue));
    YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletID, #_ev->Sender, cookie, data",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie},
        {"data", ack->Record});
    Send(ev->Sender, std::move(ack), 0, ev->Cookie);

    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Report quoter resources",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie});

    HandleQuoterTick();
}

void TKesusTablet::Handle(TEvKesus::TEvResourcesAllocatedAck::TPtr& ev) {
    Y_UNUSED(ev);
}

void TKesusTablet::ScheduleQuoterTick() {
    if (!QuoterTickProcessingIsScheduled && !QuoterTickProcessorQueue.Empty()) {
        const TInstant time = QuoterTickProcessorQueue.Top().Time;
        if (time < NextQuoterTickTime) {
            const TInstant now = TActivationContext::Now();
            Schedule(time - now, new TEvents::TEvWakeup(QUOTER_TICK_PROCESSING_WAKEUP_TAG));
            QuoterTickProcessingIsScheduled = true;
            NextQuoterTickTime = time;
        }
    }
}

void TKesusTablet::HandleQuoterTick() {
    const NHPTimer::STime hpprev = GetCycleCountFast();
    NextQuoterTickTime = TInstant::Max();
    i64 processedTasks = 0;
    while (!QuoterTickProcessorQueue.Empty()) {
        const TInstant now = TActivationContext::Now();
        bool processed = false;
        const TInstant topTime = QuoterTickProcessorQueue.Top().Time;
        if (now >= topTime) {
            TTickProcessorQueue queue;
            do {
                QuoterResources.ProcessTick(QuoterTickProcessorQueue.Top(), queue);
                QuoterTickProcessorQueue.Pop();
                processed = true;
                ++processedTasks;
            } while (!QuoterTickProcessorQueue.Empty() && QuoterTickProcessorQueue.Top().Time == topTime);

            if (processed) {
                QuoterTickProcessorQueue.Merge(std::move(queue));
            }
        }

        if (!processed) {
            break;
        }
    }
    ScheduleQuoterTick();
    QuoterResourceSessionsAccumulator.SendAll(TActivationContext::AsActorContext(), TabletID());
    const NHPTimer::STime hpnow = GetCycleCountFast();
    *QuoterResources.GetCounters().ElapsedMicrosecOnResourceAllocation += NHPTimer::GetSeconds(hpnow - hpprev) * 1000000;
    if (processedTasks) {
        *QuoterResources.GetCounters().TickProcessorTasksProcessed += processedTasks;
    }
}

void TKesusTablet::Handle(TEvKesus::TEvGetQuoterResourceCounters::TPtr& ev) {
    THolder<TEvKesus::TEvGetQuoterResourceCountersResult> reply = MakeHolder<TEvKesus::TEvGetQuoterResourceCountersResult>();
    QuoterResources.FillCounters(reply->Record);
    YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Dump tabletID, #_ev->Sender, cookie, data",
        {"tabletID", TabletID()},
       {"sender", ev->Sender},
        {"cookie", ev->Cookie},
        {"data", reply->Record});
    Send(ev->Sender, std::move(reply), 0, ev->Cookie);
}

void TKesusTablet::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
    YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "Got TEvServerDisconnected(",
        {"serverId", ev->Get()->ServerId});
    QuoterResources.DisconnectSession(ev->Get()->ServerId);
}

}
}
