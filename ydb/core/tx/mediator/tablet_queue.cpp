#include "mediator_impl.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>

#include <util/string/builder.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NTxMediator {

class TTxMediatorTabletQueue : public TActor<TTxMediatorTabletQueue> {
    struct TStepEntry;
    struct TGranularWatcher;
    struct TGranularServer;

    struct TTabletEntry : TIntrusiveListItem<TTabletEntry> {
        enum class EState {
            Init,
            Connecting,
            Connected,
        };

        struct TStep : TIntrusiveListItem<TStep> {
            TStepEntry* const StepRef;
            // The current list of transactions
            TVector<TTx> Transactions;
            // An updated list of transactions (after last sent to tablet)
            // We may need to send acks to the updated AckTo actor
            TVector<TTx> OutOfOrder;

            TStep(TStepEntry* stepRef)
                : StepRef(stepRef)
            {}
        };

        const ui64 TabletId;
        EState State = EState::Init;
        // We usually have at most a couple of steps inflight
        // Making it a list complicates things and not worth it
        TMap<TStepId, TStep> Queue;
        // A set of watchers that are watching this tablet
        THashSet<TGranularWatcher*> Watchers;

        explicit TTabletEntry(ui64 tabletId)
            : TabletId(tabletId)
        {}

        bool Redundant() const {
            return State == EState::Init && Queue.empty() && Watchers.empty();
        }

        void MergeOutOfOrder(TStep* sx);
        void MergeToOutOfOrder(TStep* sx, TVector<TTx>&& update);
    };

    struct TStepEntry {
        const TStepId Step;
        TIntrusiveList<TTabletEntry::TStep> TabletSteps;

        explicit TStepEntry(TStepId step)
            : Step(step)
        {}
    };

    struct TGranularWatcher : public TIntrusiveListItem<TGranularWatcher> {
        const TActorId ActorId;
        ui64 SubscriptionId = 0;
        ui64 Cookie = 0;
        TGranularServer* Server = nullptr;
        TActorId SessionId;
        THashMap<TTabletEntry*, ui64> Tablets; // tablet -> frozen step (max when not frozen)
        std::unique_ptr<TEvMediatorTimecast::TEvGranularUpdate> NextUpdate;

        explicit TGranularWatcher(const TActorId& actorId)
            : ActorId(actorId)
        {}
    };

    struct TGranularServer {
        TActorId ServerId;
        TIntrusiveList<TGranularWatcher> Watchers;

        explicit TGranularServer(const TActorId& serverId)
            : ServerId(serverId)
        {}
    };

    TTabletEntry* EnsureTablet(ui64 tabletId) {
        TTabletEntry* tabletEntry = Tablets.FindPtr(tabletId);
        if (tabletEntry == nullptr) {
            auto res = Tablets.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(tabletId),
                std::forward_as_tuple(tabletId));
            tabletEntry = &res.first->second;
        }
        return tabletEntry;
    }

    void SendToTablet(TTabletEntry::TStep* tabletStep, ui64 tabletId, const TActorContext& ctx) {
        auto evx = std::make_unique<TEvTxProcessing::TEvPlanStep>(tabletStep->StepRef->Step, Mediator, tabletId);
        evx->Record.MutableTransactions()->Reserve(tabletStep->Transactions.size());
        for (const TTx& tx : tabletStep->Transactions) {
            NKikimrTx::TMediatorTransaction* x = evx->Record.AddTransactions();
            x->SetTxId(tx.TxId);
            if (tx.Moderator)
                x->SetModerator(tx.Moderator);
            ActorIdToProto(tx.AckTo, x->MutableAckTo());
            LOG_DEBUG(ctx, NKikimrServices::TX_MEDIATOR_PRIVATE, "Send from %" PRIu64 " to tablet %" PRIu64 ", step# %"
                PRIu64 ", txid# %" PRIu64 ", marker M5" PRIu64, Mediator, tabletId, tabletStep->StepRef->Step, tx.TxId);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " SEND to# " << tabletId << " " << evx->ToString());
        Pipes->Send(ctx, tabletId, evx.release());
    }

    void UpdateTimecastLag(const TActorContext& ctx) {
        if (!TimecastLagCounter) {
            TimecastLagCounter = GetServiceCounters(AppData(ctx)->Counters, "processing")
                ->GetSubgroup("mediator", ToString(Mediator))
                ->GetSubgroup("sensor", "TimecastLag")
                ->GetNamedCounter("Bucket", ToString(HashBucket));
        }

        *TimecastLagCounter = (AcceptedStep - CommitedStep);
    }

    void CheckStepHead(const TActorContext& ctx, bool newAcceptedStep = false) {
        Y_UNUSED(ctx);

        bool updateTimecast = false;
        while (!Steps.empty()) {
            auto& step = Steps.front();
            if (step.Step > AcceptedStep) {
                break; // step not yet complete
            }
            if (!step.TabletSteps.Empty()) {
                break; // step has unacknowledged transactions
            }

            CommitedStep = step.Step;
            updateTimecast = true;
            Steps.pop_front();
        }

        if (updateTimecast || (newAcceptedStep && CommitedStep > 0)) {
            UpdateTimecastLag(ctx);
        }

        if (updateTimecast) {
            TEvMediatorTimecast::TEvUpdate evx;
            evx.Record.SetMediator(Mediator);
            evx.Record.SetBucket(HashBucket);
            evx.Record.SetTimeBarrier(CommitedStep);
            TAllocChunkSerializer serializer;
            const bool success = evx.SerializeToArcadiaStream(&serializer);
            Y_ABORT_UNLESS(success);
            TIntrusivePtr<TEventSerializedData> data = serializer.Release(evx.CreateSerializationInfo());

            // todo: we must throttle delivery
            const ui32 sendFlags = IEventHandle::FlagTrackDelivery;
            for (const TActorId& x : TimecastWatches) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
                    << " Mediator# " << Mediator << " SEND to# " << x.ToString() << " " << evx.ToString());
                ctx.ExecutorThread.Send(new IEventHandle(TEvMediatorTimecast::TEvUpdate::EventType, sendFlags, x, ctx.SelfID, data, 0));
            }
        }
    }

    void Handle(TEvTxMediator::TEvCommitTabletStep::TPtr& ev, const TActorContext& ctx) {
        TEvTxMediator::TEvCommitTabletStep* msg = ev->Get();
        const TStepId step = msg->Step;
        const TTabletId tabletId = msg->TabletId;

        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString() << " marker# M4");

        TTabletEntry* tabletEntry = EnsureTablet(tabletId);
        if (!ActiveStep) {
            Y_ABORT_UNLESS(AcceptedStep < step);
            ActiveStep = &Steps.emplace_back(step);
        }

        Y_ABORT_UNLESS(ActiveStep->Step == step);

        auto res = tabletEntry->Queue.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(step),
            std::forward_as_tuple(ActiveStep));
        Y_ABORT_UNLESS(res.second);

        TTabletEntry::TStep* tabletStep = &res.first->second;
        tabletStep->Transactions.swap(msg->Transactions);
        ActiveStep->TabletSteps.PushBack(tabletStep);
        ActiveTablets.PushBack(tabletEntry);

        switch (tabletEntry->State) {
        case TTabletEntry::EState::Init:
            Pipes->Prepare(ctx, tabletId);
            tabletEntry->State = TTabletEntry::EState::Connecting;
            break;
        case TTabletEntry::EState::Connecting:
            break;
        case TTabletEntry::EState::Connected:
            SendToTablet(tabletStep, tabletId, ctx);
            break;
        }
    }

    void Handle(TEvTxMediator::TEvOoOTabletStep::TPtr &ev, const TActorContext &ctx) {
        TEvTxMediator::TEvOoOTabletStep* msg = ev->Get();
        const TStepId step = msg->Step;
        const TTabletId tabletId = msg->TabletId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        Y_ABORT_UNLESS(step <= AcceptedStep);

        TTabletEntry* tabletEntry = Tablets.FindPtr(tabletId);
        if (tabletEntry == nullptr) {
            // We don't have a tablet entry, which means no steps inflight
            AckOoO(tabletId, step, msg->Transactions, ctx);
            return;
        }

        auto it = tabletEntry->Queue.find(step);
        if (it == tabletEntry->Queue.end()) {
            // This step is already confirmed, reply immediately
            AckOoO(tabletId, step, msg->Transactions, ctx);
            return;
        }

        // Save possibly updated AckTo for later
        tabletEntry->MergeToOutOfOrder(&it->second, std::move(msg->Transactions));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        const TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        const TTabletId tabletId = msg->TabletId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        TTabletEntry* tabletEntry = Tablets.FindPtr(tabletId);
        Y_ABORT_UNLESS(tabletEntry && tabletEntry->State == TTabletEntry::EState::Connecting);

        if (!Pipes->OnConnect(ev)) {
            if (msg->Dead) {
                LOG_WARN_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
                    << " Mediator# " << Mediator << " HANDLE TEvClientConnected(Dead=true)");

                tabletEntry->State = TTabletEntry::EState::Init;
                if (!tabletEntry->Queue.empty()) {
                    auto it = tabletEntry->Queue.begin();
                    while (it != tabletEntry->Queue.end()) {
                        TTabletEntry::TStep* sx = &it->second;
                        tabletEntry->MergeOutOfOrder(sx);
                        AckOoO(tabletId, sx->StepRef->Step, sx->Transactions, ctx);
                        // Note: will also auto-remove itself from sx->StepRef->TabletSteps
                        it = tabletEntry->Queue.erase(it);
                    }
                    // Unfreeze tablet watchers that have been previously frozen
                    SendGranularTabletUpdate(tabletEntry, ctx);
                    // Acked steps may have changed committed head
                    CheckStepHead(ctx);
                }
                if (tabletEntry->Redundant()) {
                    Tablets.erase(tabletId);
                }
                return;
            }

            // Keep trying to reconnect
            Pipes->Prepare(ctx, tabletId);
            return;
        }

        tabletEntry->State = TTabletEntry::EState::Connected;
        for (auto& pr : tabletEntry->Queue) {
            TTabletEntry::TStep* sx = &pr.second;
            tabletEntry->MergeOutOfOrder(sx);
            SendToTablet(sx, tabletId, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        const TEvTabletPipe::TEvClientDestroyed* msg = ev->Get();
        const TTabletId tabletId = msg->TabletId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        Pipes->OnDisconnect(ev);

        TTabletEntry* tabletEntry = Tablets.FindPtr(tabletId);
        Y_ABORT_UNLESS(tabletEntry && tabletEntry->State == TTabletEntry::EState::Connected);

        // Keep trying to reconnect as long as the queue is not empty
        if (!tabletEntry->Queue.empty()) {
            Pipes->Prepare(ctx, tabletId);
            tabletEntry->State = TTabletEntry::EState::Connecting;
            return;
        }

        tabletEntry->State = TTabletEntry::EState::Init;
        // Remove redundant disconnected tablets
        if (tabletEntry->Redundant()) {
            Tablets.erase(tabletId);
        }
    }

    void Handle(TEvTxMediator::TEvStepPlanComplete::TPtr& ev, const TActorContext& ctx) {
        const TEvTxMediator::TEvStepPlanComplete* msg = ev->Get();
        const TStepId step = msg->Step;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        Y_ABORT_UNLESS(AcceptedStep < step);

        if (ActiveStep) {
            Y_ABORT_UNLESS(ActiveStep->Step == step);
            ActiveStep = nullptr;
        } else {
            // Create a new empty step
            Steps.emplace_back(step);
        }

        AcceptedStep = step;
        SendGranularLatestStepUpdate(ctx);
        Y_ABORT_UNLESS(ActiveTablets.Empty());
        CheckStepHead(ctx, /* newAcceptedStep */ true);
    }

    void Handle(TEvTxProcessing::TEvPlanStepAccepted::TPtr& ev, const TActorContext& ctx) {
        const NKikimrTx::TEvPlanStepAccepted& record = ev->Get()->Record;
        const TTabletId tabletId = record.GetTabletId();
        const TStepId step = record.GetStep();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << ev->Get()->ToString());

        TTabletEntry* tabletEntry = Tablets.FindPtr(tabletId);
        if (!tabletEntry) {
            return;
        }

        auto it = tabletEntry->Queue.find(step);
        if (it == tabletEntry->Queue.end()) {
            return;
        }

        if (it != tabletEntry->Queue.begin()) {
            // ignore confirmations for non-head steps
            // TODO: treat them as confirmation of all preceding steps?
            return;
        }

        TTabletEntry::TStep* sx = &it->second;
        if (!sx->OutOfOrder.empty()) {
            // Confirm out-of-order requests
            AckOoO(tabletId, step, sx->OutOfOrder, ctx);
        }
        // Note: will also auto-remove itself from sx->StepRef->TabletSteps
        tabletEntry->Queue.erase(it);

        // Tablet's frozen step may have changed, update watchers
        SendGranularTabletUpdate(tabletEntry, ctx);

        CheckStepHead(ctx);
    }

    void Handle(TEvTxMediator::TEvWatchBucket::TPtr& ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << ev->Get()->ToString());
        const TActorId& source = ev->Get()->Source;
        TimecastWatches.insert(source);

        TEvMediatorTimecast::TEvUpdate evx(Mediator, HashBucket, CommitedStep);
        TAllocChunkSerializer serializer;
        const bool success = evx.SerializeToArcadiaStream(&serializer);
        Y_ABORT_UNLESS(success);
        TIntrusivePtr<TEventSerializedData> data = serializer.Release(evx.CreateSerializationInfo());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " SEND to# " << source.ToString() << " " << evx.ToString());
        const ui32 sendFlags = IEventHandle::FlagTrackDelivery;
        ctx.ExecutorThread.Send(new IEventHandle(TEvMediatorTimecast::TEvUpdate::EventType, sendFlags, source, ctx.SelfID, data, 0));
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE TEvUndelivered");
        // for now every non-delivery is reason to drop watch
        TimecastWatches.erase(ev->Sender);
    }

    void AckOoO(TTabletId tablet, TStepId step, const TVector<TTx>& transactions, const TActorContext& ctx) {
        TMap<TActorId, std::unique_ptr<TEvTxProcessing::TEvPlanStepAck>> acks;
        for (const TTx &tx : transactions) {
            auto& ack = acks[tx.AckTo];
            if (!ack) {
                ack.reset(new TEvTxProcessing::TEvPlanStepAck(tablet, step, (const ui64 *)nullptr, (const ui64 *)nullptr));
            }
            ack->Record.AddTxId(tx.TxId);
        }

        for (auto& x : acks) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
                << " Mediator# " << Mediator << " SEND to# " << x.first.ToString() << " " << x.second->ToString());
            ctx.Send(x.first, x.second.release());
        }
    }

    TGranularWatcher* EnsureGranularWatcher(const TActorId& actorId) {
        auto it = GranularWatchers.find(actorId);
        if (it == GranularWatchers.end()) {
            auto res = GranularWatchers.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(actorId),
                std::forward_as_tuple(actorId));
            Y_ABORT_UNLESS(res.second);
            it = res.first;
        }
        return &it->second;
    }

    TGranularServer* EnsureGranularServer(const TActorId& serverId) {
        auto it = GranularServers.find(serverId);
        if (it == GranularServers.end()) {
            auto res = GranularServers.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(serverId),
                std::forward_as_tuple(serverId));
            Y_ABORT_UNLESS(res.second);
            it = res.first;
        }
        return &it->second;
    }

    TStepId GetFrozenStep(TTabletEntry* tabletEntry) const {
        TStepId frozenStep = Max<ui64>();
        if (!tabletEntry->Queue.empty()) {
            TStepId nextStep = tabletEntry->Queue.begin()->second.StepRef->Step;
            // Note: when the next step is activating freezes will be sent later
            if (nextStep <= AcceptedStep) {
                frozenStep = nextStep - 1;
            }
        }

        return frozenStep;
    }

    void AddGranularWatcherTablet(TGranularWatcher* watcher, ui64 tabletId,
            NKikimrTxMediatorTimecast::TEvGranularUpdate& record)
    {
        TTabletEntry* tabletEntry = EnsureTablet(tabletId);

        const TStepId frozenStep = GetFrozenStep(tabletEntry);

        watcher->Tablets[tabletEntry] = frozenStep;
        tabletEntry->Watchers.insert(watcher);

        if (frozenStep != Max<ui64>()) {
            record.AddFrozenTablets(tabletId);
            record.AddFrozenSteps(frozenStep);
        }
    }

    void SendGranularUpdate(TGranularWatcher* watcher,
            std::unique_ptr<TEvMediatorTimecast::TEvGranularUpdate>&& response,
            const TActorContext& ctx)
    {
        // Note: we don't use delivery flags and rely on pipe server notification instead
        auto ev = std::make_unique<IEventHandle>(watcher->ActorId, ctx.SelfID, response.release(), 0, watcher->Cookie);

        // We have a stateful protocol and must ensure messages are not skipped unexpectedly
        // Send it using the same session initial subscription was received from
        if (watcher->SessionId) {
            ev->Rewrite(TEvInterconnect::EvForward, watcher->SessionId);
        }

        ctx.ExecutorThread.Send(ev.release());
    }

    void SendGranularLatestStepUpdate(const TActorContext& ctx) {
        // We are going to send the new AcceptedStep to all currently known granular watchers
        for (auto& pr : GranularWatchers) {
            TGranularWatcher* watcher = &pr.second;
            Y_ABORT_UNLESS(!watcher->NextUpdate);
            watcher->NextUpdate.reset(new TEvMediatorTimecast::TEvGranularUpdate(Mediator, HashBucket, watcher->SubscriptionId));
            watcher->NextUpdate->Record.SetLatestStep(AcceptedStep);
        }

        // Walk tablets that have steps pending at the new accepted step
        // These tablets become frozen at the previous step
        const TStepId frozenStep = AcceptedStep - 1;
        while (!ActiveTablets.Empty()) {
            TTabletEntry* tabletEntry = ActiveTablets.PopFront();
            if (!tabletEntry->Queue.empty() && tabletEntry->Queue.begin()->first == AcceptedStep) {
                // This tablet is now frozen at the previous step
                // Notify all watchers that are watching this tablet
                // Note: watchers watch tablets that are running at their node,
                // so there is usually only one watcher per running tablet.
                for (TGranularWatcher* watcher : tabletEntry->Watchers) {
                    auto it = watcher->Tablets.find(tabletEntry);
                    Y_ABORT_UNLESS(it != watcher->Tablets.end());
                    // Since this tablet became frozen just now it must not be frozen in the watcher
                    Y_ABORT_UNLESS(it->second == Max<ui64>());
                    it->second = frozenStep;
                    Y_ABORT_UNLESS(watcher->NextUpdate);
                    watcher->NextUpdate->Record.AddFrozenTablets(tabletEntry->TabletId);
                    watcher->NextUpdate->Record.AddFrozenSteps(frozenStep);
                }
            }
        }

        // Send the final update to all watchers
        for (auto& pr : GranularWatchers) {
            TGranularWatcher* watcher = &pr.second;
            Y_ABORT_UNLESS(watcher->NextUpdate);
            SendGranularUpdate(watcher, std::move(watcher->NextUpdate), ctx);
        }
    }

    void SendGranularTabletUpdate(TTabletEntry* tabletEntry, const TActorContext& ctx) {
        if (tabletEntry->Watchers.empty()) {
            return;
        }

        // Tablet has just confirmed one or more steps
        // Check whether the frozen step for this tablet changed
        const TStepId frozenStep = GetFrozenStep(tabletEntry);

        // Note: watchers watch tablets that are running at their node, so
        // there is usually only one watcher per running tablet.
        for (TGranularWatcher* watcher : tabletEntry->Watchers) {
            auto it = watcher->Tablets.find(tabletEntry);
            Y_ABORT_UNLESS(it != watcher->Tablets.end());
            // Note: frozen step is always less than the current AcceptedStep.
            // In case we don't have any complete steps yet (AcceptedStep==0)
            // none of the tablets have ever been frozen, and we never reported
            // this tablet as frozen before.
            if (it->second != frozenStep) {
                auto response = std::make_unique<TEvMediatorTimecast::TEvGranularUpdate>(Mediator, HashBucket, watcher->SubscriptionId);
                response->Record.SetLatestStep(AcceptedStep);
                if (frozenStep != Max<ui64>()) {
                    // An updated frozen step for this tablet
                    response->Record.AddFrozenTablets(tabletEntry->TabletId);
                    response->Record.AddFrozenSteps(frozenStep);
                } else {
                    // This tablet is now unfrozen
                    response->Record.AddUnfrozenTablets(tabletEntry->TabletId);
                }
                SendGranularUpdate(watcher, std::move(response), ctx);
                it->second = frozenStep;
            }
        }
    }

    void Handle(TEvMediatorTimecast::TEvGranularWatch::TPtr &ev, const TActorContext &ctx) {
        const auto* msg = ev->Get();
        const ui64 subscriptionId = msg->Record.GetSubscriptionId();
        if (subscriptionId == 0) {
            // Sanity check: ignore an empty subscriptionId
            return;
        }

        TGranularWatcher* watcher = EnsureGranularWatcher(ev->Sender);
        if (subscriptionId <= watcher->SubscriptionId) {
            // Ignore out-of-order messages: subscription id must increase
            return;
        }

        TGranularServer* server = EnsureGranularServer(ev->Recipient);
        if (watcher->Server != server) {
            // Unlink from the old server
            if (watcher->Server) {
                watcher->Server->Watchers.Remove(watcher);
                watcher->Server = nullptr;
            }
            // Link to the new server
            server->Watchers.PushBack(watcher);
            watcher->Server = server;
        }

        // This is a new subscription, forget old tablets
        for (auto& kv : watcher->Tablets) {
            TTabletEntry* tabletEntry = kv.first;
            tabletEntry->Watchers.erase(watcher);
            if (tabletEntry->Redundant()) {
                ui64 tabletId = tabletEntry->TabletId;
                Tablets.erase(tabletId);
            }
        }
        watcher->Tablets.clear();

        watcher->SubscriptionId = subscriptionId;
        watcher->SessionId = ev->InterconnectSession;
        watcher->Cookie = ev->Cookie;

        auto response = std::make_unique<TEvMediatorTimecast::TEvGranularUpdate>(Mediator, HashBucket, subscriptionId);
        response->Record.SetLatestStep(AcceptedStep);

        // Process new tablet subscriptions
        for (ui64 tabletId : msg->Record.GetTablets()) {
            AddGranularWatcherTablet(watcher, tabletId, response->Record);
        }

        SendGranularUpdate(watcher, std::move(response), ctx);
    }

    void Handle(TEvMediatorTimecast::TEvGranularWatchModify::TPtr &ev, const TActorContext &ctx) {
        auto it = GranularWatchers.find(ev->Sender);
        if (it == GranularWatchers.end()) {
            return;
        }

        TGranularWatcher* watcher = &it->second;

        const auto* msg = ev->Get();
        const ui64 subscriptionId = msg->Record.GetSubscriptionId();

        if (subscriptionId < watcher->SubscriptionId) {
            // Ignore outdated messages
            return;
        }

        watcher->SubscriptionId = subscriptionId;

        auto response = std::make_unique<TEvMediatorTimecast::TEvGranularUpdate>(Mediator, HashBucket, subscriptionId);
        response->Record.SetLatestStep(AcceptedStep);

        for (ui64 tabletId : msg->Record.GetRemoveTablets()) {
            TTabletEntry* tabletEntry = Tablets.FindPtr(tabletId);
            if (Y_LIKELY(tabletEntry)) {
                watcher->Tablets.erase(tabletEntry);
                tabletEntry->Watchers.erase(watcher);
                if (tabletEntry->Redundant()) {
                    Tablets.erase(tabletId);
                }
            }
        }

        for (ui64 tabletId : msg->Record.GetAddTablets()) {
            AddGranularWatcherTablet(watcher, tabletId, response->Record);
        }

        SendGranularUpdate(watcher, std::move(response), ctx);
    }

    void Handle(TEvTxMediator::TEvServerDisconnected::TPtr &ev, const TActorContext&) {
        auto* msg = ev->Get();

        auto it = GranularServers.find(msg->ServerId);
        if (it == GranularServers.end()) {
            return;
        }

        TGranularServer* server = &it->second;
        while (!server->Watchers.Empty()) {
            TGranularWatcher* watcher = server->Watchers.PopFront();
            watcher->Server = nullptr;
            for (auto& kv : watcher->Tablets) {
                TTabletEntry* tabletEntry = kv.first;
                tabletEntry->Watchers.erase(watcher);
                if (tabletEntry->Redundant()) {
                    ui64 tabletId = tabletEntry->TabletId;
                    Tablets.erase(tabletId);
                }
            }
            TActorId watcherId = watcher->ActorId;
            GranularWatchers.erase(watcherId);
        }

        GranularServers.erase(it);
    }

    void Die(const TActorContext &ctx) override {
        Pipes->Detach(ctx);
        Pipes.reset();

        TActor::Die(ctx);
    }

    static NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig config;
        config.CheckAliveness = true;
        config.RetryPolicy = {
            .RetryLimitCount = 30,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(500),
            .BackoffMultiplier = 2,
        };
        return config;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_MEDIATOR_TABLET_QUEUE_ACTOR;
    }

    TTxMediatorTabletQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui64 hashBucket)
        : TActor(&TThis::StateFunc)
        , Owner(owner)
        , Mediator(mediator)
        , HashRange(hashRange)
        , HashBucket(hashBucket)
        , Pipes(NTabletPipe::CreateUnboundedClientCache(GetPipeClientConfig()))
    {
       Y_UNUSED(HashRange);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProcessing::TEvPlanStepAccepted, Handle);
            HFunc(TEvTxMediator::TEvCommitTabletStep, Handle);
            HFunc(TEvTxMediator::TEvStepPlanComplete, Handle);
            HFunc(TEvTxMediator::TEvOoOTabletStep, Handle);
            HFunc(TEvTxMediator::TEvWatchBucket, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvMediatorTimecast::TEvGranularWatch, Handle);
            HFunc(TEvMediatorTimecast::TEvGranularWatchModify, Handle);
            HFunc(TEvTxMediator::TEvServerDisconnected, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

private:
    const TActorId Owner;
    const ui64 Mediator;
    const ui64 HashRange;
    const ui64 HashBucket;

    std::unique_ptr<NTabletPipe::IClientCache> Pipes;

    TList<TStepEntry> Steps;
    THashMap<TTabletId, TTabletEntry> Tablets;

    TStepId AcceptedStep = 0;
    TStepId CommitedStep = 0;
    TStepEntry* ActiveStep = nullptr;
    TIntrusiveList<TTabletEntry> ActiveTablets;

    THashSet<TActorId> TimecastWatches;
    ::NMonitoring::TDynamicCounters::TCounterPtr TimecastLagCounter;

    THashMap<TActorId, TGranularWatcher> GranularWatchers;
    THashMap<TActorId, TGranularServer> GranularServers;
};

/**
 * Returns true when all transactions of x are present in superset
 */
static bool IsSubsetOf(const TVector<TTx>& x, const TVector<TTx>& superset) {
    auto it = x.begin();
    auto itSuperset = superset.begin();
    while (it != x.end()) {
        // Position superset to the lowerbound of the current TxId
        while (itSuperset != superset.end() && itSuperset->TxId < it->TxId) {
            ++itSuperset;
        }
        if (itSuperset == superset.end() || it->TxId != itSuperset->TxId) {
            return false;
        }
        ++it;
        ++itSuperset;
    }
    return true;
}

static TString DumpTxIds(const TVector<TTx>& v) {
    TStringBuilder stream;
    stream << '{';
    for (auto it = v.begin(); it != v.end(); ++it) {
        if (it != v.begin()) {
            stream << ", ";
        }
        stream << it->TxId;
    }
    stream << '}';
    return std::move(stream);
}

void TTxMediatorTabletQueue::TTabletEntry::MergeOutOfOrder(TStep* sx) {
    if (!sx->OutOfOrder.empty()) {
        // Since OutOfOrder is an update it might be missing some lost or
        // already acknowledged transactions that we don't have to resend.
        // We have previously validated that OutOfOrder ⊂ Transactions
        sx->Transactions = std::move(sx->OutOfOrder);
    }
}

void TTxMediatorTabletQueue::TTabletEntry::MergeToOutOfOrder(TStep* sx, TVector<TTx>&& update) {
    // Update might be missing some lost or already acknowledged transactions
    // that we don't have to resend later. We validate that update is a subset
    // of a previously received step.
    const TVector<TTx>& prev = sx->OutOfOrder.empty() ? sx->Transactions : sx->OutOfOrder;
    if (Y_UNLIKELY(!IsSubsetOf(update, prev))) {
        // Coordinator shouldn't add new transaction to existing steps, so we
        // complain. However, even if that happens, it's ok for us to send
        // those transactions later, or never.
        LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_MEDIATOR_TABLETQUEUE,
            "Received out-of-order step " << sx->StepRef->Step
            << " for tablet " << TabletId
            << " with transactions " << DumpTxIds(update)
            << " which are not a subset of previously received " << DumpTxIds(prev));
    }
    sx->OutOfOrder = std::move(update);
}

}

IActor* CreateTxMediatorTabletQueue(const TActorId& owner, ui64 mediator, ui64 hashRange, ui64 hashBucket) {
    return new NTxMediator::TTxMediatorTabletQueue(owner, mediator, hashRange, hashBucket);
}

}
