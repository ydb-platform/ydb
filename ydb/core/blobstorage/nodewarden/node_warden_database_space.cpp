#include "node_warden_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

using namespace NKikimr;
using namespace NStorage;

namespace {

    void SendState(const TActorId& sender, const TActorId& subscriber,
            const NKikimrBlobStorage::TEvControllerDatabaseSpaceState& state) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerDatabaseSpaceState>();
        ev->Record.CopyFrom(state);
        TActivationContext::Send(new IEventHandle(subscriber, sender, ev.release(), IEventHandle::FlagTrackDelivery));
    }

} // anonymous

void TNodeWarden::OnRegisteredAtController() {
    RegisteredAtController = true;

    // BSC drops node's subscriptions on registration, so we report the full set here
    if (!DatabaseSpaceSubscriptions.empty()) {
        auto request = std::make_unique<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>();
        for (const auto& [scope, subscription] : DatabaseSpaceSubscriptions) {
            scope.ToProto(request->Record.AddSubscribe());
        }
        SendDatabaseSpaceRequest(std::move(request));
    }
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerSubscribeDatabaseSpace::TPtr ev) {
    // this is a request from a local actor
    const auto& record = ev->Get()->Record;
    YDB_LOG_DEBUG("TEvControllerSubscribeDatabaseSpace",
        {"marker", "NW120"},
        {"sender", ev->Sender},
        {"record", record});

    auto request = std::make_unique<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>();

    for (const auto& pb : record.GetSubscribe()) {
        const TPathId scope = TPathId::FromProto(pb);
        const auto [it, inserted] = DatabaseSpaceSubscriptions.try_emplace(scope);
        TDatabaseSpaceSubscription& subscription = it->second;
        subscription.Subscribers.insert(ev->Sender);
        if (inserted) {
            scope.ToProto(request->Record.AddSubscribe());
        } else if (subscription.Last) {
            SendState(SelfId(), ev->Sender, *subscription.Last);
        }
    }

    for (const auto& pb : record.GetUnsubscribe()) {
        RemoveDatabaseSpaceSubscriber(ev->Sender, TPathId::FromProto(pb), &request->Record);
    }

    SendDatabaseSpaceRequest(std::move(request));
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerDatabaseSpaceState::TPtr ev) {
    // this is a notification from BSC
    auto& record = ev->Get()->Record;
    YDB_LOG_DEBUG("TEvControllerDatabaseSpaceState",
        {"marker", "NW121"},
        {"record", record});

    const auto it = DatabaseSpaceSubscriptions.find(TPathId::FromProto(record.GetScope()));
    if (it == DatabaseSpaceSubscriptions.end()) {
        return; // nobody is interested anymore
    }
    TDatabaseSpaceSubscription& subscription = it->second;
    subscription.Last.emplace(std::move(record));
    for (const TActorId& subscriber : subscription.Subscribers) {
        SendState(SelfId(), subscriber, *subscription.Last);
    }
}

void TNodeWarden::Handle(TEvents::TEvUndelivered::TPtr ev) {
    if (ev->Get()->SourceType != TEvBlobStorage::EvControllerDatabaseSpaceState) {
        return;
    }

    // the subscriber has gone, drop it from every subscription
    auto request = std::make_unique<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>();
    std::vector<TPathId> scopes;
    for (const auto& [scope, subscription] : DatabaseSpaceSubscriptions) {
        if (subscription.Subscribers.contains(ev->Sender)) {
            scopes.push_back(scope);
        }
    }
    for (const auto& scope : scopes) {
        RemoveDatabaseSpaceSubscriber(ev->Sender, scope, &request->Record);
    }
    SendDatabaseSpaceRequest(std::move(request));
}

void TNodeWarden::RemoveDatabaseSpaceSubscriber(const TActorId& subscriber, TPathId scope,
        NKikimrBlobStorage::TEvControllerSubscribeDatabaseSpace *request) {
    const auto it = DatabaseSpaceSubscriptions.find(scope);
    if (it == DatabaseSpaceSubscriptions.end()) {
        return;
    }
    it->second.Subscribers.erase(subscriber);
    if (it->second.Subscribers.empty()) {
        DatabaseSpaceSubscriptions.erase(it);
        scope.ToProto(request->AddUnsubscribe());
    }
}

void TNodeWarden::SendDatabaseSpaceRequest(std::unique_ptr<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace> request) {
    // until the registration is confirmed, the changes are kept locally; the full set is sent on confirmation
    if (RegisteredAtController && (request->Record.SubscribeSize() || request->Record.UnsubscribeSize())) {
        SendToController(std::move(request));
    }
}
