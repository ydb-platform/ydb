#include "statestorage_impl.h"
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/generic/set.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOARD_REPLICA, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOARD_REPLICA, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BOARD_REPLICA, stream)

namespace NKikimr {

class TBoardReplicaActor : public TActor<TBoardReplicaActor> {

    using TOwnerIndex = TMap<TActorId, ui32, TActorId::TOrderedCmp>;
    using TPathIndex = TMap<TString, TSet<ui32>>;

    struct TEntry {
        TString Payload;
        TActorId Owner;
        TOwnerIndex::iterator OwnerIt;
        TPathIndex::iterator PathIt;
        TActorId Session = TActorId();
    };

    struct TPathSubscribeData {
        THashMap<TActorId, ui64> Subscribers; // Subcriber -> Cookie
    };

    struct TSubscriber {
        TString Path;
        TActorId Session = TActorId();
    };

    TVector<TEntry> Entries;
    TVector<ui32> AvailableEntries;

    THashMap<TActorId, TSubscriber> Subscribers;
    TMap<TString, TPathSubscribeData> PathToSubscribers;

    struct TSessionSubscribers {
        THashSet<TActorId> Subscribers;
        THashSet<TActorId> Publishers;

        bool Empty() const {
            return Subscribers.empty() && Publishers.empty();
        }
    };

    THashMap<TActorId, TSessionSubscribers> Sessions; // InterconnectSession -> Session subscribers

    TOwnerIndex IndexOwner;
    TPathIndex IndexPath;

    ui32 AllocateEntry() {
        ui32 ret;
        if (AvailableEntries) {
            ret = AvailableEntries.back();
            AvailableEntries.pop_back();
        }
        else {
            ret = Entries.size();
            Entries.emplace_back();
        }

        return ret;
    }

    void Handle(TEvStateStorage::TEvReplicaBoardPublish::TPtr &ev) {
        auto &record = ev->Get()->Record;
        const TString &path = record.GetPath();
        const TActorId &owner = ev->Sender;

        if (!record.GetRegister()) {
            BLOG_ERROR("free floating entries not implemented yet");
            return;
        }

        auto pathSubscribeDataIt = PathToSubscribers.find(path);

        auto ownerIt = IndexOwner.find(owner);
        if (ownerIt != IndexOwner.end()) {
            const ui32 entryIndex = ownerIt->second;
            TEntry &entry = Entries[entryIndex];
            if (entry.PathIt->first != path) {
                BLOG_ERROR("unconsistent path for same owner");
                // reply nothing, request suspicious
                return;
            }

            if (ev->InterconnectSession) {
                entry.Session = ev->InterconnectSession;
            }
            entry.Payload = record.GetPayload();
            Y_DEBUG_ABORT_UNLESS(entry.Owner == ActorIdFromProto(record.GetOwner()));

            if (pathSubscribeDataIt != PathToSubscribers.end()) {
                SendUpdateToSubscribers(entry, false);
            }

        } else {
            const ui32 entryIndex = AllocateEntry();
            TEntry &entry = Entries[entryIndex];

            entry.Payload = record.GetPayload();
            entry.Owner = ActorIdFromProto(record.GetOwner());
            if (ev->InterconnectSession) {
                entry.Session = ev->InterconnectSession;
            }

            auto ownerInsPairIt = IndexOwner.emplace(owner, entryIndex);
            entry.OwnerIt = ownerInsPairIt.first;
            auto pathInsPairIt = IndexPath.emplace(std::make_pair(path, TSet<ui32>()));
            entry.PathIt = pathInsPairIt.first;
            entry.PathIt->second.emplace(entryIndex);

            if (pathSubscribeDataIt != PathToSubscribers.end()) {
                SendUpdateToSubscribers(entry, false);
            }
        }

        if (ev->InterconnectSession) {
            auto sessionsIt = Sessions.find(ev->InterconnectSession);
            if (sessionsIt == Sessions.end()) {
                Send(ev->InterconnectSession, new TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);
                Sessions[ev->InterconnectSession].Publishers.insert(ev->Sender);
            } else {
                sessionsIt->second.Publishers.insert(ev->Sender);
            }
        }

        auto reply = std::make_unique<TEvStateStorage::TEvReplicaBoardPublishAck>();
        auto resp = std::make_unique<IEventHandle>(
            owner, SelfId(), reply.release(),
            IEventHandle::FlagTrackDelivery, ev->Cookie);

        if (ev->InterconnectSession) {
            resp->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
        }
        TActivationContext::Send(resp.release());
    }

    void CleanupEntry(ui32 entryIndex, TActorId session) {
        TEntry &entry = Entries[entryIndex];

        entry.PathIt->second.erase(entryIndex);
        if (entry.PathIt->second.empty()) {
            IndexPath.erase(entry.PathIt);
        }

        CleanupSessionForPublisher(session, entry.OwnerIt->first);

        IndexOwner.erase(entry.OwnerIt);

        TString().swap(entry.Payload);
        entry.Owner = TActorId();
        entry.PathIt = IndexPath.end();
        entry.OwnerIt = IndexOwner.end();
        entry.Session = TActorId();

        AvailableEntries.emplace_back(entryIndex);
    }

    void CleanupSubscriber(const TActorId& subscriber, TActorId session) {
        auto subscriberIt = Subscribers.find(subscriber);
        if (subscriberIt == Subscribers.end()) {
            return;
        }

        CleanupSessionForSubscriber(session, subscriber);

        auto& pathSubscribeData = PathToSubscribers[subscriberIt->second.Path];
        pathSubscribeData.Subscribers.erase(subscriberIt->first);
        if (pathSubscribeData.Subscribers.empty()) {
            PathToSubscribers.erase(subscriberIt->second.Path);
        }
        Subscribers.erase(subscriberIt);
    }


    void CleanupSessionForSubscriber(const TActorId& session, const TActorId& subscriber) {
        if (!session) {
            return;
        }
        auto sessionsIt = Sessions.find(session);
        if (sessionsIt != Sessions.end()) {
            sessionsIt->second.Subscribers.erase(subscriber);
            if (sessionsIt->second.Empty()) {
                Send(sessionsIt->first, new TEvents::TEvUnsubscribe());
                Sessions.erase(sessionsIt);
            }
        }
    }

    void CleanupSessionForPublisher(const TActorId& session, const TActorId& publisher) {
        if (!session) {
            return;
        }
        auto sessionsIt = Sessions.find(session);
        if (sessionsIt != Sessions.end()) {
            sessionsIt->second.Publishers.erase(publisher);
            if (sessionsIt->second.Empty()) {
                Send(sessionsIt->first, new TEvents::TEvUnsubscribe());
                Sessions.erase(sessionsIt);
            }
        }
    }

    void PassAway() override {

        for (const auto& [session, sessionSubscribers] : Sessions) {
            Send(session, new TEvents::TEvUnsubscribe());
        }

        for (const auto& xpair : IndexOwner) {
            Send(xpair.first, new TEvStateStorage::TEvReplicaShutdown());
        }

        for (const auto& [path, pathSubscribeData] : PathToSubscribers) {
            for (const auto& [subscriber, cookie] : pathSubscribeData.Subscribers) {
                auto reply = MakeHolder<TEvStateStorage::TEvReplicaShutdown>();
                Send(subscriber, std::move(reply), 0, cookie);
            }
        }

        // all cleanup in actor destructor
        TActor::PassAway();
    }

    void Handle(TEvStateStorage::TEvReplicaBoardCleanup::TPtr &ev) {
        auto ownerIt = IndexOwner.find(ev->Sender);
        if (ownerIt == IndexOwner.end()) // do nothing, already removed?
            return;

        const auto& entry = Entries[ownerIt->second];
        const auto& path = entry.PathIt->first;
        auto pathSubscribeDataIt = PathToSubscribers.find(path);
        if (pathSubscribeDataIt != PathToSubscribers.end()) {
            SendUpdateToSubscribers(entry, true);
        }

        CleanupEntry(ownerIt->second, ev->InterconnectSession);
    }

    void Handle(TEvStateStorage::TEvReplicaBoardLookup::TPtr &ev) {
        auto &record = ev->Get()->Record;
        const auto &path = record.GetPath();

        ui32 flags = 0;
        if (record.GetSubscribe()) {
            auto& pathSubscribeData  = PathToSubscribers[path];
            pathSubscribeData.Subscribers[ev->Sender] = ev->Cookie;
            auto& subscriber = Subscribers[ev->Sender];
            subscriber.Path = path;
            if (ev->InterconnectSession) {
                subscriber.Session = ev->InterconnectSession;
                auto sessionsIt = Sessions.find(ev->InterconnectSession);
                if (sessionsIt == Sessions.end()) {
                    Send(ev->InterconnectSession, new TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);
                    Sessions[ev->InterconnectSession].Subscribers.insert(ev->Sender);
                } else {
                    sessionsIt->second.Subscribers.insert(ev->Sender);
                }
            }
            flags = IEventHandle::FlagTrackDelivery;
        }

        std::unique_ptr<TEvStateStorage::TEvReplicaBoardInfo> reply;

        auto pathIt = IndexPath.find(path);
        if (pathIt == IndexPath.end()) {
            reply = std::make_unique<TEvStateStorage::TEvReplicaBoardInfo>(path, true);
        } else {
            reply = std::make_unique<TEvStateStorage::TEvReplicaBoardInfo>(path, false);
            auto *info = reply->Record.MutableInfo();
            info->Reserve(pathIt->second.size());
            for (ui32 entryIndex : pathIt->second) {
                const TEntry &entry = Entries[entryIndex];
                auto *ex = info->Add();
                ActorIdToProto(entry.Owner, ex->MutableOwner());
                ex->SetPayload(entry.Payload);
            }
        }

        auto resp = std::make_unique<IEventHandle>(
            ev->Sender, SelfId(), reply.release(), flags, ev->Cookie);
        if (ev->InterconnectSession) {
            resp->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
        }
        TActivationContext::Send(resp.release());
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        auto *msg = ev->Get();
        switch (msg->SourceType) {
            case TEvents::TEvSubscribe::EventType: {
                DisconnectSession(ev->Sender);
                return;
            }
            case TEvStateStorage::TEvReplicaBoardInfo::EventType: {
                auto subscribersIt = Subscribers.find(ev->Sender);
                if (subscribersIt == Subscribers.end()) {
                    return;
                }
                if (subscribersIt->second.Session != ev->InterconnectSession) {
                    return;
                }
                CleanupSubscriber(ev->Sender, ev->InterconnectSession);
                break;
            }
            case TEvStateStorage::TEvReplicaBoardPublishAck::EventType: {
                auto ownerIt = IndexOwner.find(ev->Sender);
                if (ownerIt == IndexOwner.end())
                    return;

                const auto& entry = Entries[ownerIt->second];
                if (entry.Session != ev->InterconnectSession) {
                    return;
                }
                const auto& path = entry.PathIt->first;
                auto pathSubscribeDataIt = PathToSubscribers.find(path);
                if (pathSubscribeDataIt != PathToSubscribers.end()) {
                    SendUpdateToSubscribers(entry, true);
                }

                CleanupEntry(ownerIt->second, ev->InterconnectSession);
                break;
            }
            default:
                Y_ABORT("Unexpected case");
        }
    }

    void SendUpdateToSubscribers(const TEntry& entry, bool dropped ) {
        const auto& path = entry.PathIt->first;

        auto pathSubscribeDataIt = PathToSubscribers.find(path);
        if (pathSubscribeDataIt == PathToSubscribers.end()) {
            return;
        }

        auto& pathSubscribeData = pathSubscribeDataIt->second;

        NKikimrStateStorage::TEvReplicaBoardInfoUpdate record;
        auto *info = record.MutableInfo();
        ActorIdToProto(entry.Owner, info->MutableOwner());
        if (dropped) {
            info->SetDropped(true);
        } else {
            info->SetPayload(entry.Payload);
        }

        for (const auto& subscriber : pathSubscribeData.Subscribers) {
            auto reply = MakeHolder<TEvStateStorage::TEvReplicaBoardInfoUpdate>(path);
            reply->Record = record;
            Send(subscriber.first, std::move(reply), 0, subscriber.second);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        DisconnectSession(ev->Sender);
    }

    void Handle(TEvStateStorage::TEvReplicaBoardUnsubscribe::TPtr& ev) {
        const auto& sender = ev->Sender;

        CleanupSubscriber(sender, ev->InterconnectSession);
    }

    void DisconnectSession(const TActorId& session) {
        auto sessionsIt = Sessions.find(session);
        if (sessionsIt == Sessions.end()) {
            return;
        }

        for (const auto& subscriber : sessionsIt->second.Subscribers) {
            auto subscribersIt = Subscribers.find(subscriber);
            if (subscribersIt == Subscribers.end()) {
                continue;
            }
            if (subscribersIt->second.Session == session) {
                CleanupSubscriber(subscriber, TActorId());
            }
        }

        for (const auto& publisher : sessionsIt->second.Publishers) {
            auto ownerIt = IndexOwner.find(publisher);
            if (ownerIt == IndexOwner.end()) {
                continue;
            }

            const auto& entry = Entries[ownerIt->second];

            if (entry.Session != session) {
                continue;
            }
            const auto& path = entry.PathIt->first;
            auto pathSubscribeDataIt = PathToSubscribers.find(path);
            if (pathSubscribeDataIt != PathToSubscribers.end()) {
                SendUpdateToSubscribers(entry, true);
            }

            CleanupEntry(ownerIt->second, TActorId());
        }

        Sessions.erase(sessionsIt);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BOARD_REPLICA_ACTOR;
    }

    TBoardReplicaActor()
        : TActor(&TThis::StateWork)
    {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaBoardPublish, Handle);
            hFunc(TEvStateStorage::TEvReplicaBoardCleanup, Handle);
            hFunc(TEvStateStorage::TEvReplicaBoardLookup, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvStateStorage::TEvReplicaBoardUnsubscribe, Handle);

            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        default:
            // in debug spam some message
            break;
        }
    }
};

IActor* CreateStateStorageBoardReplica(const TIntrusivePtr<TStateStorageInfo> &, ui32) {
    return new TBoardReplicaActor();
}

}
