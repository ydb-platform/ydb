#include "services.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/set.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

class TExampleReplicaActor : public TActor<TExampleReplicaActor> {
    using TOwnerIndex = TMap<TActorId, ui32, TActorId::TOrderedCmp>;
    using TKeyIndex = THashMap<TString, TSet<ui32>>;

    struct TEntry {
        TString Payload;
        TActorId Owner;
        TOwnerIndex::iterator OwnerIt;
        TKeyIndex::iterator KeyIt;
    };

    TVector<TEntry> Entries;
    TVector<ui32> AvailableEntries;

    TOwnerIndex IndexOwner;
    TKeyIndex IndexKey;

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

    bool IsLastEntryOnNode(TOwnerIndex::iterator ownerIt) {
        const ui32 ownerNodeId = ownerIt->first.NodeId();
        if (ownerIt != IndexOwner.begin()) {
            auto x = ownerIt;
            --x;
            if (x->first.NodeId() == ownerNodeId)
                return false;
        }

        ++ownerIt;
        if (ownerIt != IndexOwner.end()) {
            if (ownerIt->first.NodeId() == ownerNodeId)
                return false;
        }

        return true;
    }

    void CleanupEntry(ui32 entryIndex) {
        TEntry &entry = Entries[entryIndex];
        entry.KeyIt->second.erase(entryIndex);
        if (entry.KeyIt->second.empty())
            IndexKey.erase(entry.KeyIt);

        if (IsLastEntryOnNode(entry.OwnerIt))
            Send(TlsActivationContext->ExecutorThread.ActorSystem->InterconnectProxy(entry.OwnerIt->first.NodeId()), new TEvents::TEvUnsubscribe());

        IndexOwner.erase(entry.OwnerIt);

        TString().swap(entry.Payload);
        entry.Owner = TActorId();
        entry.KeyIt = IndexKey.end();
        entry.OwnerIt = IndexOwner.end();

        AvailableEntries.emplace_back(entryIndex);
    }

    void Handle(TEvExample::TEvReplicaLookup::TPtr &ev) {
        auto &record = ev->Get()->Record;
        const auto &key = record.GetKey();

        auto keyIt = IndexKey.find(key);
        if (keyIt == IndexKey.end()) {
            Send(ev->Sender, new TEvExample::TEvReplicaInfo(key), 0, ev->Cookie);
            return;
        }

        auto reply = MakeHolder<TEvExample::TEvReplicaInfo>(key);
        reply->Record.MutablePayload()->Reserve(keyIt->second.size());
        for (ui32 entryIndex : keyIt->second) {
            const TEntry &entry = Entries[entryIndex];
            reply->Record.AddPayload(entry.Payload);
        }

        Send(ev->Sender, std::move(reply), 0, ev->Cookie);
    }

    void Handle(TEvExample::TEvReplicaPublish::TPtr &ev) {
        auto &record = ev->Get()->Record;
        const TString &key = record.GetKey();
        const TString &payload = record.GetPayload();
        const TActorId &owner = ev->Sender;

        auto ownerIt = IndexOwner.find(owner);
        if (ownerIt != IndexOwner.end()) {
            const ui32 entryIndex = ownerIt->second;
            TEntry &entry = Entries[entryIndex];
            if (entry.KeyIt->first != key) {
                // reply nothing, request suspicious
                return;
            }

            entry.Payload = payload;
        }
        else {
            const ui32 entryIndex = AllocateEntry();
            TEntry &entry = Entries[entryIndex];

            entry.Payload = payload;
            entry.Owner = owner;

            entry.OwnerIt = IndexOwner.emplace(owner, entryIndex).first;
            entry.KeyIt = IndexKey.emplace(std::make_pair(key, TSet<ui32>())).first;
            entry.KeyIt->second.emplace(entryIndex);

            Send(owner, new TEvExample::TEvReplicaPublishAck(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ev->Cookie);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        auto ownerIt = IndexOwner.find(ev->Sender);
        if (ownerIt == IndexOwner.end())
            return;

        CleanupEntry(ownerIt->second);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        auto *msg = ev->Get();
        const ui32 nodeId = msg->NodeId;
        auto ownerIt = IndexOwner.lower_bound(TActorId(nodeId, 0, 0, 0));
        while (ownerIt != IndexOwner.end() && ownerIt->first.NodeId() == nodeId) {
            const ui32 idx = ownerIt->second;
            ++ownerIt;
            CleanupEntry(idx);
        }
    }

public:
    static constexpr IActor::EActivityType ActorActivityType() {
        // define app-specific activity tag to track elapsed cpu | handled events | actor count in Solomon
        return EActorActivity::ACTORLIB_COMMON;
    }

    TExampleReplicaActor()
        : TActor(&TThis::StateWork)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExample::TEvReplicaLookup, Handle);
            hFunc(TEvExample::TEvReplicaPublish, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);

            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        default:
            // here is place to spam some log message on unknown events
            break;
        }
    }
};

IActor* CreateReplica() {
    return new TExampleReplicaActor();
}

TActorId MakeReplicaId(ui32 nodeid) {
    char x[12] = { 'r', 'p', 'l' };
    memcpy(x + 5, &nodeid, sizeof(ui32));
    return TActorId(nodeid, TStringBuf(x, 12));
}
