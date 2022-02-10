#include "statestorage_impl.h"
#include <ydb/core/protos/services.pb.h>
#include <library/cpp/actors/core/interconnect.h>

#include <util/generic/set.h>

#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/hfunc.h>

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
    };

    TVector<TEntry> Entries;
    TVector<ui32> AvailableEntries;

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

        auto ownerIt = IndexOwner.find(owner);
        if (ownerIt != IndexOwner.end()) {
            const ui32 entryIndex = ownerIt->second;
            TEntry &entry = Entries[entryIndex];
            if (entry.PathIt->first != path) {
                BLOG_ERROR("unconsistent path for same owner");
                // reply nothing, request suspicious
                return;
            }

            entry.Payload = record.GetPayload();
            Y_VERIFY_DEBUG(entry.Owner == ActorIdFromProto(record.GetOwner()));
        } else {
            const ui32 entryIndex = AllocateEntry();
            TEntry &entry = Entries[entryIndex];

            entry.Payload = record.GetPayload();
            entry.Owner = ActorIdFromProto(record.GetOwner());

            auto ownerInsPairIt = IndexOwner.emplace(owner, entryIndex);
            entry.OwnerIt = ownerInsPairIt.first;
            auto pathInsPairIt = IndexPath.emplace(std::make_pair(path, TSet<ui32>()));
            entry.PathIt = pathInsPairIt.first;
            entry.PathIt->second.emplace(entryIndex);

            Send(owner, new TEvStateStorage::TEvReplicaBoardPublishAck, IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ev->Cookie);
        }
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
        entry.PathIt->second.erase(entryIndex);
        if (entry.PathIt->second.empty()) {
            IndexPath.erase(entry.PathIt);
        }

        if (IsLastEntryOnNode(entry.OwnerIt)) {
            Send(TActivationContext::InterconnectProxy(entry.OwnerIt->first.NodeId()), new TEvents::TEvUnsubscribe());
        }
        IndexOwner.erase(entry.OwnerIt);

        TString().swap(entry.Payload);
        entry.Owner = TActorId();
        entry.PathIt = IndexPath.end();
        entry.OwnerIt = IndexOwner.end();

        AvailableEntries.emplace_back(entryIndex);
    }

    void PassAway() override {
        ui32 prevNode = 0;
        for (auto &xpair : IndexOwner) {
            const ui32 nodeId = xpair.first.NodeId();
            if (nodeId != prevNode) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
                prevNode = nodeId;
            }

            Send(xpair.first, new TEvStateStorage::TEvReplicaShutdown());
        }

        // all cleanup in actor destructor
        TActor::PassAway();
    }

    void Handle(TEvStateStorage::TEvReplicaBoardCleanup::TPtr &ev) {
        auto ownerIt = IndexOwner.find(ev->Sender);
        if (ownerIt == IndexOwner.end()) // do nothing, already removed?
            return;

        CleanupEntry(ownerIt->second);
    }

    void Handle(TEvStateStorage::TEvReplicaBoardLookup::TPtr &ev) {
        auto &record = ev->Get()->Record;
        const auto &path = record.GetPath();

        if (record.GetSubscribe()) {
            BLOG_ERROR("trying to subscribe on path, must be not implemented yet");
            // reply nothing, request suspicious
            return;
        }

        auto pathIt = IndexPath.find(path);
        if (pathIt == IndexPath.end()) {
            Send(ev->Sender, new TEvStateStorage::TEvReplicaBoardInfo(path, true), 0, ev->Cookie);
            return;
        }

        auto reply = MakeHolder<TEvStateStorage::TEvReplicaBoardInfo>(path, false);
        auto *info = reply->Record.MutableInfo();
        info->Reserve(pathIt->second.size());
        for (ui32 entryIndex : pathIt->second) {
            const TEntry &entry = Entries[entryIndex];
            auto *ex = info->Add();
            ActorIdToProto(entry.Owner, ex->MutableOwner());
            ex->SetPayload(entry.Payload);
        }

        Send(ev->Sender, std::move(reply), 0, ev->Cookie);
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
            const ui32 entryToCleanupIndex = ownerIt->second;
            ++ownerIt;
            CleanupEntry(entryToCleanupIndex);
        }
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
