#pragma once

#include "datashard.h"
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NDataShard {

class TDataShard;

class TReplicationSourceOffsetsServer
    : public TActor<TReplicationSourceOffsetsServer>
{
    friend class TReplicationSourceOffsetsServerLink;

private:
    struct TReadId {
        TActorId ActorId;
        ui64 ReadId;

        TReadId() = default;

        TReadId(const TActorId& actorId, ui64 readId)
            : ActorId(actorId)
            , ReadId(readId)
        { }

        size_t Hash() const {
            return CombineHashes(ActorId.Hash(), IntHash(ReadId));
        }

        explicit operator size_t() const {
            return Hash();
        }

        bool operator==(const TReadId& rhs) const {
            return ActorId == rhs.ActorId && ReadId == rhs.ReadId;
        }
    };

    using TReadIdList = TList<TReadId>;

    struct TReadState {
        struct TInFlight {
            ui64 SeqNo = 0;
            ui64 Bytes = 0;
        };

        TPathId PathId;
        ui64 Cookie = 0;
        ui64 WindowSize = 64 * 1024 * 1024;

        ui64 NextSourceId = 0;
        ui64 NextSplitKeyId = 0;
        TActorId InterconnectSession;
        TReadIdList::iterator WaitingNodeIt;
        bool WaitingNode = false;
        bool Finished = false;

        ui64 LastSeqNo = 0;

        TList<TInFlight> InFlight;
        ui64 InFlightTotal = 0;
    };

    struct TNodeState {
        ui64 WindowSize = 128 * 1024 * 1024;
        ui64 InFlightTotal = 0;
        THashSet<TActorId> Sessions;

        TReadIdList WaitingReads;
    };

    struct TSessionState {
        ui32 NodeId = 0;
        bool Subscribed = false;
        THashSet<TReadId> Reads;
    };

public:
    TReplicationSourceOffsetsServer(TDataShard* self)
        : TActor(&TThis::StateWork)
        , Self(self)
    { }

    ~TReplicationSourceOffsetsServer() {
        Unlink();
    }

    void PassAway() override {
        Unlink();
        TActor::PassAway();
    }

    void Unlink();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_DATASHARD_SOURCE_OFFSETS_SERVER;
    }

    STFUNC(StateWork);

    void Handle(TEvDataShard::TEvGetReplicationSourceOffsets::TPtr& ev);
    void Handle(TEvDataShard::TEvReplicationSourceOffsetsAck::TPtr& ev);
    void Handle(TEvDataShard::TEvReplicationSourceOffsetsCancel::TPtr& ev);
    void ProcessRead(const TReadId& readId, TReadState& state);
    void ProcessNode(TNodeState& node);
    void SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags = 0, ui64 cookie = 0);
    void Handle(TEvents::TEvUndelivered::TPtr& ev);
    void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void NodeConnected(const TActorId& sessionId);
    void NodeDisconnected(const TActorId& sessionId);

private:
    TDataShard* Self;
    THashMap<TReadId, TReadState> Reads;
    THashMap<ui32, TNodeState> Nodes;
    THashMap<TActorId, TSessionState> Sessions;
};

/**
 * This class is a wrapper around a simple pointer that unlinks actors on destruction
 */
class TReplicationSourceOffsetsServerLink {
public:
    TReplicationSourceOffsetsServerLink() = default;

    // Cannot be copied or moved
    TReplicationSourceOffsetsServerLink(const TReplicationSourceOffsetsServerLink&) = delete;
    TReplicationSourceOffsetsServerLink& operator=(const TReplicationSourceOffsetsServerLink&) = delete;

    // Removes the other side of the link
    ~TReplicationSourceOffsetsServerLink() noexcept {
        if (Link) {
            Link->Self = nullptr;
            Link = nullptr;
        }
    }

    explicit operator bool() const {
        return bool(Link);
    }

    operator TReplicationSourceOffsetsServer*() const {
        return Link;
    }

    TReplicationSourceOffsetsServer* operator->() const {
        return Link;
    }

    TReplicationSourceOffsetsServer& operator*() const {
        return *Link;
    }

    TReplicationSourceOffsetsServerLink& operator=(TReplicationSourceOffsetsServer* link) {
        Link = link;
        return *this;
    }

private:
    TReplicationSourceOffsetsServer* Link = nullptr;
};

} // namespace NKikimr::NDataShard
