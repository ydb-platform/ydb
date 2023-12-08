#pragma once
#include <ydb/library/actors/examples/02_discovery/protocol.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/event_local.h>

#include <util/generic/vector.h>

using namespace NActors;

struct TExampleStorageConfig : public TThrRefBase {
    TVector<TActorId> Replicas;
};

struct TEvExample {
    enum EEv {
        EvReplicaLookup = EventSpaceBegin(TEvents::ES_USERSPACE + 1),
        EvReplicaPublish,

        EvReplicaInfo = EventSpaceBegin(TEvents::ES_USERSPACE + 2),
        EvReplicaPublishAck,

        EvInfo = EventSpaceBegin(TEvents::ES_USERSPACE + 3),
    };

    struct TEvReplicaLookup : public TEventPB<TEvReplicaLookup, NActorsExample::TEvReplicaLookup, EvReplicaLookup> {
        TEvReplicaLookup()
        {}

        TEvReplicaLookup(const TString &key)
        {
            Record.SetKey(key);
        }
    };

    struct TEvReplicaPublish : public TEventPB<TEvReplicaPublish, NActorsExample::TEvReplicaPublish, EvReplicaPublish> {
        TEvReplicaPublish()
        {}

        TEvReplicaPublish(const TString &key, const TString &payload)
        {
            Record.SetKey(key);
            Record.SetPayload(payload);
        }
    };

    struct TEvReplicaInfo : public TEventPB<TEvReplicaInfo, NActorsExample::TEvReplicaInfo, EvReplicaInfo> {
        TEvReplicaInfo()
        {}

        TEvReplicaInfo(const TString &key)
        {
            Record.SetKey(key);
        }
    };

    struct TEvReplicaPublishAck : public TEventPB<TEvReplicaPublishAck, NActorsExample::TEvReplicaPublishAck, EvReplicaPublishAck> {
        TEvReplicaPublishAck()
        {}

        TEvReplicaPublishAck(const TString &key)
        {
            Record.SetKey(key);
        }
    };

    struct TEvInfo : public TEventLocal<TEvInfo, EvInfo> {
        const TString Key;
        const TVector<TString> Payloads;

        TEvInfo(const TString &key, TVector<TString> &&payloads)
            : Key(key)
            , Payloads(payloads)
        {}
    };
};

IActor* CreateReplica();
IActor* CreatePublishActor(TExampleStorageConfig *config, const TString &key, const TString &what);
IActor* CreateLookupActor(TExampleStorageConfig *config, const TString &key, TActorId replyTo);
IActor* CreateEndpointActor(TExampleStorageConfig *config, const TString &publishKey, ui16 httpPort);

TActorId MakeReplicaId(ui32 nodeid);
