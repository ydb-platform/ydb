#pragma once
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NOlap::NDataSharing {
class TDestinationSession;
}

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvProposeFromInitiator: public NActors::TEventPB<TEvProposeFromInitiator, NKikimrColumnShardDataSharingProto::TEvProposeFromInitiator, TEvColumnShard::EvDataSharingProposeFromInitiator> {
    TEvProposeFromInitiator() = default;

    TEvProposeFromInitiator(const TDestinationSession& session);
};

struct TEvConfirmFromInitiator: public NActors::TEventPB<TEvConfirmFromInitiator, NKikimrColumnShardDataSharingProto::TEvConfirmFromInitiator, TEvColumnShard::EvDataSharingConfirmFromInitiator> {
    TEvConfirmFromInitiator() = default;

    TEvConfirmFromInitiator(const TString& sessionId);
};

struct TEvAckFinishFromInitiator: public NActors::TEventPB<TEvAckFinishFromInitiator, NKikimrColumnShardDataSharingProto::TEvAckFinishFromInitiator, TEvColumnShard::EvDataSharingAckFinishFromInitiator> {
    TEvAckFinishFromInitiator() = default;

    TEvAckFinishFromInitiator(const TString& sharingId) {
        Record.SetSessionId(sharingId);
    }
};

}