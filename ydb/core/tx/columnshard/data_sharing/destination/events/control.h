#pragma once
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NOlap::NDataSharing {
class TDestinationSession;
}

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvStartFromInitiator: public NActors::TEventPB<TEvStartFromInitiator, NKikimrColumnShardDataSharingProto::TEvStartFromInitiator, TEvColumnShard::EvDataSharingStartFromInitiator> {
    TEvStartFromInitiator() = default;

    TEvStartFromInitiator(const TDestinationSession& session);
};

struct TEvAckFinishFromInitiator: public NActors::TEventPB<TEvAckFinishFromInitiator, NKikimrColumnShardDataSharingProto::TEvAckFinishFromInitiator, TEvColumnShard::EvDataSharingAckFinishFromInitiator> {
    TEvAckFinishFromInitiator() = default;

    TEvAckFinishFromInitiator(const TString& sharingId) {
        Record.SetSessionId(sharingId);
    }
};

}