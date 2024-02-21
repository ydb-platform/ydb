#pragma once
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvCheckStatusFromInitiator: public NActors::TEventPB<TEvCheckStatusFromInitiator, NKikimrColumnShardDataSharingProto::TEvCheckStatusFromInitiator, TEvColumnShard::EvDataSharingCheckStatusFromInitiator> {
    TEvCheckStatusFromInitiator() = default;

    TEvCheckStatusFromInitiator(const TString& sessionId) {
        Record.SetSessionId(sessionId);
    }
};

}