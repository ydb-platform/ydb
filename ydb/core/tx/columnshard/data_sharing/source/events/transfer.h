#pragma once
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvAckDataToSource: public NActors::TEventPB<TEvAckDataToSource, NKikimrColumnShardDataSharingProto::TEvAckDataToSource, TEvColumnShard::EvDataSharingAckDataToSource> {
    TEvAckDataToSource() = default;

    TEvAckDataToSource(const TString& sessionId, const ui32 packIdx) {
        Record.SetSessionId(sessionId);
        Record.SetPackIdx(packIdx);
    }
};

struct TEvAckFinishToSource: public NActors::TEventPB<TEvAckFinishToSource, NKikimrColumnShardDataSharingProto::TEvAckFinishToSource, TEvColumnShard::EvDataSharingAckFinishToSource> {
    TEvAckFinishToSource() = default;

    TEvAckFinishToSource(const TString& sessionId) {
        Record.SetSessionId(sessionId);
    }
};

}