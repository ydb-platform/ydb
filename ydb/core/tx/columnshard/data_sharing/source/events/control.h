#pragma once
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>

namespace NKikimr::NOlap::NDataSharing {
class TSourceSession;
}

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvStartToSource: public NActors::TEventPB<TEvStartToSource, NKikimrColumnShardDataSharingProto::TEvStartToSource, TEvColumnShard::EvDataSharingStartToSource> {
    TEvStartToSource() = default;

    TEvStartToSource(const TSourceSession& session);
};

}