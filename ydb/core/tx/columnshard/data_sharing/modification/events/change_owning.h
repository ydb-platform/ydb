#pragma once
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NOlap::NDataSharing {
class TTaskForTablet;
}

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvApplyLinksModification: public NActors::TEventPB<TEvApplyLinksModification, NKikimrColumnShardDataSharingProto::TEvApplyLinksModification, TEvColumnShard::EvApplyLinksModification> {
    TEvApplyLinksModification() = default;

    TEvApplyLinksModification(const TTabletId initiatorTabletId, const TString& sessionId, const ui64 packIdx, const TTaskForTablet& task);
};

struct TEvApplyLinksModificationFinished: public NActors::TEventPB<TEvApplyLinksModificationFinished,
    NKikimrColumnShardDataSharingProto::TEvApplyLinksModificationFinished, TEvColumnShard::EvApplyLinksModificationFinished> {
    TEvApplyLinksModificationFinished() = default;
    TEvApplyLinksModificationFinished(const TTabletId modifiedTabletId, const TString& sessionId, const ui64 packIdx) {
        Record.SetSessionId(sessionId);
        Record.SetModifiedTabletId((ui64)modifiedTabletId);
        Record.SetPackIdx(packIdx);
    }
};

}