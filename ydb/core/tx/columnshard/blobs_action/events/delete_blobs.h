#pragma once
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/core/tx/columnshard/blobs_action/protos/events.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NOlap::NBlobOperations::NEvents {

struct TEvDeleteSharedBlobs: public NActors::TEventPB<TEvDeleteSharedBlobs, NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobs, TEvColumnShard::EvDeleteSharedBlobs> {
    TEvDeleteSharedBlobs() = default;

    TEvDeleteSharedBlobs(const NActors::TActorId sourceActorId, const ui64 sourceTabletId, const TString& storageId, const THashSet<NOlap::TUnifiedBlobId>& blobIds) {
        Record.SetStorageId(storageId);
        Record.SetSourceTabletId(sourceTabletId);
        NActors::ActorIdToProto(sourceActorId, Record.MutableSourceActorId());
        for (auto&& i : blobIds) {
            *Record.AddBlobIds() = i.ToStringNew();
        }
    }
};

struct TEvDeleteSharedBlobsFinished: public NActors::TEventPB<TEvDeleteSharedBlobsFinished,
    NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished, TEvColumnShard::EvDeleteSharedBlobsFinished> {
    TEvDeleteSharedBlobsFinished() = default;
    TEvDeleteSharedBlobsFinished(const TTabletId tabletId, const NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished::EStatus status) {
        Record.SetTabletId((ui64)tabletId);
        Record.SetStatus(status);
    }
};

}