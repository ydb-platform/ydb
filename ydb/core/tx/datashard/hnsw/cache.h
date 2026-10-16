#pragma once

#include "../hnsw_index.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/hnsw.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NDataShard {

constexpr ui32 HnswSnapshotFormatVersion = 1;
constexpr TStringBuf HnswGraphAuxPrefix = "HnswGraphGeneration/v1:";

struct THnswJournalSnapshot {
    struct TChange {
        TString Key;
        TRowVersion Version;
        std::optional<TString> Vector;
    };

    std::shared_ptr<void> Reservation;
    std::vector<TChange> Changes;

    void Save(std::ostream& output) const;
    static std::shared_ptr<THnswJournalSnapshot> Load(std::istream& input, ui64 maxBytes);
};

struct TEvHnswCache {
    enum EEvents {
        EvRegister = EventSpaceBegin(TKikimrEvents::ES_HNSW_CACHE),
        EvUnregister,
        EvRegistered,
        EvPublish,
        EvAcquire,
        EvAcquired,
        EvChunkRequest,
        EvChunkResponse,
        EvSnapshotRequest,
        EvSnapshotResponse,
        EvFileReady,
        EvLoaded,
        EvRetry,
        EvStatusRequest,
        EvStatusResponse,
        EvGraphAnnouncement,
        EvEnd,
    };

    struct TEvRegister : NActors::TEventLocal<TEvRegister, EvRegister> {};
    struct TEvUnregister : NActors::TEventLocal<TEvUnregister, EvUnregister> {};
    struct TEvRegistered : NActors::TEventLocal<TEvRegistered, EvRegistered> {
        std::shared_ptr<THnswCacheMemoryTracker> Tracker;
    };
    struct TEvPublish : NActors::TEventLocal<TEvPublish, EvPublish> {
        TString Key;
        std::shared_ptr<THnswIndex> Graph;
        std::shared_ptr<const THnswJournalSnapshot> Journal;
        Ydb::Table::VectorIndexSettings Settings;
    };
    struct TEvAcquire : NActors::TEventLocal<TEvAcquire, EvAcquire> {
        TString Key;
        ui32 SourceNodeId = 0;
        ui64 RequestId = 0;
        ui64 MemoryBytes = 0;
        bool Journal = false;
        Ydb::Table::VectorIndexSettings Settings;
    };
    struct TEvAcquired : NActors::TEventLocal<TEvAcquired, EvAcquired> {
        TString Key;
        ui64 RequestId = 0;
        std::shared_ptr<THnswIndex> Graph;
        std::shared_ptr<const THnswJournalSnapshot> Journal;
        TString Error;
    };
    struct TEvChunkRequest : NActors::TEventPB<TEvChunkRequest, NKikimrHnsw::TChunkRequest, EvChunkRequest> {};
    struct TEvChunkResponse : NActors::TEventPB<TEvChunkResponse, NKikimrHnsw::TChunkResponse, EvChunkResponse> {
        // Sender-side transfer bytes remain charged while queued for sending.
        std::shared_ptr<void> BufferReservation;
    };
    struct TEvSnapshotRequest : NActors::TEventPB<TEvSnapshotRequest, NKikimrHnsw::TSnapshotRequest, EvSnapshotRequest> {};
    struct TEvSnapshotResponse : NActors::TEventPB<TEvSnapshotResponse, NKikimrHnsw::TSnapshotResponse, EvSnapshotResponse> {};
    struct TEvStatusRequest : NActors::TEventPB<TEvStatusRequest, NKikimrHnsw::TStatusRequest, EvStatusRequest> {};
    struct TEvStatusResponse : NActors::TEventPB<TEvStatusResponse, NKikimrHnsw::TStatusResponse, EvStatusResponse> {};
    struct TEvGraphAnnouncement : NActors::TEventPB<TEvGraphAnnouncement, NKikimrHnsw::TGraphAnnouncement, EvGraphAnnouncement> {};
};

NActors::TActorId MakeHnswCacheId(ui32 nodeId = 0);
NActors::IActor* CreateHnswCache();

} // namespace NKikimr::NDataShard
