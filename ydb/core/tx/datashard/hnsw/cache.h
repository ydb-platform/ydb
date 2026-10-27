#pragma once

#include "../hnsw_index.h"
#include "../hnsw_memory_tracker.h"

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NDataShard {

constexpr ui32 HnswSnapshotFormatVersion = 1;

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
        EvForget,
        EvEnd,
    };
    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_HNSW_CACHE));

    struct TEvRegister : NActors::TEventLocal<TEvRegister, EvRegister> {};
    struct TEvUnregister : NActors::TEventLocal<TEvUnregister, EvUnregister> {};
    struct TEvRegistered : NActors::TEventLocal<TEvRegistered, EvRegistered> {
        std::shared_ptr<THnswCacheMemoryTracker> Tracker;
    };
    // Keys identify an immutable generation in a tablet incarnation. Reusing a
    // key for different storage is rejected, even if settings are compatible.
    struct TEvPublish : NActors::TEventLocal<TEvPublish, EvPublish> {
        TString Key;
        std::shared_ptr<THnswIndex> Graph;
        std::shared_ptr<const THnswJournalSnapshot> Journal;
        Ydb::Table::VectorIndexSettings Settings;
    };
    struct TEvAcquire : NActors::TEventLocal<TEvAcquire, EvAcquire> {
        TString Key;
        ui64 RequestId = 0;
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
    struct TEvForget : NActors::TEventLocal<TEvForget, EvForget> {
        explicit TEvForget(TString key) : Key(std::move(key)) {}
        TString Key;
    };
};

NActors::TActorId MakeHnswCacheId(ui32 nodeId = 0);
NActors::IActor* CreateHnswCache();

} // namespace NKikimr::NDataShard
