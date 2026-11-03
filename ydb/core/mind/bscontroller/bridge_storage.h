#pragma once

#include "types.h"

#include <ydb/core/base/bridge.h>
#include <ydb/core/protos/bridge_storage.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <map>
#include <set>
#include <tuple>

namespace NKikimr::NBsController::NBridgeStorage {

    using ECopyState = NKikimrBridge::TStorageGroup::EState;
    using EMode = NKikimrBridge::TStorageDatabase::EMode;

    struct TGroupContext {
        const NKikimrBridge::TGroupState* State;
        TBridgePileId AuthorityPileId;

        explicit TGroupContext(const NKikimrBridge::TGroupState* state = nullptr)
            : State(state)
            , AuthorityPileId(state ? NBridge::GetAuthorityPileId(*state) : TBridgePileId())
        {
        }
    };

    struct TCopyContext {
        TGroupContext Group;
        TBridgePileId PileId;
        NKikimrBridge::TClusterState::EPileState PileState = NKikimrBridge::TClusterState::INACTIVE;
        const NKikimrBridge::TClusterStateDetails::TPileSyncState* PileSyncState = nullptr;
        bool BridgeMode = true;
        bool SyncerRunning = false;
    };

    // Borrowed pointers are used only while observing the controller's current state.
    // No transport strings are constructed until a group is included in a response.
    struct TCopyObservation {
        ECopyState State = NKikimrBridge::TStorageGroup::UNKNOWN;
        const NKikimrBridge::TGroupState::TPile* Part = nullptr;
        bool CurrentGeneration = false;
        const TString* LastError = nullptr;
        TInstant LastErrorTimestamp;
    };

    TCopyObservation ObserveCopy(const TCopyContext& context);

    struct TCounts {
        ui64 Total = 0;
        ui64 Synced = 0;
        ui64 Running = 0;
        ui64 Pending = 0;
        ui64 Unknown = 0;

        void Count(ECopyState state);
        void Add(const TCounts& other);
        ECopyState State() const;
        void ToProto(NKikimrBridge::TStorageCounts* to) const;
    };

    struct TCopySummary {
        TCounts Groups;
        TString LastError;
        TInstant LastErrorTimestamp;

        void Add(const TCopyObservation& copy);
        void Add(const TCopySummary& copy);
    };

    struct TPoolSummary {
        TBoxStoragePoolId Id;
        TString Name;
        TPathId Owner = TPathId(0, 0);
        EMode Mode = NKikimrBridge::TStorageDatabase::MODE_UNSPECIFIED;
        std::map<TBridgePileId, TCopySummary> Copies;
    };

    struct TOwnerKey {
        TPathId PathId = TPathId(0, 0);
        TBoxStoragePoolId FallbackPool;

        bool operator<(const TOwnerKey& other) const {
            return std::tie(PathId.OwnerId, PathId.LocalPathId, FallbackPool) <
                   std::tie(other.PathId.OwnerId, other.PathId.LocalPathId, other.FallbackPool);
        }
    };

    struct TDatabaseKey {
        TOwnerKey Owner;
        TBridgePileId PileId;

        bool operator<(const TDatabaseKey& other) const {
            return std::tie(Owner, PileId) < std::tie(other.Owner, other.PileId);
        }
    };

    struct TDatabaseSummary {
        TVector<TString> StoragePools;
        EMode Mode = NKikimrBridge::TStorageDatabase::MODE_UNSPECIFIED;
        TCopySummary Copy;
    };

    struct TPileSummary {
        TString Name;
        TCounts UserGroups;
        TCounts SystemGroups;
    };

    using TPoolSummaries = std::map<TBoxStoragePoolId, TPoolSummary>;
    using TDatabaseSummaries = std::map<TDatabaseKey, TDatabaseSummary>;

    struct TSnapshot {
        ui64 Generation = 0;
        TInstant CollectedAt;
        std::map<TBridgePileId, TPileSummary> Piles;
        TPoolSummaries Pools;
        TDatabaseSummaries Databases;
    };

    // Full and filtered listings share the same owner/fallback identity and ordinary/mixed rules.
    TDatabaseSummaries BuildDatabaseSummaries(const TPoolSummaries& pools,
                                              const std::set<TBoxStoragePoolId>* selected = nullptr);

    ui64 QueryHash(const NKikimrBridge::TStorageQuery& query);
    bool ParsePageToken(const NKikimrBridge::TStorageQuery& query, ui64 generation,
                        NKikimrBridge::TStoragePageToken& token);
    TString MakePageToken(const NKikimrBridge::TStorageQuery& query, ui64 generation,
                          const TDatabaseKey& last);
    TString MakePageToken(const NKikimrBridge::TStorageQuery& query, ui64 generation,
                          const NKikimrBridge::TStoragePageToken::TGroupCursor& last);
    TDatabaseKey DatabaseCursorKey(const NKikimrBridge::TStoragePageToken::TDatabaseCursor& cursor);

} // namespace NKikimr::NBsController::NBridgeStorage
