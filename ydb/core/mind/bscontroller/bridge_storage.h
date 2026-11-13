#pragma once

#include <ydb/core/base/bridge.h>
#include <ydb/core/protos/bridge_storage.pb.h>

#include <util/datetime/base.h>

#include <optional>

namespace NKikimr::NBsController::NBridgeStorage {

    using ECopyState = NKikimrBridge::ESynchronizationStatus;

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

    // Borrowed state remains valid for the duration of one controller request.
    struct TCopyObservation {
        ECopyState State = NKikimrBridge::UNKNOWN;
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
        ui64 NotRequired = 0;

        void Count(ECopyState state);
        void CountDatabase(ECopyState state);
        ECopyState GetStatus() const;
        void ToProto(NKikimrBridge::TGroupSyncCounts* to) const;
        void ToProto(NKikimrBridge::TDatabaseSyncCounts* to) const;
    };

    // Bound both the scan and the full serialized answer. Space is reserved for its envelope.
    struct TReportBudget {
        ui64 Visited = 0;
        ui64 Bytes = 1024;

        bool Visit();
        bool Fits(ui64 bytes) const;
        bool Account(const google::protobuf::MessageLite& row);
    };

    struct TGroupRow {
        ui32 GroupId;
        std::optional<ui32> PhysicalGroupId;
        const NKikimrBridge::TStoragePool* Pool = nullptr;
        std::optional<TStringBuf> PileName;
        TCopyObservation Copy;
    };

    // Used by the live controller path and the synthetic materialization benchmark.
    bool AppendGroup(const TGroupRow& input, TReportBudget& budget, NKikimrBridge::TGroupSynchronizationResult& result);

} // namespace NKikimr::NBsController::NBridgeStorage
