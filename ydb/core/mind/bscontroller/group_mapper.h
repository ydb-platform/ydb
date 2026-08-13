#pragma once

#include "defs.h"
#include "types.h"

#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>

#include <ydb/library/actors/core/interconnect.h>

#include <variant>

namespace NKikimr {
    namespace NBsController {

        class TGroupGeometryInfo;

        struct TGroupMapperError {
            struct TStats {
                TString Domain;
                ui32 AllSlotsAreOccupied = 0;
                ui32 NotEnoughSpace = 0;
                ui32 NotAcceptingNewSlots = 0;
                ui32 NotOperational = 0;
                ui32 Decommission = 0;
            };

            TString ErrorMessage;
            TStats TotalStats;
            std::vector<TStats> MatchingDomainsStats;
            ui32 MissingFailRealmsCount = 0;
            ui32 FailRealmsWithMissingDomainsCount = 0;
            ui32 DomainsWithMissingDisksCount = 0;
            ui32 OkDisksCount = 0;
            TString RealmLocationKey;
            TString DomainLocationKey;
        };

        class TPDiskSlotTracker {
            absl::flat_hash_map<ui32, ui16> ReplicatingVDisksByNode;
            absl::flat_hash_map<TPDiskId, ui8> ReplicatingVDisksByPDisk;
            absl::flat_hash_map<TString, i32> FreeSlotsPerRack;
        public:
            ui16 GetReplicatingVDisksOnNode(ui32 nodeId) const {
                if (const auto it = ReplicatingVDisksByNode.find(nodeId); it != ReplicatingVDisksByNode.end()) {
                    return it->second;
                }
                return 0;
            }

            ui8 GetReplicatingVDisksOnPDisk(TPDiskId pdiskId) const {
                if (const auto it = ReplicatingVDisksByPDisk.find(pdiskId); it != ReplicatingVDisksByPDisk.end()) {
                    return it->second;
                }
                return 0;
            }

            i32 GetFreeSlotsOnRack(const TString& rack) const {
                if (const auto it = FreeSlotsPerRack.find(rack); it != FreeSlotsPerRack.end()) {
                    return it->second;
                }
                return 0;
            }

            void AddReplicatingVSlot(TPDiskId pdiskId) {
                ++ReplicatingVDisksByNode[pdiskId.NodeId];
                ++ReplicatingVDisksByPDisk[pdiskId];
            }

            void AddFreeSlotsForRack(const TString& rack, i32 freeSlots) {
                FreeSlotsPerRack[rack] += freeSlots;
            }
        };

        // TGroupMapper is a helper class used to create groups from a set of PDisks with their respective locations
        // over physical hardware
        class TGroupMapper {
        public:
            struct TOptions {
                bool Randomize = false;
                bool PreferLessOccupiedRack = false;
                bool WithAttentionToReplication = false;
                bool IgnoreVSlotQuotaCheck = false;
                bool SettleOnlyOnOperationalDisks = false;
                bool IsSelfHealReasonDecommit = false;
                NKikimrBlobStorage::TPDiskSpaceColor::E SpaceColorBorder = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;
                ui32 SpaceMarginPromille = 0;
            };

        private:
            class TImpl;
            const TOptions Options;
            THolder<TImpl> Impl;
            THashMap<TVDiskID, i64> VDiskAllocatedSizes;

        public:
            template<class T>
            using TGroupDefinitionBase = TVector<TVector<TVector<T>>>; // Realm/Domain/Disk
            using TGroupDefinition = TGroupDefinitionBase<TPDiskId>;
            using TForbiddenPDisks = std::unordered_set<TPDiskId, THash<TPDiskId>>;

            struct TTargetDiskConstraints {
                std::optional<ui32> NodeId = std::nullopt;
                std::optional<TPDiskId> PDiskId = std::nullopt;
            };
            using TGroupConstraintsDefinition = TGroupDefinitionBase<TTargetDiskConstraints>;

            template<typename T, typename F>
            static void Traverse(const TGroupDefinitionBase<T>& group, F&& callback) {
                for (ui32 failRealmIdx = 0; failRealmIdx != group.size(); ++failRealmIdx) {
                    const auto& realm = group[failRealmIdx];
                    for (ui32 failDomainIdx = 0; failDomainIdx != realm.size(); ++failDomainIdx) {
                        const auto& domain = realm[failDomainIdx];
                        for (ui32 vdiskIdx = 0; vdiskIdx != domain.size(); ++vdiskIdx) {
                            callback(TVDiskIdShort(failRealmIdx, failDomainIdx, vdiskIdx), domain[vdiskIdx]);
                        }
                    }
                }
            }

            struct TPDiskRecord {
                const TPDiskId PDiskId;
                const TNodeLocation Location;
                const bool Usable;
                ui32 NumSlots;
                const ui32 MaxSlots;
                const ui32 SlotSizeInUnits;
                const ui64 SlotSizeInBytes;
                TStackVec<ui32, 16> Groups;
                i64 SpaceAvailable;
                const bool Operational;
                const bool Decommitted;
                TString WhyUnusable;
                TBridgePileId BridgePileId;
                std::optional<TString> DiskScope;
            };

            struct TPDiskSpaceState {
                std::optional<ui64> EnforcedDynamicSlotSize;
                ui64 AvailableSize = 0;
                ui64 TotalSize = 0;
            };

            struct TPDiskState {
                TPDiskId PDiskId;
                TNodeLocation Location;
                bool Usable = true;
                ui32 NumSlots = 0;
                ui32 MaxSlots = 0;
                ui32 SlotSizeInUnits = 0;
                ui64 SlotSizeInBytes = 0;
                std::optional<TPDiskSpaceState> Space;
                bool Operational = false;
                NKikimrBlobStorage::EDriveStatus DriveStatus = NKikimrBlobStorage::EDriveStatus::ACTIVE;
                NKikimrBlobStorage::TMaintenanceStatus::E MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::NOT_SET;
                NKikimrBlobStorage::EDecommitStatus DecommitStatus = NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE;
                TString WhyUnusable;
                TBridgePileId BridgePileId;
                std::optional<TString> DiskScope;
            };

            struct TVSlotState {
                TVSlotId VSlotId;
                TPDiskId PDiskId;
                std::optional<ui32> GroupId;
                ui32 GroupGeneration = 0;
                TVDiskIdShort VDiskId;
                bool CountedInNumSlots = true;
                bool OccupiedByGroup = true;
                bool Ready = false;
                bool Replicating = false;
                std::optional<i64> AllocatedSize;
                std::optional<i64> SpaceUsed;
            };

            struct TGroupState {
                ui32 GroupId = 0;
                ui32 GroupGeneration = 0;
                ui32 GroupSizeInUnits = 1;
                std::optional<i64> MaxVDiskAllocatedSize;
            };

            struct TPlacementSnapshot {
                TVector<TPDiskState> PDisks;
                TVector<TVSlotState> VSlots;
                TVector<TGroupState> Groups;
                std::optional<TPDiskSlotTracker> PrecomputedReplicationTracker;
            };

            class TPlacementBuilder {
                class TState;
                THolder<TState> State;

                void UpdateMaxGroupSlotSize(ui32 groupId, ui32 groupGeneration, i64 spaceUsed);

                friend class TGroupMapper;

            public:
                explicit TPlacementBuilder(TGroupMapper& mapper);
                ~TPlacementBuilder();

                void AddGroup(const TGroupState& group);
                void AddPDisk(TPDiskState pdisk);
                void AddVSlot(const TVSlotState& vslot);
                void SetPrecomputedReplicationTracker(TPDiskSlotTracker tracker);
                void Finish();
            };

            struct TKeepVDisk {};

            struct TReplaceVDisk {
                bool RequireSameNode = false;
            };

            struct TReplaceVDiskOnPDisk {
                TPDiskId PDiskId;
            };

            struct TForceVDiskOnPDisk {
                TPDiskId PDiskId;
            };

            using TVDiskReassignment = std::variant<TKeepVDisk, TReplaceVDisk, TReplaceVDiskOnPDisk, TForceVDiskOnPDisk>;

            struct TVDiskPlacement {
                TVDiskIdShort VDiskId;
                TPDiskId PDiskId;
                std::optional<i64> AllocatedSize;
                TVDiskReassignment Reassignment = TKeepVDisk{};
            };

            struct TReassignmentRequest {
                ui32 GroupId = 0;
                ui32 GroupGeneration = 0;
                TVector<TVDiskPlacement> VDisks;
                TForbiddenPDisks ForbiddenPDisks;
                ui32 GroupSizeInUnits = 1;
                i64 MinimumRequiredSpace = Min<i64>();
                bool ExistingGroup = true;
                bool TryToRelocateLocallyFirst = false;
                TBridgePileId BridgePileId;
            };

            struct TReassignmentOutcome {
                bool Success = false;
                TGroupDefinition Group;
                i64 RequiredSpace = Min<i64>();
                TGroupMapperError Error;
            };

            TGroupMapper(TGroupGeometryInfo geom, bool randomize = false, bool preferLessOccupiedRack = false, bool withAttentionToReplication = false);
            TGroupMapper(TGroupGeometryInfo geom, TOptions options);
            ~TGroupMapper();

            static TPDiskSpaceState CapturePDiskSpace(const NKikimrBlobStorage::TPDiskMetrics& metrics);
            static bool IsPDiskOperational(bool nodeConnected, const NKikimrBlobStorage::TPDiskMetrics* metrics) {
                return nodeConnected
                       && (!metrics
                           || !metrics->HasState()
                           || metrics->GetState() == NKikimrBlobStorage::TPDiskState::Normal);
            }

            static bool SlotSpaceEnforced(const TPDiskSpaceState& space,
                                          NKikimrBlobStorage::TPDiskSpaceColor::E colorBorder);
            static bool SlotSpaceEnforced(const NKikimrBlobStorage::TPDiskMetrics& metrics,
                                          NKikimrBlobStorage::TPDiskSpaceColor::E colorBorder) {
                return metrics.HasEnforcedDynamicSlotSize()
                       && colorBorder >= NKikimrBlobStorage::TPDiskSpaceColor::YELLOW;
            }

            static i64 CalculateSpaceAvailable(const TPDiskSpaceState& space,
                                               NKikimrBlobStorage::TPDiskSpaceColor::E colorBorder, ui32 marginPromille);
            static constexpr bool AcceptsNewSlots(NKikimrBlobStorage::EDriveStatus status,
                                                  NKikimrBlobStorage::TMaintenanceStatus::E maintenanceStatus) {
                return status == NKikimrBlobStorage::EDriveStatus::ACTIVE
                       && maintenanceStatus != NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED
                       && maintenanceStatus != NKikimrBlobStorage::TMaintenanceStatus::NO_NEW_VDISKS;
            }

            static constexpr bool UsableInTermsOfDecommission(NKikimrBlobStorage::EDecommitStatus status,
                                                              bool isSelfHealReasonDecommit) {
                return status == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE
                       || status == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_REJECTED && !isSelfHealReasonDecommit;
            }

            static bool IsDecommitted(NKikimrBlobStorage::EDecommitStatus status) {
                switch (status) {
                    case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE:
                        return false;
                    case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_PENDING:
                    case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_IMMINENT:
                    case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_REJECTED:
                        return true;
                    case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_UNSET:
                    case NKikimrBlobStorage::EDecommitStatus::EDecommitStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
                    case NKikimrBlobStorage::EDecommitStatus::EDecommitStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
                        break;
                }
                Y_ABORT("unexpected EDecommitStatus");
            }

            static i64 CalculateRequiredSpace(const TVector<TVDiskPlacement>& vdisks, i64 minimumRequiredSpace = Min<i64>());

            static TReassignmentOutcome PlanGroupReassignment(TGroupGeometryInfo geom, TOptions options, TPlacementSnapshot snapshot,
                                                              TReassignmentRequest request);

            void Populate(TPlacementSnapshot snapshot);

            void SetPDiskSlotTracker(TPDiskSlotTracker&& state);

            TPDiskSlotTracker& GetPDiskSlotTracker();

            // Register PDisk inside mapper to use it in subsequent map operations
            bool RegisterPDisk(const TPDiskRecord& pdisk);

            TReassignmentOutcome PlanGroupReassignment(TReassignmentRequest request);

            // Remove PDisk from the table.
            TPDiskRecord UnregisterPDisk(TPDiskId pdiskId);

            // Adjust VDisk space quota.
            void AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment);

            // Allocate group (with incrementing number of used slots in internal structures) of given geometry. This
            // function returns true if group allocation succeeds returning PDisk layout in result variable, or false
            // otherwise. Allocation occurs on less occupied disks (measured with number of used VSlots). The resulting
            // group, if allocated, meets following requirements:
            // 1. Realm prefix and infix is the same for every disk in the same realm.
            // 2. Realm prefix is the same for all realms, but infix differs for every realm.
            // 3. Inside any fail realm the domain prefix is the same for all disks in that realm, but for every domain
            //    infix differs.
            //
            // The PDisk location given in RegisterPDisk is split into three parts (prefix, infix, suffix) depending on
            // the context (realm or domain). Prefix part includes all levels with their respective values with level
            // key strictly less than FirstDxLevel; infix part includes all levels with key in [BeginDxLevel,
            // EndDxLevel) semi-open range; and the suffix part covers the remaining parts.
            //
            // According to the stated requirements, the algorithm is as follows:
            //
            // 1. Allocate realms by splitting all PDisk locations into tuples (prefix, infix, suffix) according to
            // failRealmBeginDxLevel, failRealmEndDxLevel, and then by finding possible options to meet requirements
            // (1) and (2). That is, prefix gives us unique domains in which we can find realms to operate, while
            // prefix+infix part gives us distinct fail realms we can use while generating groups.
            bool AllocateGroup(ui32 groupId, TGroupDefinition& group, TGroupMapper::TGroupConstraintsDefinition& constraints,
                const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TForbiddenPDisks forbid, ui32 groupSizeInUnits, i64 requiredSpace,
                bool requireOperational, TBridgePileId bridgePileId, TGroupMapperError& error);
            bool AllocateGroup(ui32 groupId, TGroupDefinition& group, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks,
                TForbiddenPDisks forbid, ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational, TBridgePileId bridgePileId,
                TGroupMapperError& error);

            struct TMisplacedVDisks {
                enum EFailLevel : ui32 {
                    ALL_OK,
                    MULTIPLE_REALM_OCCUPATION,
                    PDISK_FAIL,
                    DOMAIN_FAIL,
                    REALM_FAIL,
                    EMPTY_SLOT,
                    INCORRECT_LAYOUT,
                };

                TMisplacedVDisks(EFailLevel failLevel, std::vector<TVDiskIdShort> disks, TString errorReason = "")
                    : FailLevel(failLevel)
                    , Disks(std::move(disks))
                    , ErrorReason(errorReason)
                {}

                EFailLevel FailLevel;
                std::vector<TVDiskIdShort> Disks;
                TString ErrorReason;

                operator bool() const {
                    return FailLevel != EFailLevel::INCORRECT_LAYOUT;
                }
            };

            TMisplacedVDisks FindMisplacedVDisks(const TGroupDefinition& group, ui32 groupSizeInUnits);

            std::optional<TPDiskId> TargetMisplacedVDisk(TGroupId groupId, TGroupDefinition& group, TVDiskIdShort vdisk,
                TForbiddenPDisks forbid, ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational, TBridgePileId bridgePileId,
                TString& error);
        };

    } // NBsController
} // NKikimr
