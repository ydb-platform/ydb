#pragma once

#include "defs.h"
#include "types.h"

namespace NKikimr {
    namespace NBsController {

        class TGroupGeometryInfo;

        // TGroupMapper is a helper class used to create groups from a set of PDisks with their respective locations
        // over physical hardware
        class TGroupMapper {
            class TImpl;
            THolder<TImpl> Impl;

        public:
            template<class T>
            using TGroupDefinitionBase = TVector<TVector<TVector<T>>>; // Realm/Domain/Disk
            using TGroupDefinition = TGroupDefinitionBase<TPDiskId>;
            using TForbiddenPDisks = std::unordered_set<TPDiskId, THash<TPDiskId>>;

            struct TTargetDiskConstraints {
                std::optional<ui32> NodeId = std::nullopt;
            };
            using TGroupConstraintsDefinition = TGroupDefinitionBase<TTargetDiskConstraints>;

            static void MergeTargetDiskConstraints(const TTargetDiskConstraints& from, TTargetDiskConstraints& to) {
                if (from.NodeId.has_value()) {
                    to.NodeId = from.NodeId;
                }
            }
            template<class T>
            static void MergeTargetDiskConstraints(const TVector<T>& from, TVector<T>& to) {
                Y_VERIFY_S(from.size() == to.size(), "Could not merge constraints with different sizes");
                for (ui32 i = 0; i < from.size(); ++i) {
                    MergeTargetDiskConstraints(from[i], to[i]);
                }
            }

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
                TStackVec<ui32, 16> Groups;
                i64 SpaceAvailable;
                const bool Operational;
                const bool Decommitted;
                TString WhyUnusable;
            };

        public:
            TGroupMapper(TGroupGeometryInfo geom, bool randomize = false);
            ~TGroupMapper();

            // Register PDisk inside mapper to use it in subsequent map operations
            bool RegisterPDisk(const TPDiskRecord& pdisk);

            // Remove PDisk from the table.
            void UnregisterPDisk(TPDiskId pdiskId);

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
                const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error);
            bool AllocateGroup(ui32 groupId, TGroupDefinition& group, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks,
                TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error);

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

            TMisplacedVDisks FindMisplacedVDisks(const TGroupDefinition& group);

            std::optional<TPDiskId> TargetMisplacedVDisk(TGroupId groupId, TGroupDefinition& group, TVDiskIdShort vdisk, 
                TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error);
        };

    } // NBsController
} // NKikimr
