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
            using TGroupDefinition = TVector<TVector<TVector<TPDiskId>>>; // Realm/Domain/Disk 
            using TForbiddenPDisks = std::unordered_set<TPDiskId, THash<TPDiskId>>;

        public:
            TGroupMapper(TGroupGeometryInfo geom, bool randomize = false);
            ~TGroupMapper();

            // Register PDisk inside mapper to use it in subsequent map operations
            bool RegisterPDisk(TPDiskId pdiskId, TNodeLocation location, bool usable, ui32 numSlots, ui32 maxSlots,
                const ui32 groupIds[], size_t numGroups, i64 spaceAvailable, bool operational);

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
            bool AllocateGroup(ui32 groupId, TGroupDefinition& group, const TPDiskId replacedDiskIds[],
                size_t numReplacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational,
                TString& error);
        };

    } // NBsController
} // NKikimr
