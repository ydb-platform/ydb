#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {
    namespace NSync {

        class TQuorumTrackerDebug {
        public:
            void Update(const TVDiskIdShort &vdisk) {
                ++Map[vdisk];
            }

            TString ToString() const {
                TStringStream str;
                str << "{Map: ";
                for (const auto &x : Map) {
                    str << "[" << x.first << " " << x.second << "]";
                }
                str << "}";
                return str.Str();
            }

            void Clear() {
                Map.clear();
            }

        private:
            THashMap<TVDiskIdShort, unsigned> Map;
        };

        ///////////////////////////////////////////////////////////////////////////
        // TQuorumTracker
        // The class tracks responses from other vdisks in group to obtain quorum
        ///////////////////////////////////////////////////////////////////////////
        class TQuorumTracker {
        public:
            TQuorumTracker(const TVDiskIdShort &selfVDisk,
                           std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                           bool includeMyFailDomain)
                : Top(std::move(top))
                , MyFailDomainOrderNumber(Top->GetFailDomainOrderNumber(selfVDisk))
                , IncludeMyFailDomain(includeMyFailDomain)
                , SyncedDisks(Top.get())
                , Erasure(Top->GType.GetErasure())
            {
            }

            void Update(const TVDiskIdShort &vdisk) {
                Debug.Update(vdisk);
                if (IncludeMyFailDomain || Top->GetFailDomainOrderNumber(vdisk) != MyFailDomainOrderNumber) {
                    SyncedDisks |= TBlobStorageGroupInfo::TGroupVDisks(Top.get(), vdisk);
                }
            }

            bool HasQuorum() const {
                const auto& checker = Top->GetQuorumChecker();
                return checker.CheckQuorumForGroup(SyncedDisks);
            }

            void Clear() {
                Debug.Clear();
                SyncedDisks = TBlobStorageGroupInfo::TGroupVDisks(Top.get());
            }

            void Output(IOutputStream &str) const {
                str << "{Debug# " << Debug.ToString() << "}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }

        private:
            const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
            const ui32 MyFailDomainOrderNumber;
            const bool IncludeMyFailDomain;
            TQuorumTrackerDebug Debug;
            TBlobStorageGroupInfo::TGroupVDisks SyncedDisks;

        public:
            const TBlobStorageGroupType::EErasureSpecies Erasure;
        };

    } // NSync
} // NKikimr
