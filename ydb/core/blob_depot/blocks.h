#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBlocksManager {
        // wait duration before issuing blocks via storage
        static constexpr TDuration AgentsWaitTime = TDuration::Seconds(1);

        // TTL for block lease
        static constexpr TDuration BlockLeaseTime = TDuration::Seconds(60);

        struct TBlock {
            struct TPerAgentInfo {
                TMonotonic ExpirationTimestamp = TMonotonic::Zero();
            };

            ui32 BlockedGeneration = 0;
            ui64 IssuerGuid = 0;
            ui32 Version = 0;
            THashMap<ui32, TPerAgentInfo> PerAgentInfo;

            bool CanSetNewBlock(ui32 blockedGeneration, ui64 issuerGuid) const {
                return BlockedGeneration < blockedGeneration || (BlockedGeneration == blockedGeneration &&
                    (IssuerGuid == issuerGuid && IssuerGuid && issuerGuid));
            }
        };

        TBlobDepot* const Self;
        THashMap<ui64, TBlock> Blocks;

        // tablets whose data still has to be dropped after a complete deletion has been seen; the
        // set is drained by TTxDeleteTabletData once the data is loaded
        THashSet<ui64> TabletsToDelete;
        bool DeleteTabletDataInFlight = false;

    private:
        class TTxUpdateBlock;
        class TTxQueryBlocks;
        class TTxDeleteTabletData;
        class TBlockProcessorActor;

        void OnTabletDeleted(ui64 tabletId);
        void ProcessTabletsToDelete();

    public:
        TBlocksManager(TBlobDepot *self)
            : Self(self)
        {}

        void AddBlockOnLoad(ui64 tabletId, ui32 blockedGeneration, ui64 issuerGuid, ui32 version);
        void AddBlockOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlock& block, NTabletFlatExecutor::TTransactionContext& txc);
        void OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, ui64 issuerGuid,
            std::optional<ui32> version, std::unique_ptr<IEventHandle> response);
        void Handle(TEvBlobDepot::TEvBlock::TPtr ev);
        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev);
        void OnDataLoaded();

        bool CheckBlock(ui64 tabletId, ui32 generation) const;

        // A block with generation Max<ui32>() is the tombstone Hive writes when it deletes a tablet
        // for good; from that moment on none of the tablet's data is needed and nothing can ever be
        // written for it again. We must not wait for the hard barrier that Hive sends next -- a
        // VDisk that has seen this block is free to drop the barrier records themselves, so during
        // decommission they may never reach us. Unless EnableCollectByCompleteDeletionBlock is on, no
        // tablet is considered deleted and its data waits for that hard barrier as usual.
        bool IsTabletDeleted(ui64 tabletId) const {
            if (!Self->CollectByCompleteDeletionBlock) {
                return false;
            }
            const auto it = Blocks.find(tabletId);
            return it != Blocks.end() && IsCompleteTabletDeletionBlock(it->second.BlockedGeneration);
        }

        template<typename TCallback>
        void Enumerate(TCallback&& callback) const {
            for (const auto& [tabletId, block] : Blocks) {
                callback(tabletId, block.BlockedGeneration);
            }
        }
    };

} // NKikimr::NBlobDepot
