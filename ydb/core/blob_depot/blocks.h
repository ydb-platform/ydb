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
            THashMap<ui32, TPerAgentInfo> PerAgentInfo;

            bool CanSetNewBlock(ui32 blockedGeneration, ui64 issuerGuid) const {
                return BlockedGeneration < blockedGeneration || (BlockedGeneration == blockedGeneration &&
                    (IssuerGuid == issuerGuid && IssuerGuid && issuerGuid));
            }
        };

        TBlobDepot* const Self;
        THashMap<ui64, TBlock> Blocks;

    private:
        class TTxUpdateBlock;
        class TBlockProcessorActor;

    public:
        TBlocksManager(TBlobDepot *self)
            : Self(self)
        {}

        void AddBlockOnLoad(ui64 tabletId, ui32 blockedGeneration, ui64 issuerGuid);
        void AddBlockOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlock& block, NTabletFlatExecutor::TTransactionContext& txc);
        void OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, ui64 issuerGuid,
            std::unique_ptr<IEventHandle> response);
        void Handle(TEvBlobDepot::TEvBlock::TPtr ev);
        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev);

        bool CheckBlock(ui64 tabletId, ui32 generation) const;

        template<typename TCallback>
        void Enumerate(TCallback&& callback) const {
            for (const auto& [tabletId, block] : Blocks) {
                callback(tabletId, block.BlockedGeneration);
            }
        }
    };

} // NKikimr::NBlobDepot
