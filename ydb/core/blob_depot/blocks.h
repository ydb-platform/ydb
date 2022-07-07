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
            THashMap<ui32, TPerAgentInfo> PerAgentInfo;
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

        void AddBlockOnLoad(ui64 tabletId, ui32 generation);
        void OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, std::unique_ptr<IEventHandle> response);
        void Handle(TEvBlobDepot::TEvBlock::TPtr ev);
        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev);
    };

} // NKikimr::NBlobDepot
