#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent::TBlocksManager
        : public TRequestSender
    {
        struct TBlockInfo {
            ui32 BlockedGeneration;
            TMonotonic ExpirationTimestamp; // not valid after
            bool RefreshInFlight = false;
            TIntrusiveList<TQuery, TPendingBlockChecks> PendingBlockChecks;
        };

        THashMap<ui64, TBlockInfo> Blocks;

    public:
        TBlocksManager(TBlobDepotAgent& agent)
            : TRequestSender(agent)
        {}

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TQueryBlockContext : TRequestContext {
            TMonotonic Timestamp;
            ui64 TabletId;

            TQueryBlockContext(TMonotonic timestamp, ui64 tabletId)
                : Timestamp(timestamp)
                , TabletId(tabletId)
            {}
        };

        NKikimrProto::EReplyStatus CheckBlockForTablet(ui64 tabletId, ui32 generation, TQuery *query,
                ui32 *blockedGeneration) {
            auto& block = Blocks[tabletId];
            const TMonotonic issueTime = TActivationContext::Monotonic();
            if (generation <= block.BlockedGeneration) {
                return NKikimrProto::RACE;
            } else if (issueTime < block.ExpirationTimestamp) {
                if (blockedGeneration) {
                    *blockedGeneration = block.BlockedGeneration;
                }
                return NKikimrProto::OK;
            } else if (!block.RefreshInFlight) {
                NKikimrBlobDepot::TEvQueryBlocks queryBlocks;
                queryBlocks.AddTabletIds(tabletId);
                Agent.Issue(std::move(queryBlocks), this, std::make_shared<TQueryBlockContext>(
                    TActivationContext::Monotonic(), tabletId));
                block.RefreshInFlight = true;
                block.PendingBlockChecks.PushBack(query);
            }
            return NKikimrProto::UNKNOWN;
        }

        void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
            if (auto *p = std::get_if<TEvBlobDepot::TEvQueryBlocksResult*>(&response)) {
                Handle(std::move(context), (*p)->Record);
            } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                IssueOnUpdateBlock(context, false);
            } else {
                Y_FAIL("unexpected response type");
            }
        }

        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvQueryBlocksResult& msg) {
            auto& queryBlockContext = context->Obtain<TQueryBlockContext>();
            STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA01, "TEvQueryBlocksResult", (VirtualGroupId, Agent.VirtualGroupId),
                (Msg, msg), (TabletId, queryBlockContext.TabletId));
            auto& block = Blocks[queryBlockContext.TabletId];
            Y_VERIFY(msg.BlockedGenerationsSize() == 1);
            const ui32 newBlockedGeneration = msg.GetBlockedGenerations(0);
            Y_VERIFY(block.BlockedGeneration <= newBlockedGeneration);
            block.BlockedGeneration = newBlockedGeneration;
            block.ExpirationTimestamp = queryBlockContext.Timestamp + TDuration::MilliSeconds(msg.GetTimeToLiveMs());
            IssueOnUpdateBlock(context, true);
        }

        void IssueOnUpdateBlock(const TRequestContext::TPtr& context, bool success) {
            auto& queryBlockContext = context->Obtain<TQueryBlockContext>();
            auto& block = Blocks[queryBlockContext.TabletId];
            TIntrusiveList<TQuery, TPendingBlockChecks> temp;
            temp.Swap(block.PendingBlockChecks);
            for (auto it = temp.begin(); it != temp.end(); ) {
                const auto current = it++;
                current->OnUpdateBlock(success);
            }
        }

        ui32 GetBlockForTablet(ui64 tabletId) {
            const auto it = Blocks.find(tabletId);
            return it != Blocks.end() ? it->second.BlockedGeneration : 0;
        }

        void SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic expirationTimestamp) {
            auto& block = Blocks[tabletId];
            Y_VERIFY(block.BlockedGeneration <= blockedGeneration);
            if (block.BlockedGeneration < blockedGeneration) {
                block.BlockedGeneration = blockedGeneration;
                block.ExpirationTimestamp = expirationTimestamp;
            } else if (block.ExpirationTimestamp < expirationTimestamp) {
                block.ExpirationTimestamp = expirationTimestamp;
            }
        }
    };

    TBlobDepotAgent::TBlocksManagerPtr TBlobDepotAgent::CreateBlocksManager() {
        return {new TBlocksManager{*this}, std::default_delete<TBlocksManager>{}};
    }
    
    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvQueryBlocks msg, TRequestSender *sender, TRequestContext::TPtr context) {
        auto ev = std::make_unique<TEvBlobDepot::TEvQueryBlocks>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(context));
    }

    NKikimrProto::EReplyStatus TBlobDepotAgent::CheckBlockForTablet(ui64 tabletId, ui32 generation, TQuery *query,
            ui32 *blockedGeneration) {
        return BlocksManager->CheckBlockForTablet(tabletId, generation, query, blockedGeneration);
    }

    ui32 TBlobDepotAgent::GetBlockForTablet(ui64 tabletId) {
        return BlocksManager->GetBlockForTablet(tabletId);
    }

    void TBlobDepotAgent::SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic expirationTimestamp) {
        BlocksManager->SetBlockForTablet(tabletId, blockedGeneration, expirationTimestamp);
    }

} // NKikimr::NBlobDepot
