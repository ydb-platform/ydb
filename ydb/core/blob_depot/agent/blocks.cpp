#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent::TBlocksManager
        : public TRequestSender
    {
        struct TBlock {
            ui32 BlockedGeneration;
            TDuration TimeToLive;
            TMonotonic ExpirationTimestamp; // not valid after
            bool RefreshInFlight = false;
            TIntrusiveList<TQuery, TPendingBlockChecks> PendingBlockChecks;
        };

        THashMap<ui64, TBlock> Blocks;

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
            NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
            auto& block = Blocks[tabletId];
            const TMonotonic now = TActivationContext::Monotonic();
            if (generation <= block.BlockedGeneration) {
                status = NKikimrProto::RACE;
            } else if (now < block.ExpirationTimestamp) {
                if (blockedGeneration) {
                    *blockedGeneration = block.BlockedGeneration;
                }
                status = NKikimrProto::OK;
            }
            if (status != NKikimrProto::RACE && now + block.TimeToLive / 2 >= block.ExpirationTimestamp && !block.RefreshInFlight) {
                NKikimrBlobDepot::TEvQueryBlocks queryBlocks;
                queryBlocks.AddTabletIds(tabletId);
                Agent.Issue(std::move(queryBlocks), this, std::make_shared<TQueryBlockContext>(
                    TActivationContext::Monotonic(), tabletId));
                block.RefreshInFlight = true;
                block.PendingBlockChecks.PushBack(query);
            }
            STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA99, "CheckBlockForTablet", (QueryId, query->GetQueryId()),
                (TabletId, tabletId), (Generation, generation), (Status, status), (Now, now),
                (ExpirationTimestamp, block.ExpirationTimestamp));
            return status;
        }

        void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override {
            if (auto *p = std::get_if<TEvBlobDepot::TEvQueryBlocksResult*>(&response)) {
                Handle(std::move(context), (*p)->Record);
            } else if (std::holds_alternative<TTabletDisconnected>(response)) {
                auto& queryBlockContext = context->Obtain<TQueryBlockContext>();
                if (const auto it = Blocks.find(queryBlockContext.TabletId); it != Blocks.end()) {
                    IssueOnUpdateBlock(it->second, false);
                }
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
            block.TimeToLive = TDuration::MilliSeconds(msg.GetTimeToLiveMs());
            block.ExpirationTimestamp = queryBlockContext.Timestamp + block.TimeToLive;
            block.RefreshInFlight = false;
            IssueOnUpdateBlock(block, true);
        }

        void IssueOnUpdateBlock(TBlock& block, bool success) {
            TIntrusiveList<TQuery, TPendingBlockChecks> pendingBlockChecks;
            pendingBlockChecks.Append(block.PendingBlockChecks);
            pendingBlockChecks.ForEach([success](TQuery *query) {
                query->OnUpdateBlock(success);
            });
        }

        ui32 GetBlockForTablet(ui64 tabletId) {
            const auto it = Blocks.find(tabletId);
            return it != Blocks.end() ? it->second.BlockedGeneration : 0;
        }

        void SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic timestamp, TDuration timeToLive) {
            auto& block = Blocks[tabletId];
            Y_VERIFY(block.BlockedGeneration <= blockedGeneration);
            block.BlockedGeneration = blockedGeneration;
            block.TimeToLive = timeToLive;
            block.ExpirationTimestamp = timestamp + timeToLive;
        }

        void OnBlockedTablets(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TEvPushNotify::TBlockedTablet>& tablets) {
            for (const auto& tablet : tablets) {
                if (const auto it = Blocks.find(tablet.GetTabletId()); it != Blocks.end()) {
                    auto& block = it->second;
                    block.BlockedGeneration = tablet.GetBlockedGeneration();
                    block.ExpirationTimestamp = TMonotonic::Zero();
                    IssueOnUpdateBlock(block, true);
                }
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

    void TBlobDepotAgent::SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic timestamp, TDuration timeToLive) {
        BlocksManager->SetBlockForTablet(tabletId, blockedGeneration, timestamp, timeToLive);
    }

    void TBlobDepotAgent::OnBlockedTablets(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TEvPushNotify::TBlockedTablet>& tablets) {
        BlocksManager->OnBlockedTablets(tablets);
    }

} // NKikimr::NBlobDepot
