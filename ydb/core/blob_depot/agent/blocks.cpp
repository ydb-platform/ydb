#include "blocks.h"

namespace NKikimr::NBlobDepot {

    NKikimrProto::EReplyStatus TBlobDepotAgent::TBlocksManager::CheckBlockForTablet(ui64 tabletId, ui32 generation,
            TQuery *query, ui32 *blockedGeneration) {
        NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
        auto& block = Blocks[tabletId];
        const TMonotonic now = TActivationContext::Monotonic();
        if (generation <= block.BlockedGeneration) {
            status = NKikimrProto::BLOCKED;
        } else if (now < block.ExpirationTimestamp) {
            if (blockedGeneration) {
                *blockedGeneration = block.BlockedGeneration;
            }
            status = NKikimrProto::OK;
        }
        if (status != NKikimrProto::BLOCKED && now + block.TimeToLive / 2 >= block.ExpirationTimestamp && !block.RefreshInFlight) {
            NKikimrBlobDepot::TEvQueryBlocks queryBlocks;
            queryBlocks.AddTabletIds(tabletId);
            Agent.Issue(std::move(queryBlocks), this, std::make_shared<TQueryBlockContext>(
                TActivationContext::Monotonic(), tabletId));
            block.RefreshInFlight = true;
        }
        if (status == NKikimrProto::UNKNOWN) {
            block.PendingBlockChecks.PushBack(query);
        }
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA01, "CheckBlockForTablet", (QueryId, query->GetQueryId()),
            (TabletId, tabletId), (Generation, generation), (Status, status), (Now, now),
            (ExpirationTimestamp, block.ExpirationTimestamp));
        return status;
    }

    void TBlobDepotAgent::TBlocksManager::ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) {
        if (auto *p = std::get_if<TEvBlobDepot::TEvQueryBlocksResult*>(&response)) {
            Handle(std::move(context), (*p)->Record);
        } else if (std::holds_alternative<TTabletDisconnected>(response)) {
            // do nothing, query executors will receive this notification as well
        } else {
            Y_FAIL("unexpected response type");
        }
    }

    void TBlobDepotAgent::TBlocksManager::Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvQueryBlocksResult& msg) {
        auto& queryBlockContext = context->Obtain<TQueryBlockContext>();
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA02, "TEvQueryBlocksResult", (VirtualGroupId, Agent.VirtualGroupId),
            (Msg, msg), (TabletId, queryBlockContext.TabletId));
        auto& block = Blocks[queryBlockContext.TabletId];
        Y_VERIFY(msg.BlockedGenerationsSize() == 1);
        const ui32 newBlockedGeneration = msg.GetBlockedGenerations(0);
        Y_VERIFY(block.BlockedGeneration <= newBlockedGeneration);
        block.BlockedGeneration = newBlockedGeneration;
        block.TimeToLive = TDuration::MilliSeconds(msg.GetTimeToLiveMs());
        block.ExpirationTimestamp = queryBlockContext.Timestamp + block.TimeToLive;
        block.RefreshInFlight = false;
        IssueOnUpdateBlock(block);
    }

    void TBlobDepotAgent::TBlocksManager::IssueOnUpdateBlock(TBlock& block) {
        TIntrusiveList<TQuery, TPendingBlockChecks> pendingBlockChecks;
        pendingBlockChecks.Append(block.PendingBlockChecks);
        pendingBlockChecks.ForEach([](TQuery *query) { query->OnUpdateBlock(); });
    }

    std::tuple<ui32, ui64> TBlobDepotAgent::TBlocksManager::GetBlockForTablet(ui64 tabletId) {
        if (const auto it = Blocks.find(tabletId); it != Blocks.end()) {
            const auto& record = it->second;
            return {record.BlockedGeneration, record.IssuerGuid};
        } else {
            return {0, 0};
        }
    }

    void TBlobDepotAgent::TBlocksManager::SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic timestamp, TDuration timeToLive) {
        auto& block = Blocks[tabletId];
        Y_VERIFY(block.BlockedGeneration <= blockedGeneration);
        block.BlockedGeneration = blockedGeneration;
        block.TimeToLive = timeToLive;
        block.ExpirationTimestamp = timestamp + timeToLive;
    }

    void TBlobDepotAgent::TBlocksManager::OnBlockedTablets(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TEvPushNotify::TBlockedTablet>& tablets) {
        for (const auto& tablet : tablets) {
            if (const auto it = Blocks.find(tablet.GetTabletId()); it != Blocks.end()) {
                auto& block = it->second;
                block.BlockedGeneration = tablet.GetBlockedGeneration();
                block.IssuerGuid = tablet.GetIssuerGuid();
                block.ExpirationTimestamp = TMonotonic::Zero();
                IssueOnUpdateBlock(block);
            }
        }
    }

} // NKikimr::NBlobDepot
