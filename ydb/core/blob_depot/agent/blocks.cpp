#include "blocks.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    NKikimrProto::EReplyStatus TBlobDepotAgent::TBlocksManager::CheckBlockForTablet(ui64 tabletId, std::optional<ui32> generation,
            TQuery *query, ui32 *blockedGeneration) {
        NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
        auto& block = Blocks[tabletId];
        const TMonotonic now = TActivationContext::Monotonic();
        if (generation && generation <= block.BlockedGeneration) {
            status = NKikimrProto::BLOCKED;
        } else if (now < block.ExpirationTimestamp) {
            if (blockedGeneration) {
                *blockedGeneration = block.BlockedGeneration;
            }
            status = NKikimrProto::OK;
        }
        bool refreshQueried = false;
        if (status != NKikimrProto::BLOCKED && now + block.TimeToLive / 2 >= block.ExpirationTimestamp && !block.RefreshId) {
            NKikimrBlobDepot::TEvQueryBlocks queryBlocks;
            queryBlocks.AddTabletIds(tabletId);
            block.RefreshId = Agent.Issue(std::move(queryBlocks), this, std::make_shared<TQueryBlockContext>(
                TActivationContext::Monotonic(), tabletId));
            refreshQueried = true;
        }
        if (status == NKikimrProto::UNKNOWN) {
            block.PendingBlockChecks.PushBack(query);
        }
        YDB_LOG_DEBUG("CheckBlockForTablet",
            {"marker", "BDA01"},
            {"agentId", Agent.LogId},
            {"queryId", query->GetQueryId()},
            {"tabletId", tabletId},
            {"generation", generation},
            {"status", status},
            {"now", now},
            {"expirationTimestamp", block.ExpirationTimestamp},
            {"refreshQueried", refreshQueried},
            {"refreshId", block.RefreshId});
        return status;
    }

    void TBlobDepotAgent::TBlocksManager::ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) {
        if (auto *p = std::get_if<TEvBlobDepot::TEvQueryBlocksResult*>(&response)) {
            Handle(std::move(context), (*p)->Record);
        } else if (std::holds_alternative<TTabletDisconnected>(response)) {
            auto& queryBlockContext = context->Obtain<TQueryBlockContext>();
            auto& block = Blocks[queryBlockContext.TabletId];
            YDB_LOG_DEBUG("TBlocksManager::TTabletDisconnected",
                {"marker", "BDA36"},
                {"agentId", Agent.LogId},
                {"tabletId", queryBlockContext.TabletId},
                {"refreshId", block.RefreshId});
            block.RefreshId = 0;
            IssueOnUpdateBlock(block);
        } else {
            Y_ABORT("unexpected response type");
        }
    }

    void TBlobDepotAgent::TBlocksManager::Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvQueryBlocksResult& msg) {
        auto& queryBlockContext = context->Obtain<TQueryBlockContext>();
        YDB_LOG_DEBUG("TEvQueryBlocksResult",
            {"marker", "BDA02"},
            {"agentId", Agent.LogId},
            {"msg", msg},
            {"tabletId", queryBlockContext.TabletId});
        auto& block = Blocks[queryBlockContext.TabletId];
        Y_ABORT_UNLESS(block.RefreshId);
        Y_ABORT_UNLESS(msg.BlockedGenerationsSize() == 1);
        const ui32 newBlockedGeneration = msg.GetBlockedGenerations(0);
        Y_ABORT_UNLESS(block.BlockedGeneration <= newBlockedGeneration);
        block.BlockedGeneration = newBlockedGeneration;
        block.TimeToLive = TDuration::MilliSeconds(msg.GetTimeToLiveMs());
        block.ExpirationTimestamp = queryBlockContext.Timestamp + block.TimeToLive;
        block.RefreshId = 0;
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
        Y_ABORT_UNLESS(block.BlockedGeneration <= blockedGeneration);
        block.BlockedGeneration = blockedGeneration;
        block.TimeToLive = timeToLive;
        block.ExpirationTimestamp = timestamp + timeToLive;
        if (block.RefreshId) {
            Agent.DropTabletRequest(block.RefreshId);
            block.RefreshId = 0;
        }
    }

    void TBlobDepotAgent::TBlocksManager::OnBlockedTablets(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TEvPushNotify::TBlockedTablet>& tablets) {
        for (const auto& tablet : tablets) {
            if (const auto it = Blocks.find(tablet.GetTabletId()); it != Blocks.end()) {
                auto& block = it->second;
                YDB_LOG_DEBUG("OnBlockedTablets",
                    {"marker", "BDA37"},
                    {"agentId", Agent.LogId},
                    {"tabletId", it->first},
                    {"refreshId", block.RefreshId},
                    {"blockedGeneration", tablet.GetBlockedGeneration()},
                    {"issuerGuid", tablet.GetIssuerGuid()});
                block.BlockedGeneration = tablet.GetBlockedGeneration();
                block.IssuerGuid = tablet.GetIssuerGuid();
                block.ExpirationTimestamp = TMonotonic::Zero();
                if (block.RefreshId) {
                    Agent.DropTabletRequest(block.RefreshId);
                    block.RefreshId = 0;
                }
                IssueOnUpdateBlock(block);
            }
        }
    }

} // NKikimr::NBlobDepot
