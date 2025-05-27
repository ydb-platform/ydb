#pragma once

#include "defs.h"
#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent::TBlocksManager
        : public TRequestSender
    {
        struct TBlock {
            ui32 BlockedGeneration = 0;
            ui64 IssuerGuid = 0;
            TDuration TimeToLive;
            TMonotonic ExpirationTimestamp; // not valid after
            ui64 RefreshId = 0;
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

        NKikimrProto::EReplyStatus CheckBlockForTablet(ui64 tabletId, std::optional<ui32> generation, TQuery *query, ui32 *blockedGeneration);
        void ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) override;
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvQueryBlocksResult& msg);
        void IssueOnUpdateBlock(TBlock& block);
        std::tuple<ui32, ui64> GetBlockForTablet(ui64 tabletId);
        void SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic timestamp, TDuration timeToLive);
        void OnBlockedTablets(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TEvPushNotify::TBlockedTablet>& tablets);
    };

} // NKikimr::NBlobDepot
