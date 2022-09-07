#pragma once

#include "defs.h"
#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent::TBlobMappingCache
        : public TRequestSender
    {
        struct TQueryWaitingForKey {
            TQuery* const Query;
            ui64 Id;

            TQueryWaitingForKey(TQuery *query, ui64 id)
                : Query(query)
                , Id(id)
            {}
        };

        struct TCachedKeyItem : TIntrusiveListItem<TCachedKeyItem> {
            TStringBuf Key;
            TResolvedValueChain Values;
            bool ResolveInFlight = false;
            std::list<TQueryWaitingForKey> QueriesWaitingForKey;
        };

        THashMap<TString, TCachedKeyItem> Cache;
        TIntrusiveList<TCachedKeyItem> Queue;

    public:
        TBlobMappingCache(TBlobDepotAgent& agent)
            : TRequestSender(agent)
        {}

        void HandleResolveResult(const NKikimrBlobDepot::TEvResolveResult& msg, TRequestContext::TPtr context);
        const TResolvedValueChain *ResolveKey(TString key, TQuery *query, TRequestContext::TPtr context);
        void ProcessResponse(ui64 /*tag*/, TRequestContext::TPtr /*context*/, TResponse response) override;
    };

} // NKikimr::NBlobDepot
