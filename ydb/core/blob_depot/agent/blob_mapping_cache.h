#pragma once

#include "defs.h"
#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent::TBlobMappingCache
        : public TRequestSender
    {
        struct TCachedKeyItem : TIntrusiveListItem<TCachedKeyItem> {
            TStringBuf Key;
            TResolvedValueChain Values;
            bool ResolveInFlight = false;
            THashSet<ui64> PendingQueries;

            TCachedKeyItem(TStringBuf key)
                : Key(std::move(key))
            {}
        };

        THashMap<TString, TCachedKeyItem> Cache;
        TIntrusiveList<TCachedKeyItem> Queue;

    public:
        TBlobMappingCache(TBlobDepotAgent& agent)
            : TRequestSender(agent)
        {}

        void HandleResolveResult(ui64 tag, const NKikimrBlobDepot::TEvResolveResult& msg, TRequestContext::TPtr context);
        const TResolvedValueChain *ResolveKey(TString key, TQuery *query, TRequestContext::TPtr context, bool force = false);
        void ProcessResponse(ui64 tag, TRequestContext::TPtr /*context*/, TResponse response) override;
    };

} // NKikimr::NBlobDepot
