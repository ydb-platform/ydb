#pragma once

#include "defs.h"
#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent::TBlobMappingCache
        : public TRequestSender
        , public std::enable_shared_from_this<TBlobMappingCache>
    {
        struct TCachedKeyItem : TIntrusiveListItem<TCachedKeyItem> {
            TStringBuf Key; // key buffer (view of key part of the Cache set)
            TResolvedValue Value; // recently resolved value, if any
            ui32 OrdinaryResolvePending = 0;
            ui32 MustRestoreFirstResolvePending = 0;
            THashMap<ui64, bool> PendingQueries; // a set of queries waiting for this blob

            TCachedKeyItem(TStringBuf key)
                : Key(key)
            {}
        };

        THashMap<TString, TCachedKeyItem> Cache;
        TIntrusiveList<TCachedKeyItem> Queue;

    public:
        TBlobMappingCache(TBlobDepotAgent& agent)
            : TRequestSender(agent)
        {}

        void HandleResolveResult(ui64 tag, const NKikimrBlobDepot::TEvResolveResult& msg, TRequestContext::TPtr context);
        const TResolvedValue *ResolveKey(TString key, TQuery *query, TRequestContext::TPtr context, bool mustRestoreFirst);

    private:
        void ProcessResponse(ui64 tag, TRequestContext::TPtr /*context*/, TResponse response) override;
    };

} // NKikimr::NBlobDepot
