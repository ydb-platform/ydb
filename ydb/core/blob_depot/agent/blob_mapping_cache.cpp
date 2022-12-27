#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    struct TResolveContext : TRequestContext {
        TString Key;

        TResolveContext(TString key)
            : Key(std::move(key))
        {}
    };

    void TBlobDepotAgent::TBlobMappingCache::HandleResolveResult(ui64 tag, const NKikimrBlobDepot::TEvResolveResult& msg,
            TRequestContext::TPtr context) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA28, "HandleResolveResult", (VirtualGroupId, Agent.VirtualGroupId),
            (Cookie, tag), (Msg, msg));

        auto process = [&](TString key, const NKikimrBlobDepot::TEvResolveResult::TResolvedKey *item) {
            const TStringBuf keyBuf = key;
            TCachedKeyItem& entry = Cache.try_emplace(std::move(key), keyBuf).first->second;
            if (item) {
                Y_VERIFY(entry.Key == item->GetKey());
                entry.Values = item->GetValueChain();
            } else {
                entry.Values.Clear();
            }
            Queue.PushBack(&entry);
            entry.ResolveInFlight = false;
            for (const ui64 id : std::exchange(entry.PendingQueries, {})) {
                Agent.OnRequestComplete(id, TKeyResolved{
                    entry.Values.empty() ? nullptr : &entry.Values,
                    item && item->HasErrorReason() ? std::make_optional(item->GetErrorReason()) : std::nullopt
                }, Agent.OtherRequestInFlight);
            }
        };

        for (const auto& item : msg.GetResolvedKeys()) {
            process(item.GetKey(), &item);
            if (context && context->Obtain<TResolveContext>().Key == item.GetKey()) {
                context.reset();
            }
        }
        if (context) {
            process(context->Obtain<TResolveContext>().Key, nullptr);
        }
    }

    const TResolvedValueChain *TBlobDepotAgent::TBlobMappingCache::ResolveKey(TString key, TQuery *query,
            TRequestContext::TPtr context) {
        const TStringBuf keyBuf = key;
        auto& entry = Cache.try_emplace(std::move(key), keyBuf).first->second;
        if (!entry.Values.empty()) {
            return &entry.Values;
        }
        if (!entry.ResolveInFlight) {
            entry.ResolveInFlight = true;

            NKikimrBlobDepot::TEvResolve msg;
            auto *item = msg.AddItems();
            item->SetExactKey(TString(keyBuf));

            if (Agent.VirtualGroupId) {
                const auto& id = TLogoBlobID::FromBinary(keyBuf);
                item->SetTabletId(id.TabletID());
            }

            Agent.Issue(std::move(msg), this, std::make_unique<TResolveContext>(TString(keyBuf)));
        }

        const ui64 id = Agent.NextOtherRequestId++;
        const bool inserted1 = entry.PendingQueries.emplace(id).second;
        Y_VERIFY(inserted1);
        auto cancelCallback = [&entry, id] {
            const size_t numErased = entry.PendingQueries.erase(id);
            Y_VERIFY(numErased);
        };
        Agent.RegisterRequest(id, query, std::move(context), std::move(cancelCallback), false);

        return nullptr;
    }

    void TBlobDepotAgent::TBlobMappingCache::ProcessResponse(ui64 tag, TRequestContext::TPtr context, TResponse response) {
        if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
            HandleResolveResult(tag, (*p)->Record, std::move(context));
        } else if (std::holds_alternative<TTabletDisconnected>(response)) {
            STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA38, "TBlobMappingCache::TTabletDisconnected",
                (VirtualGroupId, Agent.VirtualGroupId), (Cookie, tag));
            if (auto resolveContext = std::dynamic_pointer_cast<TResolveContext>(context)) {
                if (const auto it = Cache.find(resolveContext->Key); it != Cache.end() && it->second.ResolveInFlight) {
                    for (const ui64 id : std::exchange(it->second.PendingQueries, {})) {
                        Agent.OnRequestComplete(id, response, Agent.OtherRequestInFlight);
                    }
                    it->second.ResolveInFlight = false;
                }
            }
        } else {
            Y_FAIL();
        }
    }

} // NKikimr::NBlobDepot
