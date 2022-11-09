#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    struct TResolveContext : TRequestContext {
        TString Key;

        TResolveContext(TString key)
            : Key(std::move(key))
        {}
    };

    void TBlobDepotAgent::TBlobMappingCache::HandleResolveResult(const NKikimrBlobDepot::TEvResolveResult& msg,
            TRequestContext::TPtr context) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA28, "HandleResolveResult", (VirtualGroupId, Agent.VirtualGroupId), (Msg, msg));

        auto process = [&](TString key, const NKikimrBlobDepot::TEvResolveResult::TResolvedKey *item) {
            const auto [it, inserted] = Cache.try_emplace(std::move(key));
            auto& entry = it->second;
            if (inserted) {
                entry.Key = it->first;
            }
            if (item) {
                Y_VERIFY(it->first == item->GetKey());
                entry.Values = item->GetValueChain();
            } else {
                entry.Values.Clear();
            }
            Queue.PushBack(&entry);
            entry.ResolveInFlight = false;
            for (TQueryWaitingForKey& q : std::exchange(entry.QueriesWaitingForKey, {})) {
                Agent.OnRequestComplete(q.Id, TKeyResolved{entry.Values.empty() ? nullptr : &entry.Values,
                    item && item->HasErrorReason() ? std::make_optional(item->GetErrorReason()) : std::nullopt},
                    Agent.OtherRequestInFlight);
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
        const auto [it, inserted] = Cache.try_emplace(std::move(key));
        auto& entry = it->second;
        if (inserted) {
            entry.Key = it->first;
        }
        if (!entry.Values.empty()) {
            return &entry.Values;
        }
        if (!entry.ResolveInFlight) {
            entry.ResolveInFlight = true;

            NKikimrBlobDepot::TEvResolve msg;
            auto *item = msg.AddItems();
            item->SetExactKey(it->first);

            if (Agent.VirtualGroupId) {
                const auto& id = TLogoBlobID::FromBinary(it->first);
                item->SetTabletId(id.TabletID());
            }

            Agent.Issue(std::move(msg), this, std::make_unique<TResolveContext>(it->first));
        }

        const ui64 id = Agent.NextRequestId++;
        auto queryIt = entry.QueriesWaitingForKey.emplace(entry.QueriesWaitingForKey.end(), query, id);
        auto cancelCallback = [&entry, queryIt] {
            entry.QueriesWaitingForKey.erase(queryIt);
        };
        Agent.RegisterRequest(id, query, std::move(context), std::move(cancelCallback), false);

        return nullptr;
    }

    void TBlobDepotAgent::TBlobMappingCache::ProcessResponse(ui64 /*tag*/, TRequestContext::TPtr context, TResponse response) {
        if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
            HandleResolveResult((*p)->Record, std::move(context));
        } else if (std::holds_alternative<TTabletDisconnected>(response)) {
            for (auto& [key, entry] : Cache) {
                if (entry.ResolveInFlight) {
                    for (TQueryWaitingForKey& item : std::exchange(entry.QueriesWaitingForKey, {})) {
                        Agent.OnRequestComplete(item.Id, response, Agent.OtherRequestInFlight);
                    }
                }
            }
        } else {
            Y_FAIL();
        }
    }

} // NKikimr::NBlobDepot
