#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::TBlobMappingCache::HandleResolveResult(const NKikimrBlobDepot::TEvResolveResult& msg) {
        for (const auto& item : msg.GetResolvedKeys()) {
            TString key = item.GetKey();
            const auto [it, inserted] = Cache.try_emplace(std::move(key));
            auto& entry = it->second;
            if (inserted) {
                entry.Key = it->first;
            }
            entry.Values = item.GetValueChain();
            Queue.PushBack(&entry);

            entry.ResolveInFlight = false;

            for (TQueryWaitingForKey& item : std::exchange(entry.QueriesWaitingForKey, {})) {
                Agent.OnRequestComplete(item.Id, TKeyResolved{&entry.Values}, Agent.OtherRequestInFlight);
            }
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
            item->SetBeginningKey(it->first);
            item->SetEndingKey(it->first);
            item->SetIncludeEnding(true);

            if (Agent.VirtualGroupId) {
                const auto& id = TLogoBlobID::FromBinary(it->first);
                item->SetTabletId(id.TabletID());
            }

            Agent.Issue(std::move(msg), this, nullptr);
        }

        const ui64 id = Agent.NextRequestId++;
        auto queryIt = entry.QueriesWaitingForKey.emplace(entry.QueriesWaitingForKey.end(), query, id);
        auto cancelCallback = [&entry, queryIt] {
            entry.QueriesWaitingForKey.erase(queryIt);
        };
        Agent.RegisterRequest(id, query, std::move(context), std::move(cancelCallback), false);

        return nullptr;
    }

    void TBlobDepotAgent::TBlobMappingCache::ProcessResponse(ui64 /*tag*/, TRequestContext::TPtr /*context*/, TResponse response) {
        if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
            HandleResolveResult((*p)->Record);
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
