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
            std::optional<TValueChain> Values;
            bool ResolveInFlight = false;
            std::list<TQueryWaitingForKey> QueriesWaitingForKey;
        };

        THashMap<TString, TCachedKeyItem> Cache;
        TIntrusiveList<TCachedKeyItem> Queue;

    public:
        TBlobMappingCache(TBlobDepotAgent& agent)
            : TRequestSender(agent)
        {}

        void HandleResolveResult(const NKikimrBlobDepot::TEvResolveResult& msg) {
            for (const auto& item : msg.GetResolvedKeys()) {
                TString key = item.GetKey();
                const auto [it, inserted] = Cache.try_emplace(std::move(key));
                auto& entry = it->second;
                if (inserted) {
                    entry.Key = it->first;
                }
                entry.Values.emplace(item.GetValueChain());
                Queue.PushBack(&entry);

                entry.ResolveInFlight = false;

                for (TQueryWaitingForKey& item : std::exchange(entry.QueriesWaitingForKey, {})) {
                    Agent.OnRequestComplete(item.Id, TKeyResolved{&entry.Values.value()}, Agent.OtherRequestInFlight);
                }
            }
        }

        const TValueChain *ResolveKey(TString key, TQuery *query, TRequestContext::TPtr context) {
            const auto [it, inserted] = Cache.try_emplace(std::move(key));
            auto& entry = it->second;
            if (inserted) {
                entry.Key = it->first;
            }
            if (entry.Values) {
                return &entry.Values.value();
            }
            if (!entry.ResolveInFlight) {
                entry.ResolveInFlight = true;

                NKikimrBlobDepot::TEvResolve msg;
                auto *item = msg.AddItems();
                item->SetBeginningKey(it->first);
                item->SetEndingKey(it->first);
                item->SetIncludeEnding(true);
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

        void ProcessResponse(ui64 /*tag*/, TRequestContext::TPtr /*context*/, TResponse response) {
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
    };

    TBlobDepotAgent::TBlobMappingCachePtr TBlobDepotAgent::CreateBlobMappingCache() {
        return {new TBlobMappingCache{*this}, std::default_delete<TBlobMappingCache>{}};
    }

    void TBlobDepotAgent::HandleResolveResult(const NKikimrBlobDepot::TEvResolveResult& msg) {
        BlobMappingCache->HandleResolveResult(msg);
    }

    const TValueChain *TBlobDepotAgent::ResolveKey(TString key, TQuery *query, TRequestContext::TPtr context) {
        return BlobMappingCache->ResolveKey(std::move(key), query, std::move(context));
    }

} // NKikimr::NBlobDepot
