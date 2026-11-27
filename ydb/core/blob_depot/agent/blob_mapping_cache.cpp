#include "blob_mapping_cache.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    struct TResolveContext : TRequestContext {
        TString Key;
        bool MustRestoreFirst;

        TResolveContext(TString key, bool mustRestoreFirst)
            : Key(std::move(key))
            , MustRestoreFirst(mustRestoreFirst)
        {}
    };

    void TBlobDepotAgent::TBlobMappingCache::CheckRouteRevision(TCachedKeyItem& entry) {
        if (entry.RouteRevision != RouteRevision) {
            // Invalidate lazily so removing S3 does not scan the whole cache on the agent mailbox.
            if (AnyOf(entry.Value.Chain, [](const auto& link) { return link.S3Locator.has_value(); })) {
                entry.Value = {};
            }
            entry.RouteRevision = RouteRevision;
        }
    }

    void TBlobDepotAgent::TBlobMappingCache::HandleResolveResult(ui64 tag, const NKikimrBlobDepot::TEvResolveResult& msg,
            TRequestContext::TPtr context) {
        YDB_LOG_DEBUG("HandleResolveResult",
            {"marker", "BDA28"},
            {"agentId", Agent.LogId},
            {"cookie", tag},
            {"msg", msg});

        auto process = [&](const auto& item, bool nodata) {
            // check if there is an error or no data attached
            if (item.HasErrorReason() || item.GetValueChain().empty() || nodata) {
                YDB_LOG_DEBUG("HandleResolveResult error",
                    {"marker", "BDA43"},
                    {"agentId", Agent.LogId},
                    {"item", item},
                    {"noData", nodata},
                    {"key", Agent.PrettyKey(item.GetKey())});

                const TString errorReason = item.HasErrorReason() ? item.GetErrorReason() : "no data attached to the key";
                const auto it = Cache.find(item.GetKey());
                if (it != Cache.end()) {
                    for (const auto& [id, mustRestoreFirst] : it->second.PendingQueries) {
                        Agent.OnRequestComplete(id, nodata ? TKeyResolved(nullptr) :
                            TKeyResolved(TKeyResolved::ResolutionError, errorReason),
                            Agent.OtherRequestInFlight);
                    }
                    Cache.erase(it);
                }
            } else {
                TString key = item.GetKey();
                TStringBuf keyBuf = key;
                const auto [it, inserted] = Cache.try_emplace(std::move(key), keyBuf);
                auto& [key_, entry] = *it;
                CheckRouteRevision(entry);

                YDB_LOG_DEBUG("HandleResolveResult success",
                    {"marker", "BDA44"},
                    {"agentId", Agent.LogId},
                    {"item", item},
                    {"noData", nodata},
                    {"key", Agent.PrettyKey(key_)},
                    {"currentValue", entry.Value});

                // update value if it supersedes current one
                if (TResolvedValue value(item); value.Supersedes(entry.Value)) {
                    entry.Value = std::move(value);
                }

                // notify matching queries about this
                for (auto it = entry.PendingQueries.begin(); it != entry.PendingQueries.end(); ) {
                    const auto& [id, mustRestoreFirst] = *it;
                    if (mustRestoreFirst <= entry.Value.ReliablyWritten) {
                        Agent.OnRequestComplete(id, TKeyResolved(&entry.Value), Agent.OtherRequestInFlight);
                        entry.PendingQueries.erase(it++);
                    } else {
                        ++it;
                    }
                }

                if (context) {
                    auto& resolveContext = context->Obtain<TResolveContext>();
                    if (resolveContext.MustRestoreFirst) {
                        --entry.MustRestoreFirstResolvePending;
                    } else {
                        --entry.OrdinaryResolvePending;
                    }
                }

                Queue.PushBack(&entry);
                while (Cache.size() > 1'000'000) {
                    auto& front = *Queue.Front();
                    Cache.erase(front.Key);
                }
            }
        };

        if (msg.GetStatus() != NKikimrProto::OK && msg.GetStatus() != NKikimrProto::OVERRUN) {
            if (context) {
                NKikimrBlobDepot::TEvResolveResult::TResolvedKey item;
                item.SetKey(context->Obtain<TResolveContext>().Key);
                item.SetErrorReason(msg.GetErrorReason() ? msg.GetErrorReason()
                    : NKikimrProto::EReplyStatus_Name(msg.GetStatus()));
                process(item, false);
            }
            return; // a failed lookup does not prove absence and must not populate the cache
        }

        if (!context) {
            // aside requests -- just add received data to cache
            for (const auto& item : msg.GetResolvedKeys()) {
                process(item, false);
            }
        } else {
            auto& resolveContext = context->Obtain<TResolveContext>();
            if (msg.ResolvedKeysSize() == 1) {
                const auto& item = msg.GetResolvedKeys(0);
                Y_ABORT_UNLESS(item.GetKey() == resolveContext.Key);
                process(item, false);
            } else if (msg.ResolvedKeysSize() == 0) {
                NKikimrBlobDepot::TEvResolveResult::TResolvedKey item;
                item.SetKey(resolveContext.Key);
                process(item, true);
            } else {
                Y_ABORT("unexpected resolve response");
            }
        }
    }

    void TBlobDepotAgent::TBlobMappingCache::RefreshAfterMissing(const TString& key, const TResolvedValue& stale,
            const TResolvedValue& current) {
        if (const auto it = Cache.find(key); it != Cache.end()) {
            CheckRouteRevision(it->second);
            auto& cached = it->second.Value;
            if (cached.Defined && cached == stale) {
                cached = current;
            }
        }
    }

    const TResolvedValue *TBlobDepotAgent::TBlobMappingCache::ResolveKey(TString key, TQuery *query,
            TRequestContext::TPtr context, bool mustRestoreFirst, NKikimrBlobStorage::TDataKind::E dataKind) {
        // obtain entry in the cache
        const TStringBuf keyBuf = key;
        const auto [it, inserted] = Cache.try_emplace(std::move(key), keyBuf);
        auto& entry = it->second;
        CheckRouteRevision(entry);

        YDB_LOG_DEBUG("ResolveKey",
            {"marker", "BDA45"},
            {"agentId", Agent.LogId},
            {"key", Agent.PrettyKey(it->first)},
            {"mustRestoreFirst", mustRestoreFirst},
            {"value", entry.Value},
            {"ordinaryResolvePending", entry.OrdinaryResolvePending},
            {"mustRestoreFirstResolvePending", entry.MustRestoreFirstResolvePending});

        // return cached value if we have all conditions met for this query
        if (!entry.Value.IsEmpty() && mustRestoreFirst <= entry.Value.ReliablyWritten) {
            return &entry.Value;
        }

        // register query-local request for the key
        const ui64 id = Agent.NextOtherRequestId++;
        const bool inserted1 = entry.PendingQueries.emplace(id, mustRestoreFirst).second;
        Y_ABORT_UNLESS(inserted1);
        auto cancelCallback = [&entry, id, self = weak_from_this()] {
            if (!self.expired()) {
                const size_t numErased = entry.PendingQueries.erase(id);
                Y_ABORT_UNLESS(numErased);
            }
        };
        Agent.RegisterRequest(id, query, std::move(context), std::move(cancelCallback), false);

        // see if we have to issue the query
        if (!entry.MustRestoreFirstResolvePending && (mustRestoreFirst || !entry.OrdinaryResolvePending)) {
            NKikimrBlobDepot::TEvResolve msg;
            auto *item = msg.AddItems();
            item->SetExactKey(TString(keyBuf));
            if (mustRestoreFirst != item->GetMustRestoreFirst()) {
                item->SetMustRestoreFirst(mustRestoreFirst);
            }
            // All waiters for this blob share its tablet's data kind. The query that issues the resolve
            // supplies its deadline and bridge accounting, including when other queries join later.
            query->SetResolveParameters(msg, dataKind);

            Agent.Issue(std::move(msg), this, std::make_unique<TResolveContext>(TString(keyBuf), mustRestoreFirst));
            ++(mustRestoreFirst ? entry.MustRestoreFirstResolvePending : entry.OrdinaryResolvePending);
        }

        return nullptr;
    }

    void TBlobDepotAgent::TBlobMappingCache::ProcessResponse(ui64 tag, TRequestContext::TPtr context, TResponse response) {
        if (auto *p = std::get_if<TEvBlobDepot::TEvResolveResult*>(&response)) {
            HandleResolveResult(tag, (*p)->Record, std::move(context));
        } else if (std::holds_alternative<TTabletDisconnected>(response)) {
            YDB_LOG_DEBUG("TBlobMappingCache::TTabletDisconnected",
                {"marker", "BDA38"},
                {"agentId", Agent.LogId},
                {"cookie", tag});
            auto& resolveContext = context->Obtain<TResolveContext>();
            if (const auto it = Cache.find(resolveContext.Key); it != Cache.end()) {
                for (const auto& [id, mustRestoreFirst] : std::exchange(it->second.PendingQueries, {})) {
                    Agent.OnRequestComplete(id, TKeyResolved(TKeyResolved::ResolutionError, "BlobDepot tablet disconnected"),
                        Agent.OtherRequestInFlight);
                }
                if (!it->second.Value.IsEmpty()) {
                    --(resolveContext.MustRestoreFirst
                        ? it->second.MustRestoreFirstResolvePending
                        : it->second.OrdinaryResolvePending);
                } else {
                    Cache.erase(it);
                }
            }
        } else {
            Y_ABORT();
        }
    }

} // NKikimr::NBlobDepot
