#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ApplyCacheUpdates(NKikimrBlobStorage::TCacheUpdate *cacheUpdate, ui32 senderNodeId) {
        NKikimrBlobStorage::TCacheUpdate updates;

        for (const auto& item : cacheUpdate->GetKeyValuePairs()) {
            const auto [it, inserted] = Cache.try_emplace(item.GetKey());
            TCacheItem& cacheItem = it->second;
            auto newValue = item.HasValue() ? std::make_optional(item.GetValue()) : std::nullopt;
            if (inserted || cacheItem.Generation < item.GetGeneration()) {
                cacheItem.Generation = item.GetGeneration();
                cacheItem.Value = std::move(newValue);
                AddCacheUpdate(&updates, it, true);

                // notify subscribers
                for (auto pos = CacheSubscriptions.lower_bound(std::make_tuple(it->first, TActorId()));
                        pos != CacheSubscriptions.end() && std::get<0>(*pos) == it->first; ++pos) {
                    const auto& [key, actorId] = *pos;
                    Send(actorId, new TEvNodeWardenQueryCacheResult(item.GetKey(), item.HasValue()
                        ? std::make_optional(std::make_tuple(item.GetGeneration(), item.GetValue()))
                        : std::nullopt));
                }
            } else if (cacheItem.Generation == item.GetGeneration()) {
                auto printOptionalString = [](const std::optional<TString>& res) -> TString {
                    if (res) {
                        return HexEncode(*res);
                    } else {
                        return "<nullopt>";
                    }
                };
                Y_VERIFY_DEBUG_S(cacheItem.Value == newValue, "CachedItem# " <<
                        printOptionalString(cacheItem.Value) << " NewItem# " << printOptionalString(newValue));
                Y_UNUSED(printOptionalString);  // for release build
            }
        }

        if (!IsSelfStatic) {
            return; // nothing to do for dynamic nodes
        }

        // propagate forward, if bound
        if (Binding && Binding->SessionId && Binding->NodeId != senderNodeId) {
            auto ev = std::make_unique<TEvNodeConfigPush>();
            ev->Record.MutableCacheUpdate()->CopyFrom(updates);
            SendEvent(*Binding, std::move(ev));
        }
        // propagate backwards
        for (auto& [nodeId, info] : DirectBoundNodes) {
            auto ev = std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), nullptr);
            info.LastReportedRootNodeId = GetRootNodeId();
            ev->Record.MutableCacheUpdate()->CopyFrom(updates);
            SendEvent(nodeId, info, std::move(ev));
        }
        // propagate to connected dynamic nodes
        for (const auto& [sessionId, actorId] : DynamicConfigSubscribers) {
            auto ev = std::make_unique<TEvNodeWardenDynamicConfigPush>();
            ev->Record.MutableCacheUpdate()->CopyFrom(updates);
            auto handle = std::make_unique<IEventHandle>(actorId, SelfId(), ev.release());
            handle->Rewrite(TEvInterconnect::EvForward, sessionId);
            TActivationContext::Send(handle.release());
        }
    }

    void TDistributedConfigKeeper::AddCacheUpdate(NKikimrBlobStorage::TCacheUpdate *cacheUpdate,
            THashMap<TString, TCacheItem>::const_iterator it, bool addValue) {
        auto *kvp = cacheUpdate->AddKeyValuePairs();
        kvp->SetKey(it->first);
        kvp->SetGeneration(it->second.Generation);
        if (const auto& value = it->second.Value; value && addValue) {
            kvp->SetValue(*value);
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenUpdateCache::TPtr ev) {
        ApplyCacheUpdates(&ev->Get()->CacheUpdate, 0);
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenQueryCache::TPtr ev) {
        auto& msg = *ev->Get();
        if (msg.Subscribe) {
            CacheSubscriptions.emplace(msg.Key, ev->Sender);
        }
        const auto it = Cache.find(msg.Key);
        Send(ev->Sender, new TEvNodeWardenQueryCacheResult(msg.Key, it != Cache.end() && it->second.Value
            ? std::make_optional(std::make_tuple(it->second.Generation, *it->second.Value))
            : std::nullopt));
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenUnsubscribeFromCache::TPtr ev) {
        CacheSubscriptions.erase(std::make_tuple(ev->Get()->Key, ev->Sender));
    }

} // NKikimr::NStorage
