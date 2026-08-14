#pragma once

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/system/yassert.h>

namespace NKikimr::NPQ {

/**
 * Hash map with TTL expiry via a deadline queue.
 *
 * TValue must expose a public TInstant Deadline field.
 *
 * - TryInsert sets Deadline = now + ttl once and enqueues (key, deadline).
 * - TouchDeadline moves Deadline forward (max with now+ttl) and enqueues again;
 *   older queue entries become stale and are skipped on Expire.
 * - Erase removes only from the map; queue entries are dropped lazily.
 * - Expire pops queue while front.Deadline <= now; erases the map entry only when
 *   HashMap[key].Deadline == front.Deadline.
 *
 * Wakeup scheduling is the caller's responsibility (e.g. every ~1 minute).
 */
template <typename TKey, typename TValue, typename THash = THash<TKey>>
class TDeadlineMap {
public:
    static constexpr TDuration DefaultTtl = TDuration::Minutes(5);

    // Inserts a new key. Sets Deadline = now + ttl. Returns nullptr if key exists.
    TValue* TryInsert(const TKey& key, TValue value, TInstant now, TDuration ttl = DefaultTtl) {
        if (Map.FindPtr(key)) {
            return nullptr;
        }
        value.Deadline = now + ttl;
        const TInstant deadline = value.Deadline;
        auto [it, inserted] = Map.emplace(key, std::move(value));
        Y_ABORT_UNLESS(inserted);
        Queue.push_back(TQueueItem{deadline, key});
        return &it->second;
    }

    TValue* Find(const TKey& key) {
        return Map.FindPtr(key);
    }

    const TValue* Find(const TKey& key) const {
        return Map.FindPtr(key);
    }

    // Moves Deadline forward to Max(old, now + ttl). Returns false if key is missing.
    bool TouchDeadline(const TKey& key, TInstant now, TDuration ttl = DefaultTtl) {
        TValue* value = Find(key);
        if (!value) {
            return false;
        }
        const TInstant next = now + ttl;
        if (next <= value->Deadline) {
            return true;
        }
        value->Deadline = next;
        Queue.push_back(TQueueItem{value->Deadline, key});
        return true;
    }

    // Removes from the map only; matching queue entries are skipped later by Expire.
    bool Erase(const TKey& key) {
        return Map.erase(key) > 0;
    }

    // Drops expired entries. Returns how many map entries were erased.
    size_t Expire(TInstant now) {
        size_t erased = 0;
        while (!Queue.empty() && Queue.front().Deadline <= now) {
            const TQueueItem item = Queue.front();
            Queue.pop_front();
            auto it = Map.find(item.Key);
            if (it.IsEnd()) {
                continue;
            }
            if (it->second.Deadline == item.Deadline) {
                Map.erase(it);
                ++erased;
            }
        }
        return erased;
    }

    size_t Size() const {
        return Map.size();
    }

    bool Empty() const {
        return Map.empty();
    }

    auto begin() {
        return Map.begin();
    }
    auto end() {
        return Map.end();
    }
    auto begin() const {
        return Map.begin();
    }
    auto end() const {
        return Map.end();
    }

private:
    struct TQueueItem {
        TInstant Deadline;
        TKey Key;
    };

    THashMap<TKey, TValue, THash> Map;
    TDeque<TQueueItem> Queue;
};

} // namespace NKikimr::NPQ
