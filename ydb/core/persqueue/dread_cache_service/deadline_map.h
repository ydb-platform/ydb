#pragma once

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>

namespace NKikimr::NPQ {

/**
 * Hash map with TTL expiry via a deadline queue.
 *
 * TValue must expose a public TInstant Deadline field.
 *
 * TTL is fixed (DefaultTtl). The expiry queue relies on nondecreasing deadlines
 * for Insert/FindOrInsert with a nondecreasing `now` (FIFO Expire is correct then).
 * TouchDeadline may enqueue a later deadline; older queue entries become stale.
 *
 * - Insert sets Deadline = now + DefaultTtl once and enqueues (key, deadline).
 *   Returns false if the key already exists (existing entry is unchanged).
 * - FindOrInsert returns a reference to the existing entry, or inserts and
 *   returns the new one. Does not overwrite value/deadline on hit.
 * - TouchDeadline moves Deadline forward (max with now + DefaultTtl) and enqueues
 *   again; older queue entries are skipped on Expire.
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

    TValue* Find(const TKey& key) {
        return Map.FindPtr(key);
    }

    const TValue* Find(const TKey& key) const {
        return Map.FindPtr(key);
    }

    // Inserts a new key. Sets Deadline = now + DefaultTtl. Returns false if key exists.
    bool Insert(const TKey& key, TValue value, TInstant now) {
        if (Map.FindPtr(key)) {
            return false;
        }
        value.Deadline = now + DefaultTtl;
        const TInstant deadline = value.Deadline;
        auto [it, inserted] = Map.emplace(key, std::move(value));
        if (!inserted) {
            return false;
        }
        Queue.push_back(TQueueItem{deadline, key});
        return true;
    }

    // Returns existing entry, or inserts value with Deadline = now + DefaultTtl.
    TValue& FindOrInsert(const TKey& key, TValue value, TInstant now) {
        if (TValue* existing = Find(key)) {
            return *existing;
        }
        value.Deadline = now + DefaultTtl;
        const TInstant deadline = value.Deadline;
        auto [it, inserted] = Map.emplace(key, std::move(value));
        if (!inserted) {
            return it->second;
        }
        Queue.push_back(TQueueItem{deadline, key});
        return it->second;
    }

    // Moves Deadline forward to Max(old, now + DefaultTtl). Returns false if key is missing.
    bool TouchDeadline(const TKey& key, TInstant now) {
        TValue* value = Find(key);
        if (!value) {
            return false;
        }
        const TInstant next = now + DefaultTtl;
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
