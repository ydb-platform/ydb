#pragma once

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <stdexcept>
#include <algorithm>

namespace NYql {

// TCheckpointHashMap - hash table with checkpoint/rollback support
//
// This class behaves like a regular THashMap but provides the ability
// to create checkpoints of the current state and rollback to them.
//
// Key methods:
// - Checkpoint() - creates a savepoint of the current state
// - Rollback() - reverts all changes back to the last checkpoint
// - Set(key, value) - sets value for the given key
// - Get(key) - gets value by key (throws exception if key doesn't exist)
// - erase(key) - removes element by key
// - find(key) - finds element, returns const_iterator
//
// Usage example:
//   TCheckpointHashMap<int, string> map;
//   map.Set(1, "hello");
//   map.Checkpoint();
//   map.Set(2, "world");
//   map.Set(1, "modified");
//   map.Rollback();  // map now contains only {1: "hello"}
//
// Alternative with TCheckpointGuard (RAII):
//   {
//       TCheckpointGuard guard(map);
//       map.Set(2, "temp");
//   }  // automatic rollback when leaving scope
//
// WARNING: This class is NOT exception-safe!
// - Internal state corruption is possible during exceptions

template <typename TKey, typename TValue>
class TCheckpointHashMap {
public:
    using key_type = typename THashMap<TKey, TValue>::key_type;
    using mapped_type = typename THashMap<TKey, TValue>::mapped_type;
    using value_type = typename THashMap<TKey, TValue>::value_type;
    using size_type = typename THashMap<TKey, TValue>::size_type;
    using const_iterator = typename THashMap<TKey, TValue>::const_iterator;

    using TUnderlyingMap = THashMap<TKey, TValue>;

    TCheckpointHashMap()
        : CurrentCheckpointId_(0)
    {
    }

    TCheckpointHashMap(const TCheckpointHashMap& other) = delete;

    TCheckpointHashMap(TCheckpointHashMap&& other) = delete;

    TCheckpointHashMap& operator=(const TCheckpointHashMap& other) = delete;

    TCheckpointHashMap& operator=(TCheckpointHashMap&& other) = delete;

    void Checkpoint() {
        ++CurrentCheckpointId_;
    }

    void Rollback() {
        YQL_ENSURE(CurrentCheckpointId_ != 0, "Cannot rollback without checkpoint");

        auto it = Commands_.rbegin();
        while (it != Commands_.rend() && it->first == CurrentCheckpointId_) {
            const auto& cmd = it->second;
            if (cmd.OldValue.Defined()) {
                Map_[cmd.Key] = *cmd.OldValue;
            } else {
                Map_.erase(cmd.Key);
            }
            ++it;
        }

        auto eraseFrom = it.base();
        Commands_.erase(eraseFrom, Commands_.end());

        --CurrentCheckpointId_;
    }

    void Set(const TKey& key, const TValue& value) {
        typename TUnderlyingMap::insert_ctx ctx = nullptr;
        auto it = Map_.find(key, ctx);
        if (CurrentCheckpointId_ != 0) {
            if (it == Map_.end()) {
                Commands_.emplace_back(CurrentCheckpointId_, TUpdateCommand(key, Nothing()));
            } else {
                Commands_.emplace_back(CurrentCheckpointId_, TUpdateCommand(key, it->second));
            }
        }

        if (it == Map_.end()) {
            Map_.emplace_direct(ctx, key, value);
        } else {
            it->second = value;
        }
    }

    const TValue& Get(const TKey& key) const {
        return Map_.at(key);
    }

    size_type erase(const TKey& key) {
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            if (CurrentCheckpointId_ != 0) {
                Commands_.emplace_back(CurrentCheckpointId_, TUpdateCommand(key, it->second));
            }
            Map_.erase(it);
            return 1;
        }
        return 0;
    }

    const_iterator find(const TKey& key) const {
        return Map_.find(key);
    }

    size_type count(const TKey& key) const {
        return Map_.count(key);
    }

    bool contains(const TKey& key) const {
        return Map_.find(key) != Map_.end();
    }

    const_iterator begin() const {
        return Map_.begin();
    }

    const_iterator cbegin() const {
        return Map_.cbegin();
    }

    const_iterator end() const {
        return Map_.end();
    }

    const_iterator cend() const {
        return Map_.cend();
    }

    bool empty() const {
        return Map_.empty();
    }

    size_type size() const {
        return Map_.size();
    }

    size_type max_size() const {
        return Map_.max_size();
    }

    bool operator==(const TCheckpointHashMap& other) const {
        return Map_ == other.Map_;
    }

    bool operator!=(const TCheckpointHashMap& other) const {
        return !(*this == other);
    }

    const THashMap<TKey, TValue>& GetUnderlyingMap() const {
        return Map_;
    }

    size_t GetCommandCount() const {
        return Commands_.size();
    }

private:
    struct TUpdateCommand {
        TKey Key;
        TMaybe<TValue> OldValue;

        TUpdateCommand(TKey key, TMaybe<TValue> oldVal)
            : Key(std::move(key))
            , OldValue(std::move(oldVal))
        {
        }
    };

    using TCommand = TUpdateCommand;

    TUnderlyingMap Map_;
    TVector<std::pair<size_t, TCommand>> Commands_;
    size_t CurrentCheckpointId_;
};

template <typename TKey, typename TValue>
class TCheckpointGuard {
public:
    explicit TCheckpointGuard(TCheckpointHashMap<TKey, TValue>& map)
        : Map_(map)
    {
        Map_.Checkpoint();
    }

    ~TCheckpointGuard() {
        Map_.Rollback();
    }

private:
    TCheckpointHashMap<TKey, TValue>& Map_;
};

} // namespace NYql
