#pragma once

#include "schemeshard_path_db_ref.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <functional>
#include <memory>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

namespace NDbRefDetail {
    template <class P> struct TConstView;
    template <class T> struct TConstView<TIntrusivePtr<T>> { using type = TIntrusiveConstPtr<T>; };
    template <class T> struct TConstView<TIntrusiveConstPtr<T>> { using type = TIntrusiveConstPtr<T>; };
    template <class T> struct TConstView<std::shared_ptr<T>> { using type = std::shared_ptr<const T>; };
}

// Teardown interface: maps self-register so Clear() iterates one registry.
class IDbRefMap {
public:
    virtual ~IDbRefMap() = default;
    virtual void clear() = 0;

    // Debug: every entry points at a live path.
    virtual void DebugCheckConsistency(const std::function<bool(const TPathId&)>& pathExists) const = 0;

    // Debug: feed each held pathId to the DbRefCount reconciliation.
    virtual void DebugForEachRef(const std::function<void(const TPathId&)>& fn) const = 0;
};

// THashMap<TPathId, V> holding a DbRefCount self-ref per entry: insert acquires,
// erase releases. No operator[], so a missing-key read can't silently acquire.
// Proposal rollback is coordinated at explicit Grab* call sites. The container
// itself only accounts for membership references.
template <class V>
class TDbRefMap : public IDbRefMap {
    using TInner = THashMap<TPathId, V>;

public:
    using iterator = typename TInner::iterator;
    using const_iterator = typename TInner::const_iterator;
    using value_type = typename TInner::value_type;
    using TConstView = typename NDbRefDetail::TConstView<V>::type;

    // Self-registers at construction (registration can't be missed); `reason`
    // is the map's name, logged on each DbRefCount change.
    TDbRefMap(TRefLabel reason, TSchemeShard* ss, TVector<IDbRefMap*>& registry)
        : Reason(reason)
        , SS(ss)
    {
        registry.push_back(this);
    }

    // Non-copyable/movable: registered by address; a copy would double-acquire.
    TDbRefMap(const TDbRefMap&) = delete;
    TDbRefMap& operator=(const TDbRefMap&) = delete;
    TDbRefMap(TDbRefMap&&) = delete;
    TDbRefMap& operator=(TDbRefMap&&) = delete;

    // Teardown drops entries without releasing (the counters die with the shard).
    ~TDbRefMap() override = default;

    const TInner& AsMap() const {
        return Map;
    }

    // Insert/assign, acquiring exactly one reference on a new key.
    V& Set(const TPathId& id, V value) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, std::move(value)).first;
            AcquirePathDbRef(SS, id, Reason);
        } else {
            it->second = std::move(value);
        }
        return it->second;
    }

    // Acquires on new key, no undo. SubDomains-only.
    V& Emplace(const TPathId& id) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, V{}).first;
            AcquirePathDbRef(SS, id, Reason);
        }
        return it->second;
    }

    // Mutable pointee access. Snapshotting is the caller's responsibility.
    // The slot cannot be replaced through this reference.
    const V& Update(const TPathId& id) {
        return Map.at(id);
    }

    // Remove membership and release its reference.
    size_t erase(const TPathId& id) {
        if (Map.contains(id)) {
            ReleasePathDbRef(SS, id, Reason);
        }
        return Map.erase(id);
    }

    // Read-only: const pointee, so at(id)->Mutate() won't compile; mutate via Update().
    TConstView at(const TPathId& id) const { return Map.at(id); }

    // Read accessors are const-only: they never hand out a mutable slot, so a caller
    // can't reseat an entry (it->second = newPtr) and desync the self-ref. The
    // const_iterator/const V* still permit pointee mutation (->Field); the sanctioned
    // mutation gates are Set/Update.
    const_iterator find(const TPathId& id) const { return Map.find(id); }
    const V* FindPtr(const TPathId& id) const { return Map.FindPtr(id); }
    V Value(const TPathId& id, const V& def) const { return Map.Value(id, def); }
    bool contains(const TPathId& id) const { return Map.contains(id); }
    size_t count(const TPathId& id) const { return Map.count(id); }
    size_t size() const { return Map.size(); }
    bool empty() const { return Map.empty(); }
    const_iterator begin() const { return Map.begin(); }
    const_iterator end() const { return Map.end(); }

    // Drop everything without releasing (TSchemeShard::Clear teardown).
    void clear() override {
        Map.clear();
    }

    void DebugCheckConsistency(const std::function<bool(const TPathId&)>& pathExists) const override {
        for (const auto& [id, value] : Map) {
            Y_VERIFY_DEBUG_S(pathExists(id), "self-ref for pathId " << id << " absent from PathsById");
        }
    }

    void DebugForEachRef(const std::function<void(const TPathId&)>& fn) const override {
        for (const auto& [id, value] : Map) {
            fn(id);
        }
    }

    // Restore membership after an external snapshot has already restored path
    // counters. Ordinary Set/erase would account for those references twice.
    // A null value marks an entry that did not exist in the snapshot.
    void RestoreMembershipWithoutRefcount(const TPathId& id, V value) {
        if (value) {
            Map[id] = std::move(value);
        } else {
            Map.erase(id);
        }
    }

private:
    TRefLabel Reason;
    TSchemeShard* SS = nullptr;
    TInner Map;
};

} // NKikimr::NSchemeShard
