#pragma once

#include "schemeshard_path_ref.h"
#include "schemeshard__operation_memory_changes.h"

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

// Clone a value for an undo snapshot; TTableInfo uses DeepCopy (COW-shares its
// partitioning), other types copy-construct.
template <class T>
TIntrusivePtr<T> DbRefUndoClone(const TIntrusivePtr<T>& p) {
    return p ? TIntrusivePtr<T>(new T(*p)) : TIntrusivePtr<T>();
}
template <class T>
std::shared_ptr<T> DbRefUndoClone(const std::shared_ptr<T>& p) {
    return p ? std::make_shared<T>(*p) : std::shared_ptr<T>();
}
inline TIntrusivePtr<TTableInfo> DbRefUndoClone(const TIntrusivePtr<TTableInfo>& p) {
    return p ? TTableInfo::DeepCopy(*p) : TIntrusivePtr<TTableInfo>();
}

// Restore an Update() snapshot on abort. Default: swap the pointer back.
// TTableInfo restores contents into the still-live object instead, because
// TTLEnabledTables aliases that object by raw pointer and must stay in sync;
// operator= on the TSimpleRefCount base is a no-op, so the live refcount is kept.
template <class V>
void DbRefUndoRestoreSlot(V& slot, const V& snap) {
    slot = snap;
}
inline void DbRefUndoRestoreSlot(TIntrusivePtr<TTableInfo>& slot, const TIntrusivePtr<TTableInfo>& snap) {
    if (slot && snap) {
        *slot = *snap;
    } else {
        slot = snap;
    }
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
// Set/Update record undo and belong to the armed propose phase; SetUntracked/
// UpdateUntracked/erase are for other phases (init, plan-step, progress, stats).
template <class V>
class TDbRefMap : public IDbRefMap {
    using TInner = THashMap<TPathId, V>;

public:
    using iterator = typename TInner::iterator;
    using const_iterator = typename TInner::const_iterator;
    using value_type = typename TInner::value_type;
    using TConstView = typename NDbRefDetail::TConstView<V>::type;

    // Designated-initializer args: Foo.Set({.Path=id, .Value=info, .Changes=ctx.MemChanges}).
    struct TSetArgs {
        TPathId Path;
        V Value;
        TMemoryChanges& Changes;
    };

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

    // Insert/assign (acquires on new key) and record the matching undo.
    V& Set(TSetArgs args) {
        Y_VERIFY_DEBUG_S(args.Changes.IsArmed(),
            "tracked Set on " << Reason.c_str() << " outside an armed propose; use SetUntracked");
        auto it = Map.find(args.Path);
        if (it == Map.end()) {
            Y_VERIFY_DEBUG_S(args.Changes.IsPathTracked(args.Path),
                "Set(" << Reason.c_str() << ") acquires a ref on " << args.Path
                << " but the path was not grabbed in this tx; GrabNewPath/GrabPath it first");
            args.Changes.RecordDbRefUndo([this, id = args.Path]() { UndoErase(id); });
            it = Map.emplace(args.Path, std::move(args.Value)).first;
            AcquirePathDbRef(SS, args.Path, Reason);
        } else {
            args.Changes.RecordDbRefUndo([this, id = args.Path, old = it->second]() { UndoRestore(id, old); });
            it->second = std::move(args.Value);
        }
        return it->second;
    }

    // Insert/assign, acquiring on new key, without recording undo (init restore, SubDomains).
    V& SetUntracked(const TPathId& id, V value) {
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
    V& EmplaceUntracked(const TPathId& id) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, V{}).first;
            AcquirePathDbRef(SS, id, Reason);
        }
        return it->second;
    }

    // Mutable access that records a deduped undo snapshot. The only tracked way to mutate.
    // Returns const V&: the pointee stays mutable (->Field), but the slot can't be
    // reseated (Update(id,mc) = newPtr won't compile), which would desync the undo.
    const V& Update(const TPathId& id, TMemoryChanges& changes) {
        Y_VERIFY_DEBUG_S(changes.IsArmed(),
            "tracked Update on " << Reason.c_str() << " outside an armed propose; use UpdateUntracked");
        V& slot = Map.at(id);
        if (changes.NeedsUpdateSnapshot(this, id)) {
            changes.RecordDbRefUndo([this, id, snap = DbRefUndoClone(slot)]() {
                UndoRestoreInPlace(id, snap);
            });
        }
        return slot;
    }

    // Pointee-mutable access without undo, for non-transactional callers (init, stats).
    // Const V& (no slot reseat); operations use Update() so their mutation is undoable.
    const V& UpdateUntracked(const TPathId& id) {
        return Map.at(id);
    }

    // No undo: only legal outside an armed propose (an armed-propose erase can't roll back).
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
    // mutation gates are Set/Update/UpdateUntracked.
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

private:
    // Undo is driven only by TMemoryChanges; ops must never call these.
    friend class TMemoryChanges;

    // Set-undo: bring back the replaced value's original pointer (the Paths snapshot
    // owns the counter). Used when Set() overwrote an existing entry.
    V& UndoRestore(const TPathId& id, V value) {
        auto& slot = Map[id];
        slot = std::move(value);
        return slot;
    }

    // Update-undo: restore pre-mutation contents into the SAME live object rather than
    // swapping the pointer, so secondary aliases of it (e.g. TTLEnabledTables) don't
    // desync. Falls back to a pointer restore if either side is null.
    void UndoRestoreInPlace(const TPathId& id, const V& snap) {
        auto it = Map.find(id);
        if (it != Map.end()) {
            DbRefUndoRestoreSlot(it->second, snap);
        }
    }

    // Drop a tx-created entry without releasing (Paths owns the counter).
    void UndoErase(const TPathId& id) {
        Map.erase(id);
    }

    TRefLabel Reason;
    TSchemeShard* SS = nullptr;
    TInner Map;
};

} // NKikimr::NSchemeShard
