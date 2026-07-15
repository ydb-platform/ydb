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

// Maps a value pointer type to its read-only (const-pointee) counterpart, so
// at() can hand out a non-mutating view whatever smart pointer V uses.
namespace NSelfRefDetail {
    template <class P> struct TConstView;
    template <class T> struct TConstView<TIntrusivePtr<T>> { using type = TIntrusiveConstPtr<T>; };
    template <class T> struct TConstView<TIntrusiveConstPtr<T>> { using type = TIntrusiveConstPtr<T>; };
    template <class T> struct TConstView<std::shared_ptr<T>> { using type = std::shared_ptr<const T>; };
}

// Deep-copy a value for an Update() undo snapshot. Generic pointers copy-construct
// the pointee; TTableInfo needs DeepCopy — its Partitions are raw pointers into
// PartitionStore and must be re-pointed at the copy's own store, else a restored
// snapshot dangles.
template <class T>
TIntrusivePtr<T> SelfRefUndoClone(const TIntrusivePtr<T>& p) {
    return p ? TIntrusivePtr<T>(new T(*p)) : TIntrusivePtr<T>();
}
template <class T>
std::shared_ptr<T> SelfRefUndoClone(const std::shared_ptr<T>& p) {
    return p ? std::make_shared<T>(*p) : std::shared_ptr<T>();
}
inline TIntrusivePtr<TTableInfo> SelfRefUndoClone(const TIntrusivePtr<TTableInfo>& p) {
    return p ? TTableInfo::DeepCopy(*p) : TIntrusivePtr<TTableInfo>();
}

// Teardown interface: every TSelfRefMap registers (via SetSchemeShard) so
// TSchemeShard::Clear() iterates one registry instead of a per-map list.
class ISelfRefMap {
public:
    virtual ~ISelfRefMap() = default;
    virtual void clear() = 0;

    // Debug: Refs mirror Map 1:1 and every self-ref points at a live path.
    // Checked after UnDo to catch rollback drift at its cause, not later.
    virtual void DebugCheckConsistency(const std::function<bool(const TPathId&)>& pathExists) const = 0;

    // Debug: feed every held self-ref to a DbRefCount reconciliation pass.
    virtual void DebugForEachRef(const std::function<void(const TPathRef&)>& fn) const = 0;
};

// THashMap<TPathId, V> whose entries own a TPathRef self-ref (insert acquires,
// erase releases). No operator[], so a missing-key read can't silently acquire.
template <class V>
class TSelfRefMap : public ISelfRefMap {
    using TInner = THashMap<TPathId, V>;
    using TElem = std::remove_reference_t<decltype(*std::declval<V&>())>;

public:
    using iterator = typename TInner::iterator;
    using const_iterator = typename TInner::const_iterator;
    using value_type = typename TInner::value_type;
    using TConstView = typename NSelfRefDetail::TConstView<V>::type;

    // Named arguments for Set (C++20 designated initializers at the call site):
    //   Foo.Set({.Path = id, .Value = info, .Changes = context.MemChanges});
    struct TSetArgs {
        TPathId Path;
        V Value;
        TMemoryChanges& Changes;
    };

    // `reason` is this map's name, logged on every DbRefCount change for the
    // self-ref (so traces say "CdcStreams", not a shared "type info record").
    explicit TSelfRefMap(const char* reason)
        : Reason(reason)
    {}

    // Non-copyable, non-movable: refs mirror live DbRefCounts and the map is
    // registered by address, so a copy would double-acquire and orphan the entry.
    TSelfRefMap(const TSelfRefMap&) = delete;
    TSelfRefMap& operator=(const TSelfRefMap&) = delete;
    TSelfRefMap(TSelfRefMap&&) = delete;
    TSelfRefMap& operator=(TSelfRefMap&&) = delete;

    // Registers into the owner's teardown list; without this SS stays null and
    // the first acquire fails loudly, so registration cannot be silently missed.
    void SetSchemeShard(TSchemeShard* ss, TVector<ISelfRefMap*>& registry) {
        SS = ss;
        registry.push_back(this);
    }

    // Whole-map destruction is whole-graph teardown: disarm, don't release
    // against other TSchemeShard members dying in an unspecified order.
    ~TSelfRefMap() override {
        for (auto& [id, ref] : Refs) {
            ref.Disarm();
        }
    }

    // Read-only view for APIs that take a const THashMap& (never for mutation).
    const TInner& AsMap() const {
        return Map;
    }

    // Insert or assign, acquiring when the key is new, and record the matching
    // rollback on Changes in the same act — a mutation can't skip its undo.
    V& Set(TSetArgs args) {
        auto it = Map.find(args.Path);
        if (it == Map.end()) {
            args.Changes.RecordSelfRefUndo([this, id = args.Path]() { UndoErase(id); });
            it = Map.emplace(args.Path, std::move(args.Value)).first;
            Refs.emplace(args.Path, TPathRef(SS, args.Path, Reason));
        } else {
            args.Changes.RecordSelfRefUndo([this, id = args.Path, old = it->second]() { UndoRestore(id, old); });
            it->second = std::move(args.Value);
        }
        return it->second;
    }

    // Acquire-if-new insert or assign WITHOUT recording undo. For init restore
    // and SubDomains (which self-manages rollback via GrabDomain).
    V& SetUntracked(const TPathId& id, V value) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, std::move(value)).first;
            Refs.emplace(id, TPathRef(SS, id, Reason));
        } else {
            it->second = std::move(value);
        }
        return it->second;
    }

    // Insert a default-constructed value if absent (acquiring), without recording
    // an undo — SubDomains-only, paired with a manual GrabDomain like SetUntracked.
    V& EmplaceUntracked(const TPathId& id) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, V{}).first;
            Refs.emplace(id, TPathRef(SS, id, Reason));
        }
        return it->second;
    }

    // Mutable access that records the rollback in the same act, so an in-place
    // mutation (CreateNextVersion, FinishAlter, ...) can't skip its undo. The
    // snapshot is a single struct copy (nested TIntrusivePtr members are shared)
    // and deduped per tx, so it is cheap. The only way to get mutable access.
    V& Update(const TPathId& id, TMemoryChanges& changes) {
        V& slot = Map.at(id);
        if (changes.NeedsUpdateSnapshot(this, id)) {
            changes.RecordSelfRefUndo([this, id, snap = SelfRefUndoClone(slot)]() {
                UndoRestore(id, snap);
            });
        }
        return slot;
    }

    // Mutable access WITHOUT recording undo — for non-transactional callers
    // (init restore, stats, background scans) that have no TMemoryChanges to roll
    // back into. Operations must use Update() so their mutation is undoable.
    V& MutableUntracked(const TPathId& id) {
        return Map.at(id);
    }

    size_t erase(const TPathId& id) {
        Refs.erase(id);
        return Map.erase(id);
    }

    void erase(iterator it) {
        Refs.erase(it->first);
        Map.erase(it);
    }

    // Read-only: the pointee is const, so at(id)->Mutate() won't compile —
    // mutation must go through Update(id, changes) so the undo can't be skipped.
    TConstView at(const TPathId& id) const { return Map.at(id); }
    iterator find(const TPathId& id) { return Map.find(id); }
    const_iterator find(const TPathId& id) const { return Map.find(id); }
    V* FindPtr(const TPathId& id) { return Map.FindPtr(id); }
    const V* FindPtr(const TPathId& id) const { return Map.FindPtr(id); }
    V Value(const TPathId& id, const V& def) const { return Map.Value(id, def); }
    bool contains(const TPathId& id) const { return Map.contains(id); }
    size_t count(const TPathId& id) const { return Map.count(id); }
    size_t size() const { return Map.size(); }
    bool empty() const { return Map.empty(); }
    iterator begin() { return Map.begin(); }
    iterator end() { return Map.end(); }
    const_iterator begin() const { return Map.begin(); }
    const_iterator end() const { return Map.end(); }

    // Disarm all refs then drop everything, for TSchemeShard::Clear() where
    // PathsById is being torn down and refs must not touch it.
    void clear() override {
        for (auto& [id, ref] : Refs) {
            ref.Disarm();
        }
        Refs.clear();
        Map.clear();
    }

    void DebugCheckConsistency(const std::function<bool(const TPathId&)>& pathExists) const override {
        Y_VERIFY_DEBUG_S(Refs.size() == Map.size(),
            "self-ref map Refs/Map size mismatch: " << Refs.size() << " vs " << Map.size());
        for (const auto& [id, ref] : Refs) {
            Y_VERIFY_DEBUG_S(pathExists(id), "self-ref for pathId " << id << " absent from PathsById");
        }
    }

    void DebugForEachRef(const std::function<void(const TPathRef&)>& fn) const override {
        for (const auto& [id, ref] : Refs) {
            fn(ref);
        }
    }

private:
    // Undo is driven only by TMemoryChanges; ops must never call these.
    friend class TMemoryChanges;

    // Restore a snapshot value without acquiring: the Paths snapshot owns the
    // DbRefCount rollback, so the re-added ref is adopted (armed, not acquired).
    V& UndoRestore(const TPathId& id, V value) {
        auto& slot = Map[id];
        slot = std::move(value);
        if (!Refs.contains(id)) {
            Refs.emplace(id, TPathRef(TPathRef::Adopt, SS, id, Reason));
        }
        return slot;
    }

    // Drop a tx-created entry. Disarm not release: Paths owns the counter
    // rollback and PathsById may already be gone here.
    void UndoErase(const TPathId& id) {
        if (auto it = Refs.find(id); it != Refs.end()) {
            it->second.Disarm();
            Refs.erase(it);
        }
        Map.erase(id);
    }

    const char* Reason;
    TSchemeShard* SS = nullptr;
    TInner Map;
    THashMap<TPathId, TPathRef> Refs;
};

} // NKikimr::NSchemeShard
