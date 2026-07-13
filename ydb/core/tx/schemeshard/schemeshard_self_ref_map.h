#pragma once

#include "schemeshard_path_ref.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <functional>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

// Teardown interface: every TSelfRefMap registers itself (via SetSchemeShard)
// so TSchemeShard::Clear() disarms+drops all of them by iterating one registry
// instead of a hand-maintained per-map list (which had already drifted).
class ISelfRefMap {
public:
    virtual ~ISelfRefMap() = default;
    virtual void clear() = 0;

    // Debug invariant: Refs mirror Map 1:1 and every self-ref points at a live
    // path. Checked after TMemoryChanges::UnDo to catch rollback drift (a
    // forgotten Grab, a snapshot the undo forgot to reconcile) at the
    // transaction that caused it, not at a distant later decrement.
    virtual void DebugCheckConsistency(const std::function<bool(const TPathId&)>& pathExists) const = 0;
};

// A THashMap<TPathId, V> whose entries own a TPathRef self-reference on their
// path: inserting a new key acquires it, erasing releases it. Replaces the
// manual AcquireSelfDbRef/ReleaseSelfDbRef pairing for path-owning info maps
// (Tables, Topics, CdcStreams, ...). Intentionally has no operator[]: insertion
// goes through Set/Emplace so a read of a missing key can never silently insert
// and acquire a bogus reference.
template <class V>
class TSelfRefMap : public ISelfRefMap {
    using TInner = THashMap<TPathId, V>;

public:
    using iterator = typename TInner::iterator;
    using const_iterator = typename TInner::const_iterator;
    using value_type = typename TInner::value_type;

    // Registers into the owner's teardown list; without this SS stays null and
    // the first acquire fails loudly, so registration cannot be silently missed.
    void SetSchemeShard(TSchemeShard* ss, TVector<ISelfRefMap*>& registry) {
        SS = ss;
        registry.push_back(this);
    }

    // Whole-map destruction is whole-graph teardown: disarm rather than let
    // ~TPathRef release against other TSchemeShard members dying in an
    // unspecified order. Makes teardown self-contained (no reliance on the
    // IsBeingDestroyed guard for these maps).
    ~TSelfRefMap() override {
        for (auto& [id, ref] : Refs) {
            ref.Disarm();
        }
    }

    // Read-only view for APIs that take a const THashMap& (never for mutation).
    operator const TInner&() const {
        return Map;
    }

    // Insert or assign, acquiring the self-ref when the key is new, and record
    // the matching rollback on `changes` in the same act — so a forward mutation
    // can never be applied without its undo (the bug class GrabNew*/Grab* left
    // open). TChanges is a template param only to avoid a header cycle with
    // TMemoryChanges; the sole caller passes TMemoryChanges.
    template <class TChanges>
    V& Set(const TPathId& id, V value, TChanges& changes) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            changes.RecordSelfRef(*this, id, V{});
            it = Map.emplace(id, std::move(value)).first;
            Refs.emplace(id, TPathRef(SS, id, "type info record"));
        } else {
            changes.RecordSelfRef(*this, id, it->second);
            it->second = std::move(value);
        }
        return it->second;
    }

    // Insert or assign, acquiring when new, WITHOUT recording an undo. For init
    // restore (no operation to roll back) and SubDomains (which records its own
    // rollback via TMemoryChanges::GrabDomain because it also refreshes counters).
    V& SetUntracked(const TPathId& id, V value) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, std::move(value)).first;
            Refs.emplace(id, TPathRef(SS, id, "type info record"));
        } else {
            it->second = std::move(value);
        }
        return it->second;
    }

    // Insert a default-constructed value if absent (acquiring), return the ref.
    V& Emplace(const TPathId& id) {
        auto it = Map.find(id);
        if (it == Map.end()) {
            it = Map.emplace(id, V{}).first;
            Refs.emplace(id, TPathRef(SS, id, "type info record"));
        }
        return it->second;
    }

    size_t erase(const TPathId& id) {
        Refs.erase(id);
        return Map.erase(id);
    }

    // Undo-only: restore a snapshot value without acquiring. TMemoryChanges
    // rolls DbRefCount back wholesale via the Paths snapshot, so a re-added
    // entry only needs an armed (adopted) ref so a later real erase releases.
    V& UndoRestore(const TPathId& id, V value) {
        auto& slot = Map[id];
        slot = std::move(value);
        if (!Refs.contains(id)) {
            Refs.emplace(id, TPathRef(TPathRef::Adopt, SS, id, "type info record"));
        }
        return slot;
    }

    // Undo-only: drop an entry created during the tx. Disarm rather than
    // release: the path and its DbRefCount are rolled back by the Paths
    // snapshot, and PathsById may already be gone when this runs.
    void UndoErase(const TPathId& id) {
        if (auto it = Refs.find(id); it != Refs.end()) {
            it->second.Disarm();
            Refs.erase(it);
        }
        Map.erase(id);
    }

    void erase(iterator it) {
        Refs.erase(it->first);
        Map.erase(it);
    }

    V& at(const TPathId& id) { return Map.at(id); }
    const V& at(const TPathId& id) const { return Map.at(id); }
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

private:
    TSchemeShard* SS = nullptr;
    TInner Map;
    THashMap<TPathId, TPathRef> Refs;
};

} // NKikimr::NSchemeShard
