#pragma once

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <optional>

#include <util/generic/noncopyable.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include "tuple.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

/**
 * Join hash table backed by absl::flat_hash_map (SwissTable). Build rows are
 * grouped by embedded row hash (first ui32, same as TNeumannHashTable / Hash()).
 * Hash collisions are resolved with TTupleLayout::KeysEqual in Apply().
 */
class TSwissJoinHashTable : public NNonCopyable::TMoveOnly {
  public:
    TSwissJoinHashTable() = default;

    explicit TSwissJoinHashTable(const TTupleLayout* layout)
        : TSwissJoinHashTable() {
        SetTupleLayout(layout);
    }

    void SetTupleLayout(const TTupleLayout* layout) {
        Clear();
        Layout_ = layout;
    }

    TSwissJoinHashTable(TSwissJoinHashTable&&) = default;
    TSwissJoinHashTable& operator=(TSwissJoinHashTable&&) = default;

    ui64 RequiredMemoryForBuild(int nItems) const {
        // Rough upper bound: SwissTable buckets + one pointer per row in value vectors.
        constexpr ui64 kPerMapEntry = 48;
        return static_cast<ui64>(nItems) * (kPerMapEntry + sizeof(const ui8*));
    }

    void Build(const ui8* const tuples, const ui8* const overflow, int nItems,
               std::optional<ui32> /*estimatedLogSize*/ = std::nullopt) {
        MKQL_ENSURE_S(Layout_ != nullptr);
        MKQL_ENSURE_S(Tuples_ == nullptr && Overflow_ == nullptr && Map_.empty());

        Tuples_ = tuples;
        Overflow_ = overflow;

        MKQL_ENSURE(nItems >= 0, "nItems out of range");
        const size_t rowSize = Layout_->TotalRowSize;
        Map_.reserve(static_cast<size_t>(std::max(1, nItems)));

        for (int ind = 0; ind != nItems; ++ind) {
            const ui8* row = tuples + rowSize * static_cast<size_t>(ind);
            const ui32 h = Hash(row);
            Map_[h].push_back(row);
        }
    }

    bool Empty() const {
        return Map_.empty();
    }

    void Apply(const ui8* const row, const ui8* const overflow,
               std::invocable<const ui8*> auto onMatch) const {
        MKQL_ENSURE(Layout_ != nullptr, "sanity check");
        MKQL_ENSURE(!Map_.empty() && Tuples_ != nullptr, "lookup to empty table?");

        const ui32 h = Hash(row);
        auto it = Map_.find(h);
        if (it == Map_.end()) {
            return;
        }
        for (const ui8* buildRow : it->second) {
            if (Layout_->KeysEqual(row, overflow, buildRow, Overflow_)) {
                onMatch(buildRow);
            }
        }
    }

    void Clear() {
        Map_.clear();
        Tuples_ = nullptr;
        Overflow_ = nullptr;
    }

  private:
    const TTupleLayout* Layout_ = nullptr;
    const ui8* Tuples_ = nullptr;
    const ui8* Overflow_ = nullptr;
    absl::flat_hash_map<ui32, TMKQLVector<const ui8*>> Map_;
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
