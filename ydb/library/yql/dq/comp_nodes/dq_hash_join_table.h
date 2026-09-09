#pragma once
#include "type_utils.h"
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/neumann_hash_table.h>
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

#include <type_traits>

namespace NKikimr::NMiniKQL::NJoinTable {

using TTuple = const NYql::NUdf::TUnboxedValue*;
using TSizedTuple = std::span<const NYql::NUdf::TUnboxedValue>;

bool NeedToTrackUnusedRightTuples(EJoinKind kind);

bool NeedToTrackUnusedLeftTuples(EJoinKind kind);

class TStdJoinTable {
    struct TuplesWithSameJoinKey {
        std::vector<TTuple> Tuples;
        bool Used;
    };

  public:
    TStdJoinTable(int tupleSize, NKikimr::NMiniKQL::TWideUnboxedEqual eq, NKikimr::NMiniKQL::TWideUnboxedHasher hash,
                  bool trackUnusedTuples)
        : TupleSize(tupleSize)
        , TrackUnusedTuples(trackUnusedTuples)
        , BuiltTable(1, hash, eq)
    {}

    void Add(TSizedTuple tuple) {
        MKQL_ENSURE(BuiltTable.empty(), "JoinTable is built already");
        MKQL_ENSURE(std::ssize(tuple) == TupleSize,
                    TStringBuilder() << "tuple size promise(" << TupleSize << ") vs actual(" << std::ssize(tuple) << ") mismatch");
        for (int idx = 0; idx < TupleSize; ++idx) {
            Tuples.push_back(tuple[idx]);
        }
    }

    void Build() {
        MKQL_ENSURE(BuiltTable.empty(), "JoinTable is built already");
        for (int index = 0; index < std::ssize(Tuples); index += TupleSize) {
            TTuple thisTuple = &Tuples[index];
            auto [it, ok] = BuiltTable.emplace(
                thisTuple, TuplesWithSameJoinKey{.Tuples = std::vector{thisTuple}, .Used = !TrackUnusedTuples});
            if (!ok) {
                it->second.Tuples.emplace_back(thisTuple);
            }
        }
    }

    void Lookup(TTuple key, std::invocable<TTuple> auto produce) {
        auto it = BuiltTable.find(key);
        if (it != BuiltTable.end()) {
            it->second.Used = true;
            std::ranges::for_each(it->second.Tuples, produce);
        }
    }

    bool UnusedTrackingOn() const {
        return TrackUnusedTuples;
    }

    const auto& MapView() const {
        return BuiltTable;
    }

    void ForEachUnused(std::function<void(TTuple)> produce) {
        MKQL_ENSURE(TrackUnusedTuples, "wasn't tracking tuples at all");
        for (auto& tuplesSameKey : BuiltTable) {
            if (!tuplesSameKey.second.Used) {
                std::ranges::for_each(tuplesSameKey.second.Tuples, produce);
                tuplesSameKey.second.Used = true;
            }
        }
    }

  private:
    const int TupleSize;
    const bool TrackUnusedTuples;
    std::vector<NYql::NUdf::TUnboxedValue> Tuples;
    std::unordered_map<TTuple, TuplesWithSameJoinKey, NKikimr::NMiniKQL::TWideUnboxedHasher,
                       NKikimr::NMiniKQL::TWideUnboxedEqual>
        BuiltTable;
};

class TNeumannJoinTable : public NNonCopyable::TMoveOnly {
  public:

    TNeumannJoinTable(const NPackedTuple::TTupleLayout* layout, bool trackUsed = false)
        : Table_(layout)
        , RowWidth_(layout->TotalRowSize)
        , TrackUsed_(trackUsed)
    {
        MKQL_ENSURE(Empty(), "table should be empty by default");
    }

    void BuildWith(IBlockLayoutConverter::TPackResult data) {
        BuildData_ = std::move(data);
        MKQL_ENSURE(BuildData_.NTuples >= 0 && BuildData_.NTuples <= std::numeric_limits<int>::max(),
                    TStringBuilder() << "NTuples (" << BuildData_.NTuples << ") exceeds int range");
        MKQL_ENSURE(
            BuildData_.PackedTuples.size() >= static_cast<size_t>(BuildData_.NTuples) * RowWidth_,
            TStringBuilder() << "NTuples (" << BuildData_.NTuples << ") exceeds PackedTuples capacity ("
                             << BuildData_.PackedTuples.size() << " bytes, row width " << RowWidth_ << ")");
        Table_.Build(BuildData_.PackedTuples.data(), BuildData_.Overflow.data(),
                     BuildData_.NTuples);
        if (TrackUsed_ && BuildData_.NTuples > 0) {
            Used_.resize(BuildData_.NTuples, 0);
        }
    }


    bool Empty() const {
        return Table_.Empty();
    }

    ui64 RequiredMemoryForBuild(int nTuples) const {
        return Table_.RequiredMemoryForBuild(nTuples);
    }

    void Lookup(TSingleTuple row, std::invocable<TSingleTuple> auto consume) {
        size_t resumeIndex = 0;
        Lookup(row, resumeIndex, consume, [] { return false; });
    }

    // resumeIndex is how many matches of this probe were already consumed
    bool Lookup(TSingleTuple row, size_t& resumeIndex, auto consume, std::predicate auto isFull) {
        if (Empty()) {
            resumeIndex = 0;
            return true;
        }
        bool full = false;
        size_t seen = 0;
        Table_.Apply(row.PackedData, row.OverflowBegin, [&](const ui8* packed) {
            if (seen++ < resumeIndex) {
                return true;
            }
            const TSingleTuple match{packed, BuildData_.Overflow.data()};
            bool keep = true;
            if constexpr (std::is_void_v<decltype(consume(match))>) {
                consume(match);
            } else {
                keep = bool(consume(match));
            }
            resumeIndex = seen;
            full = isFull();
            return keep && !full;
        });
        if (full) {
            return false;
        }
        resumeIndex = 0;
        return true;
    }

    // Stops on the first accepted match. Semi/only joins only need existence, so
    // walking the rest of a duplicate chain is wasted work.
    bool LookupAny(TSingleTuple row, std::predicate<TSingleTuple> auto accept) {
        if (Empty()) {
            return false;
        }
        auto iterator = Table_.Find(row.PackedData, row.OverflowBegin);
        while (const ui8* tuplePackedData = Table_.NextMatch(iterator, row.OverflowBegin)) {
            if (accept(TSingleTuple{tuplePackedData, BuildData_.Overflow.data()})) {
                return true;
            }
        }
        return false;
    }

    bool ForEachFrom(size_t& resumeIndex, std::invocable<TSingleTuple> auto consume,
                     std::predicate auto isFull) const {
        const size_t nTuples = static_cast<size_t>(BuildData_.NTuples);
        for (; resumeIndex < nTuples; ++resumeIndex) {
            consume(TSingleTuple{
                BuildData_.PackedTuples.data() + resumeIndex * RowWidth_,
                BuildData_.Overflow.data()
            });
            if (isFull()) {
                ++resumeIndex;
                return false;
            }
        }
        return true;
    }

    // After the pair is accepted, including join filters. Returns true if this tuple was unused
    bool MarkUsed(TSingleTuple tuple) {
        if (!TrackUsed_) {
            return false;
        }
        const size_t index = Table_.IndexOfPackedRow(tuple.PackedData);
        MKQL_ENSURE(index < Used_.size(), "used-tracking index out of bounds");
        const bool first = Used_[index] == 0;
        Used_[index] = 1;
        return first;
    }

    bool ForEachUnused(size_t& resumeIndex, std::invocable<TSingleTuple> auto consume,
                       std::predicate auto isFull) const {
        MKQL_ENSURE(TrackUsed_, "ForEachUnused called but not tracking used tuples");
        const size_t nTuples = static_cast<size_t>(BuildData_.NTuples);
        for (; resumeIndex < nTuples; ++resumeIndex) {
            if (Used_[resumeIndex]) {
                continue;
            }
            consume(TSingleTuple{
                Table_.PackedRow(resumeIndex),
                BuildData_.Overflow.data()
            });
            if (isFull()) {
                ++resumeIndex;
                return false;
            }
        }
        return true;
    }

  private:
    IBlockLayoutConverter::TPackResult BuildData_;
    NKikimr::NMiniKQL::NPackedTuple::TNeumannHashTable<false, false> Table_;
    size_t RowWidth_ = 0;
    bool TrackUsed_ = false;
    TMKQLVector<ui8> Used_;
};

} // namespace NKikimr::NMiniKQL::NJoinTable
