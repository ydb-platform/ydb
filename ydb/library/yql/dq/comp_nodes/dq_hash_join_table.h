#pragma once
#include "type_utils.h"
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/neumann_hash_table.h>
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

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

    void BuildWith(IBlockLayoutConverter::TPackResult data, TMKQLVector<TArrowRowRef> rowRefs) {
        BuildData_ = std::move(data);
        BuildRowRefs_ = std::move(rowRefs);
        MKQL_ENSURE(BuildData_.NTuples >= 0 && BuildData_.NTuples <= std::numeric_limits<int>::max(),
                    TStringBuilder() << "NTuples (" << BuildData_.NTuples << ") exceeds int range");
        MKQL_ENSURE(
            BuildData_.PackedTuples.size() >= static_cast<size_t>(BuildData_.NTuples) * RowWidth_,
            TStringBuilder() << "NTuples (" << BuildData_.NTuples << ") exceeds PackedTuples capacity ("
                             << BuildData_.PackedTuples.size() << " bytes, row width " << RowWidth_ << ")");
        MKQL_ENSURE(
            std::ssize(BuildRowRefs_) == BuildData_.NTuples,
            TStringBuilder() << "row refs (" << BuildRowRefs_.size()
                             << ") must be parallel to packed rows (" << BuildData_.NTuples << ")");
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

    // Lookup invokes consume(matched_packed_tuple, matched_arrow_row_ref) for each
    // build row matching the given probe row. The arrow ref points into the
    // original Arrow chunk that produced this build row (or kNullChunk for
    // synthetic spill-loaded chunks; see THybridHashJoin spill path).
    void Lookup(TSingleTuple row, std::invocable<TSingleTuple, TArrowRowRef> auto consume) {
        if (Empty()){
            return;
        }
        Table_.Apply(row.PackedData, row.OverflowBegin, [consume, this](const ui8* tuplePackedData) {
            // ForceOutplace=true in Table_ guarantees tuplePackedData points into
            // BuildData_.PackedTuples, so this index is well-defined regardless of row size.
            size_t index = (tuplePackedData - BuildData_.PackedTuples.data()) / RowWidth_;
            if (TrackUsed_) {
                MKQL_ENSURE(index < Used_.size(), "used-tracking index out of bounds");
                Used_[index] = 1;
            }
            consume(TSingleTuple{tuplePackedData, BuildData_.Overflow.data()},
                    BuildRowRefs_[index]);
        });
    }

    void ForEachUnused(std::invocable<TSingleTuple, TArrowRowRef> auto consume) const {
        MKQL_ENSURE(TrackUsed_, "ForEachUnused called but not tracking used tuples");
        for (size_t i = 0; i < static_cast<size_t>(BuildData_.NTuples); ++i) {
            if (!Used_[i]) {
                consume(TSingleTuple{
                            BuildData_.PackedTuples.data() + i * RowWidth_,
                            BuildData_.Overflow.data()
                        },
                        BuildRowRefs_[i]);
            }
        }
    }

  private:
    IBlockLayoutConverter::TPackResult BuildData_;
    // Parallel to BuildData_ rows: ref to the Arrow chunk row that produced each
    // packed build row. Used by the indexed-output path to drive arrow::Take.
    TMKQLVector<TArrowRowRef> BuildRowRefs_;
    NKikimr::NMiniKQL::NPackedTuple::TNeumannHashTable<false, false, /*ForceOutplace=*/true> Table_;
    size_t RowWidth_ = 0;
    bool TrackUsed_ = false;
    TMKQLVector<ui8> Used_;
};

} // namespace NKikimr::NMiniKQL::NJoinTable