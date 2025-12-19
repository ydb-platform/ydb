#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>

#include <util/string/printf.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

// Common types used by both IBlockLayoutConverter and IScalarLayoutConverter
struct TPackResult {
    std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
    std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
    i64 NTuples{ 0 };
    i64 AllocatedBytes() const;
    TPackResult() = default;
    ~TPackResult() = default;

    TPackResult(TPackResult&& other)
        : PackedTuples(std::move(other.PackedTuples))
        , Overflow(std::move(other.Overflow))
        , NTuples(other.NTuples)
    {
        other.NTuples = 0;
    }

    TPackResult& operator=(TPackResult&& other) {
        PackedTuples = std::move(other.PackedTuples);
        Overflow = std::move(other.Overflow);
        NTuples = other.NTuples;
        other.NTuples = 0;
        return *this;
    }

    TPackResult(const TPackResult& other) = delete;
    TPackResult& operator=(const TPackResult& other) = delete;

    bool Empty() const {
        bool allFieldEmpty = NTuples == 0 && PackedTuples.empty();
        bool haveOneFieldEmpty = NTuples == 0 || PackedTuples.empty();
        MKQL_ENSURE(allFieldEmpty == haveOneFieldEmpty, "inconsistent state");
        if (allFieldEmpty) {
            MKQL_ENSURE(Overflow.empty(), "sanity check");
        }
        return allFieldEmpty;
    }

    void ForEachTuple(std::invocable<TSingleTuple> auto fn) const {   // todo: add range-based for support
        int tupleSize = std::ssize(PackedTuples) / NTuples;
        for (int index = 0; index < NTuples; ++index) {
            // Cout << Sprintf("index: %i, ", index);
            // Cout.Flush();
            fn(TSingleTuple{ .PackedData = &PackedTuples[index * tupleSize], .OverflowBegin = Overflow.data() });
        }
    }

    void Clear() {
        *this = TPackResult{};
        MKQL_ENSURE(Empty(), "sanity check");
    }

    void AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout);
    // void Append(TPackResult other, const NPackedTuple::TTupleLayout* layout);
};

using TPackedTuple = std::vector<ui8, TMKQLAllocator<ui8>>;
using TOverflow = std::vector<ui8, TMKQLAllocator<ui8>>;

}   // namespace NKikimr::NMiniKQL
