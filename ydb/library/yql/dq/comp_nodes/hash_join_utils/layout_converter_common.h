#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

struct TSingleTuple {
    const ui8* PackedData;
    const ui8* OverflowBegin;
};


// Common types used by both IBlockLayoutConverter and IScalarLayoutConverter
struct TPackResult {
    std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
    std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
    int64_t NTuples{0};
    int64_t AllocatedBytes() const;
    TPackResult() = default;
    ~TPackResult() = default;
    TPackResult(TPackResult&& other)
        : PackedTuples(std::move(other.PackedTuples))
        , Overflow(std::move(other.Overflow))
        , NTuples(other.NTuples) {
        other.NTuples = 0;
    }
    TPackResult& operator=(TPackResult&& other){
        PackedTuples = std::move(other.PackedTuples);
        Overflow = std::move(other.Overflow);
        NTuples = other.NTuples; 
        other.NTuples = 0;
        return *this;
    }
    TPackResult(const TPackResult& other) = delete;
    TPackResult& operator=(const TPackResult& other) = delete;
    
    bool Empty() const {
        bool allFieldEmpty = NTuples == 0 && Overflow.empty() && PackedTuples.empty();
        bool haveOneFieldEmpty = NTuples == 0 || Overflow.empty() || PackedTuples.empty();
        MKQL_ENSURE(allFieldEmpty == haveOneFieldEmpty, "inconsistent state");
        return allFieldEmpty;
    }
    void ForEachTuple(std::invocable<TSingleTuple> auto fn) {
        int tupleSize = std::ssize(PackedTuples) / NTuples;
        for (int index = 0; index < NTuples; ++index) {
            fn(TSingleTuple{.PackedData = &PackedTuples[index*tupleSize], .OverflowBegin = Overflow.data()});
        }
    }
    void AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout);
};

TPackResult Flatten(TMKQLVector<TPackResult> tuples, const NPackedTuple::TTupleLayout* layout);


using TPackedTuple = std::vector<ui8, TMKQLAllocator<ui8>>;
using TOverflow = std::vector<ui8, TMKQLAllocator<ui8>>;

} // namespace NKikimr::NMiniKQL
