#include "layout_converter_common.h"
namespace NKikimr::NMiniKQL {


int64_t TPackResult::AllocatedBytes() const {
    return PackedTuples.capacity() + Overflow.capacity();
}

void TPackResult::AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout) {
    layout->TupleDeepCopy(tuple.PackedData, tuple.OverflowBegin, PackedTuples, Overflow);
    NTuples++;  
}

TPackResult Flatten(TMKQLVector<TPackResult> tuples, const NPackedTuple::TTupleLayout* layout) {
    TPackResult flattened;
    flattened.NTuples = std::accumulate(tuples.begin(), tuples.end(), i64{0},
                                        [](i64 summ, const auto& packRes) { return summ += packRes.NTuples; });

    i64 totalTuplesSize = std::accumulate(tuples.begin(), tuples.end(), i64{0}, [](i64 summ, const auto& packRes) {
        return summ += std::ssize(packRes.PackedTuples);
    });
    flattened.PackedTuples.reserve(totalTuplesSize);

    i64 totaOverflowlSize =
        std::accumulate(tuples.begin(), tuples.end(), i64{0},
                        [](i64 summ, const auto& packRes) { return summ += std::ssize(packRes.Overflow); });
    flattened.Overflow.reserve(totaOverflowlSize);
    // todo: assertNoAllocations until the end of a function

    int tupleSize = layout->TotalRowSize;
    for (const TPackResult& tupleBatch : tuples) {
        layout->Concat(flattened.PackedTuples, flattened.Overflow,
                                std::ssize(flattened.PackedTuples) / tupleSize, tupleBatch.PackedTuples.data(),
                                tupleBatch.Overflow.data(), tupleBatch.PackedTuples.size() / tupleSize,
                                tupleBatch.Overflow.size());
    }
    return flattened;
}


}
