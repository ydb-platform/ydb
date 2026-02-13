#include "layout_converter_common.h"
#include "tuple.h"

namespace NKikimr::NMiniKQL {

i64 TPackResult::AllocatedBytes() const {
    return PackedTuples.size() + Overflow.size();
}

void TPackResult::AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout) {
    layout->TupleDeepCopy(tuple.PackedData, tuple.OverflowBegin, PackedTuples, Overflow);
    NTuples++;
}

} // namespace NKikimr::NMiniKQL
