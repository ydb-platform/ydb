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
// void TPackResult::Append(TPackResult other, const NPackedTuple::TTupleLayout* layout) {
//     layout->Concat(std::vector<ui8, TMKQLAllocator<ui8>> &dst, std::vector<ui8, TMKQLAllocator<ui8>> &dstOverflow, ui32 dstCount, const ui8 *src, const ui8 *srcOverflow, ui32 srcCount, ui32 srcOverflowSize)
//     layout->TupleDeepCopy(tuple.PackedData, tuple.OverflowBegin, PackedTuples, Overflow);
//     NTuples++;
// }

} // namespace NKikimr::NMiniKQL
