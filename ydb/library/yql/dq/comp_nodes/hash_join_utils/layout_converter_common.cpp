#include "layout_converter_common.h"
namespace NKikimr::NMiniKQL {


int64_t TPackResult::AllocatedBytes() const {
    return PackedTuples.capacity() + Overflow.capacity();
}

}
