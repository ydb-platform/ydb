#include "layout_converter_common.h"
namespace NKikimr::NMiniKQL {


i64 TPackResult::AllocatedBytes() const {
    return PackedTuples.capacity() + Overflow.capacity();
}

}
