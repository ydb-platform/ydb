#include "range.h"

namespace NSQLTranslationV1 {

bool TTokenIndexRange::IsEmpty() const {
    // For an empty range, End is the preceding index modulo uint32.
    // This also covers Start == 0, where End is UINT32_MAX.
    return End + 1 == Start;
}

} // namespace NSQLTranslationV1
