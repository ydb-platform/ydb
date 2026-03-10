#include "yql_yt_yson_block_iterator.h"

namespace NYql::NFmr {

TStringBuf TIndexedBlock::GetRowBytes(ui64 rowIndex) {
    auto markup = Rows[rowIndex];
    auto boundary = markup.back();

    // If row with a separator
    if (boundary.EndOffset < Data.size() && Data[boundary.EndOffset] == ';') {
        ++boundary.EndOffset;
    }
    return SliceRange(Data, boundary);
}

} // namespace NYql::NFmr
