#include "parquet_import_limits.h"

#include <contrib/libs/apache/arrow/cpp/src/parquet/metadata.h>

namespace NKikimr::NDataShard {

ui64 EstimateParquetFooterMemory(const parquet::FileMetaData& metadata, ui64 footerBytes) {
    const ui64 rowGroups = static_cast<ui64>(Max(metadata.num_row_groups(), 0));
    const ui64 columns = static_cast<ui64>(Max(metadata.num_columns(), 0));
    return footerBytes
        + rowGroups * ParquetFooterBytesPerRowGroup
        + rowGroups * columns * ParquetFooterBytesPerColumnChunk
        + columns * ParquetFooterBytesPerColumn;
}

} // namespace NKikimr::NDataShard
