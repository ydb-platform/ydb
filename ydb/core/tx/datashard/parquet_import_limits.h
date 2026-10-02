#pragma once

#include <util/generic/utility.h>
#include <util/system/types.h>

namespace parquet {
class FileMetaData;
}

namespace NKikimr::NDataShard {

// The limits the Parquet importer puts on a file, all derived from the limit
// of its read buffer, RestoreReadBufferSizeLimit. The exporter writes its
// files within them, so that a backup is imported with the default settings.

// What the importer reads first, from the end of the file.
constexpr ui64 ParquetFooterTailBytes = 64 * 1024;

// A footer longer than this is refused before it is downloaded: an eighth of
// the buffer, and never less than the tail that is read anyway.
constexpr ui64 ParquetFooterSizeLimit(ui64 bufferSizeLimit) {
    return Max(bufferSizeLimit / 8, ParquetFooterTailBytes);
}

// What a parsed footer takes in memory, by estimate. Arrow keeps it as thrift
// structures: one per column chunk, one per row group and one per column, a
// few hundred bytes each (sizeof is 560, 96 and 320 in this Arrow, before the
// vectors they hold), while a column chunk takes as little as 3 bytes in the
// footer. The strings they hold are copies of what is in the footer, so its
// own size covers them.
constexpr ui64 ParquetFooterBytesPerColumnChunk = 640;
constexpr ui64 ParquetFooterBytesPerRowGroup = 128;
constexpr ui64 ParquetFooterBytesPerColumn = 384;

ui64 EstimateParquetFooterMemory(const parquet::FileMetaData& metadata, ui64 footerBytes);

// The most bytes a row group may take, in the file or uncompressed, next to a
// parsed footer of that memory: the rest of the buffer.
constexpr ui64 ParquetRowGroupSizeLimit(ui64 bufferSizeLimit, ui64 footerMemory) {
    return bufferSizeLimit > footerMemory ? bufferSizeLimit - footerMemory : 0;
}

} // namespace NKikimr::NDataShard
