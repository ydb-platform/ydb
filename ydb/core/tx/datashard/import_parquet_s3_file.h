#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/memory_pool.h>

#include <expected>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace parquet {
class FileMetaData;
}

namespace NKikimr::NDataShard {

struct TParquetFetchRange {
    ui64 Offset = 0;
    ui64 Length = 0;
    ui64 Fetched = 0;
};

class TParquetSparseFile {
public:
    explicit TParquetSparseFile(ui64 fileSize);

    ui64 GetFileSize() const {
        return FileSize;
    }

    ui64 BufferedBytes() const {
        return BufferedBytes_;
    }

    // Stores a byte range. Loaded bytes at its start are skipped (a retried chunk repeats
    // them); any other overlap is an error.
    std::expected<void, TString> PutRange(ui64 offset, TString data);

    bool HasBytes(ui64 offset, ui64 length) const;

    bool IsFullyBuffered() const;

    // The bytes of the range, which must be loaded, copied to out.
    bool CopyBytes(ui64 offset, ui64 length, char* out) const;

    // A file over the loaded bytes for Arrow; a read is copied once into the pool, where
    // it is counted.
    std::shared_ptr<arrow::io::RandomAccessFile> MakeRandomAccessFile(
        const std::shared_ptr<TParquetSparseFile>& owner,
        arrow::MemoryPool* pool = arrow::default_memory_pool()) const;

    static TParquetFetchRange FooterTailRange(ui64 contentLength);

    std::expected<TMaybe<TParquetFetchRange>, TString> TryParseFooterMetadataRange() const;

    // The footer length from the last bytes of the file, which must be loaded.
    std::expected<ui64, TString> FooterMetadataLength() const;

    // The ranges of the given columns per row group; the other columns are not downloaded.
    std::expected<TVector<TVector<TParquetFetchRange>>, TString> PlanColumnChunkRangesByRowGroup(
        const parquet::FileMetaData& metadata,
        const std::vector<int>& columns) const;

    void Clear();

    // Drops the loaded bytes before offset, keeping a segment tail that crosses it: a
    // finished row group is evicted, the footer kept.
    void ClearBefore(ui64 offset);

    TMaybe<TString> ReadBytes(ui64 offset, ui64 length) const;

private:
    // Loaded bytes: sorted, disjoint, never adjacent (touching puts merge), so a loaded
    // range lies within one segment.
    struct TSegment {
        ui64 Offset = 0;
        TString Data;

        ui64 End() const {
            return Offset + Data.size();
        }
    };

    // First segment ending after offset (i.e. the one containing offset, if any).
    TVector<TSegment>::const_iterator FindSegment(ui64 offset) const;

    ui64 FileSize = 0;
    ui64 BufferedBytes_ = 0;
    TVector<TSegment> Segments;
};

// What a parsed footer takes: thrift structures of a few hundred bytes per column chunk
// and row group (sizeof 560 and 96), while a chunk is as little as 3 bytes in the footer;
// and per column, next to its 320-byte thrift element, the schema node, column descriptor,
// manifest field and Arrow field that Arrow builds for it.
constexpr ui64 ParquetFooterBytesPerColumnChunk = 640;
constexpr ui64 ParquetFooterBytesPerRowGroup = 128;
constexpr ui64 ParquetFooterBytesPerColumn = 2048;

ui64 EstimateParquetFooterMemory(const parquet::FileMetaData& metadata, ui64 footerBytes);

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
