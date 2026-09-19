#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

#include <expected>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

    // Stores one byte range. Bytes already loaded at the start of the range
    // are skipped (a retried or re-routed chunk may repeat them); overlapping
    // loaded bytes anywhere else is an error.
    std::expected<void, TString> PutRange(ui64 offset, TString data);

    bool HasBytes(ui64 offset, ui64 length) const;

    bool IsFullyBuffered() const;

    std::shared_ptr<arrow::io::RandomAccessFile> MakeRandomAccessFile(
        const std::shared_ptr<TParquetSparseFile>& owner) const;

    static TParquetFetchRange FooterTailRange(ui64 contentLength);

    std::expected<TMaybe<TParquetFetchRange>, TString> TryParseFooterMetadataRange() const;

    std::expected<TVector<TParquetFetchRange>, TString> PlanColumnChunkRanges(
        const std::shared_ptr<TParquetSparseFile>& owner) const;

    std::expected<TVector<TVector<TParquetFetchRange>>, TString> PlanColumnChunkRangesByRowGroup(
        const std::shared_ptr<TParquetSparseFile>& owner) const;

    void Clear();

    // Drops every buffered byte before offset while preserving a segment tail
    // that crosses the boundary. This lets the import engine evict a completed
    // row group without discarding the cached Parquet footer suffix.
    void ClearBefore(ui64 offset);

    TMaybe<TString> ReadBytes(ui64 offset, ui64 length) const;

private:
    // Loaded bytes, sorted by offset, pairwise disjoint and never adjacent:
    // touching puts are merged, so a fully loaded range always lies within
    // exactly one segment and lookups are a binary search.
    struct TSegment {
        ui64 Offset = 0;
        TString Data;

        ui64 End() const {
            return Offset + Data.size();
        }
    };

    // First segment ending after offset (i.e. the one containing offset, if any).
    TVector<TSegment>::const_iterator FindSegment(ui64 offset) const;

    TVector<TParquetFetchRange> SubtractLoaded(const TVector<arrow::io::ReadRange>& ranges) const;

    ui64 FileSize = 0;
    ui64 BufferedBytes_ = 0;
    TVector<TSegment> Segments;
};

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
