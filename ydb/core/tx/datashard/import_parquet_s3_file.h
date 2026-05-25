#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

#include <functional>

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

    void PutRange(ui64 offset, TString data);

    bool HasBytes(ui64 offset, ui64 length) const;

    bool IsFullyBuffered() const;

    std::shared_ptr<arrow::io::RandomAccessFile> MakeRandomAccessFile(
        const std::shared_ptr<TParquetSparseFile>& owner) const;

    static TParquetFetchRange FooterTailRange(ui64 contentLength);

    bool TryParseFooterMetadataRange(TString& error, TMaybe<TParquetFetchRange>& metadataRange) const;

    bool PlanColumnChunkRanges(
        const std::shared_ptr<TParquetSparseFile>& owner,
        TString& error,
        TVector<TParquetFetchRange>& outRanges) const;

    void AppendChecksum(const std::function<void(TStringBuf)>& addChunk) const;

    bool ReadBytes(ui64 offset, ui64 length, TString& out) const;

    ui64 FileSize = 0;
    ui64 BufferedBytes_ = 0;
    TVector<std::pair<ui64, TString>> Segments;
};

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
