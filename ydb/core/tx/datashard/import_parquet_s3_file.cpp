#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_parquet_s3_file.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/int_util_internal.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/ubsan.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/exception.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_writer.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/metadata.h>

#include <cstring>

#include <algorithm>

#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {

namespace {

using ::arrow::io::ReadRange;

static constexpr int64_t kDefaultFooterReadSize = 64 * 1024;
static constexpr uint32_t kFooterSize = 8;

static int64_t GetFooterReadSize(ui64 contentLength) {
    if (contentLength < kFooterSize) {
        return -1;
    }

    return std::min<int64_t>(static_cast<int64_t>(contentLength), kDefaultFooterReadSize);
}

static std::expected<uint32_t, TString> ParseFooterLength(
    const uint8_t* data,
    int64_t footerReadSize,
    ui64 sourceSize)
{
    if (footerReadSize < static_cast<int64_t>(kFooterSize)) {
        return std::unexpected(TString("parquet footer is too small"));
    }

    if (memcmp(data + footerReadSize - 4, parquet::kParquetMagic, 4) != 0 &&
        memcmp(data + footerReadSize - 4, parquet::kParquetEMagic, 4) != 0) {
        return std::unexpected(TString("parquet magic bytes not found in footer"));
    }

    const uint32_t metadataLen = ::arrow::util::SafeLoadAs<uint32_t>(
        reinterpret_cast<const uint8_t*>(data + footerReadSize - kFooterSize));
    if (metadataLen > sourceSize - kFooterSize) {
        return std::unexpected(TStringBuilder() << "parquet metadata length " << metadataLen
            << " exceeds file size " << sourceSize);
    }

    return metadataLen;
}

static ReadRange ComputeColumnChunkRange(
    const parquet::FileMetaData* fileMetadata,
    int64_t sourceSize,
    int rowGroupIndex,
    int columnIndex)
{
    auto rowGroupMetadata = fileMetadata->RowGroup(rowGroupIndex);
    auto columnMetadata = rowGroupMetadata->ColumnChunk(columnIndex);

    int64_t colStart = columnMetadata->data_page_offset();
    if (columnMetadata->has_dictionary_page() &&
        columnMetadata->dictionary_page_offset() > 0 &&
        colStart > columnMetadata->dictionary_page_offset()) {
        colStart = columnMetadata->dictionary_page_offset();
    }

    const int64_t colLength = columnMetadata->total_compressed_size();
    int64_t colEnd = 0;
    if (::arrow::internal::AddWithOverflow(colStart, colLength, &colEnd) || colEnd > sourceSize) {
        throw parquet::ParquetException("invalid parquet column metadata");
    }

    static constexpr int64_t kMaxDictHeaderSize = 100;
    const parquet::ApplicationVersion& version = fileMetadata->writer_version();
    if (version.VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION())) {
        const int64_t bytesRemaining = sourceSize - colEnd;
        const int64_t padding = std::min<int64_t>(kMaxDictHeaderSize, bytesRemaining);
        return {colStart, colLength + padding};
    }

    return {colStart, colLength};
}

static TVector<ReadRange> CoalesceReadRanges(TVector<ReadRange> ranges) {
    ranges.erase(
        std::remove_if(ranges.begin(), ranges.end(), [](const ReadRange& range) { return range.length <= 0; }),
        ranges.end());
    if (ranges.empty()) {
        return ranges;
    }

    std::sort(ranges.begin(), ranges.end(), [](const ReadRange& a, const ReadRange& b) {
        return a.offset < b.offset;
    });

    TVector<ReadRange> coalesced;
    int64_t start = ranges[0].offset;
    int64_t end = ranges[0].offset + ranges[0].length;

    for (size_t i = 1; i < ranges.size(); ++i) {
        const int64_t rangeStart = ranges[i].offset;
        const int64_t rangeEnd = ranges[i].offset + ranges[i].length;
        if (rangeStart <= end) {
            end = std::max(end, rangeEnd);
        } else {
            coalesced.push_back({start, end - start});
            start = rangeStart;
            end = rangeEnd;
        }
    }

    coalesced.push_back({start, end - start});
    return coalesced;
}

class TParquetSparseRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    TParquetSparseRandomAccessFile(std::shared_ptr<TParquetSparseFile> file, arrow::MemoryPool* pool)
        : File(std::move(file))
        , Pool(pool)
    {
    }

    arrow::Result<int64_t> GetSize() override {
        return static_cast<int64_t>(File->GetFileSize());
    }

    arrow::Result<int64_t> Tell() const override {
        return Position;
    }

    arrow::Status Seek(int64_t position) override {
        Position = position;
        return arrow::Status::OK();
    }

    arrow::Status Close() override {
        return arrow::Status::OK();
    }

    bool closed() const override {
        return false;
    }

    arrow::Result<int64_t> Read(int64_t, void*) override {
        return arrow::Status::NotImplemented("Read");
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t) override {
        return arrow::Status::NotImplemented("Read");
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("invalid ReadAt arguments");
        }

        if (!File->HasBytes(static_cast<ui64>(position), static_cast<ui64>(nbytes))) {
            return arrow::Status::Invalid("parquet byte range is not loaded");
        }

        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(nbytes, Pool));
        if (!File->CopyBytes(static_cast<ui64>(position), static_cast<ui64>(nbytes),
                reinterpret_cast<char*>(buffer->mutable_data())))
        {
            return arrow::Status::Invalid("parquet byte range is not loaded");
        }
        return std::shared_ptr<arrow::Buffer>(std::move(buffer));
    }

private:
    std::shared_ptr<TParquetSparseFile> File;
    arrow::MemoryPool* const Pool;
    int64_t Position = 0;
};

} // anonymous namespace

TParquetSparseFile::TParquetSparseFile(ui64 fileSize)
    : FileSize(fileSize)
{
}

TVector<TParquetSparseFile::TSegment>::const_iterator TParquetSparseFile::FindSegment(ui64 offset) const {
    // Segments are disjoint and sorted, so their ends are sorted as well.
    return std::upper_bound(Segments.begin(), Segments.end(), offset,
        [](ui64 offset, const TSegment& segment) { return offset < segment.End(); });
}

std::expected<void, TString> TParquetSparseFile::PutRange(ui64 offset, TString data) {
    if (data.empty()) {
        return {};
    }
    if (offset > FileSize || data.size() > FileSize - offset) {
        return std::unexpected(TStringBuilder() << "parquet range [" << offset << ", "
            << offset + data.size() << ") is past the end of a " << FileSize << " byte file");
    }

    ui64 begin = offset;
    const ui64 end = offset + data.size();

    // Skip the prefix that is already loaded.
    auto it = Segments.begin() + (FindSegment(begin) - Segments.begin());
    if (it != Segments.end() && it->Offset <= begin) {
        const ui64 covered = Min(it->End(), end) - begin;
        if (covered == data.size()) {
            return {};
        }
        data.erase(0, covered);
        begin = it->End();
        ++it;
    }

    // Now no segment contains begin; the next one must start at or after end.
    if (it != Segments.end() && it->Offset < end) {
        return std::unexpected(TStringBuilder() << "parquet range [" << begin << ", " << end
            << ") overlaps loaded range [" << it->Offset << ", " << it->End() << ")");
    }

    BufferedBytes_ += data.size();

    const bool mergePrev = it != Segments.begin() && std::prev(it)->End() == begin;
    const bool mergeNext = it != Segments.end() && it->Offset == end;
    if (mergePrev && mergeNext) {
        auto prev = std::prev(it);
        prev->Data.append(data);
        prev->Data.append(it->Data);
        Segments.erase(it);
    } else if (mergePrev) {
        std::prev(it)->Data.append(data);
    } else if (mergeNext) {
        data.append(it->Data);
        it->Data = std::move(data);
        it->Offset = begin;
    } else {
        Segments.insert(it, TSegment{.Offset = begin, .Data = std::move(data)});
    }

    return {};
}

bool TParquetSparseFile::HasBytes(ui64 offset, ui64 length) const {
    if (length == 0) {
        return true;
    }
    if (offset > FileSize || length > FileSize - offset) {
        return false;
    }

    const auto it = FindSegment(offset);
    return it != Segments.end() && it->Offset <= offset && it->End() >= offset + length;
}

TMaybe<TString> TParquetSparseFile::ReadBytes(ui64 offset, ui64 length) const {
    if (!HasBytes(offset, length)) {
        return Nothing();
    }
    if (length == 0) {
        return TString();
    }

    const auto it = FindSegment(offset);
    return TString(it->Data.data() + (offset - it->Offset), length);
}

bool TParquetSparseFile::IsFullyBuffered() const {
    return HasBytes(0, FileSize);
}

bool TParquetSparseFile::CopyBytes(ui64 offset, ui64 length, char* out) const {
    if (!HasBytes(offset, length)) {
        return false;
    }
    if (length == 0) {
        return true;
    }

    const auto it = FindSegment(offset);
    memcpy(out, it->Data.data() + (offset - it->Offset), length);
    return true;
}

std::shared_ptr<arrow::io::RandomAccessFile> TParquetSparseFile::MakeRandomAccessFile(
    const std::shared_ptr<TParquetSparseFile>& owner,
    arrow::MemoryPool* pool) const
{
    return std::make_shared<TParquetSparseRandomAccessFile>(owner, pool);
}

TParquetFetchRange TParquetSparseFile::FooterTailRange(ui64 contentLength) {
    TParquetFetchRange range;
    const int64_t footerReadSize = GetFooterReadSize(contentLength);
    Y_ENSURE(footerReadSize > 0, "parquet file is too small");

    range.Offset = contentLength - static_cast<ui64>(footerReadSize);
    range.Length = static_cast<ui64>(footerReadSize);
    return range;
}

std::expected<TMaybe<TParquetFetchRange>, TString> TParquetSparseFile::TryParseFooterMetadataRange() const {
    const int64_t footerReadSize = GetFooterReadSize(FileSize);
    if (footerReadSize < 0) {
        return std::unexpected(TString("parquet file is too small"));
    }

    const ui64 footerOffset = FileSize - static_cast<ui64>(footerReadSize);
    auto footerData = ReadBytes(footerOffset, static_cast<ui64>(footerReadSize));
    if (!footerData) {
        return std::unexpected(TString("parquet footer tail is not loaded"));
    }

    auto metadataLen = ParseFooterLength(
        reinterpret_cast<const uint8_t*>(footerData->data()),
        footerReadSize,
        FileSize);
    if (!metadataLen) {
        return std::unexpected(std::move(metadataLen.error()));
    }

    if (static_cast<ui64>(footerReadSize) >= static_cast<ui64>(*metadataLen) + kFooterSize) {
        return TMaybe<TParquetFetchRange>{};
    }

    const ui64 metadataOffset = FileSize - kFooterSize - *metadataLen;
    if (HasBytes(metadataOffset, *metadataLen)) {
        return TMaybe<TParquetFetchRange>{};
    }

    return TMaybe<TParquetFetchRange>(TParquetFetchRange{
        .Offset = metadataOffset,
        .Length = footerOffset - metadataOffset,
    });
}

std::expected<ui64, TString> TParquetSparseFile::FooterMetadataLength() const {
    const int64_t footerReadSize = GetFooterReadSize(FileSize);
    if (footerReadSize < 0) {
        return std::unexpected(TString("parquet file is too small"));
    }

    const ui64 footerOffset = FileSize - static_cast<ui64>(footerReadSize);
    auto footerData = ReadBytes(footerOffset, static_cast<ui64>(footerReadSize));
    if (!footerData) {
        return std::unexpected(TString("parquet footer tail is not loaded"));
    }

    auto metadataLen = ParseFooterLength(
        reinterpret_cast<const uint8_t*>(footerData->data()),
        footerReadSize,
        FileSize);
    if (!metadataLen) {
        return std::unexpected(std::move(metadataLen.error()));
    }
    return static_cast<ui64>(*metadataLen);
}

std::expected<TVector<TVector<TParquetFetchRange>>, TString>
TParquetSparseFile::PlanColumnChunkRangesByRowGroup(
    const parquet::FileMetaData& metadata,
    const std::vector<int>& columns) const
{
    try {
        for (const int col : columns) {
            if (col < 0 || col >= metadata.num_columns()) {
                return std::unexpected(TStringBuilder() << "parquet column " << col
                    << " is outside a file with " << metadata.num_columns() << " columns");
            }
        }

        TVector<TVector<TParquetFetchRange>> outRanges;
        outRanges.resize(metadata.num_row_groups());

        for (int32_t row = 0; row < metadata.num_row_groups(); ++row) {
            TVector<ReadRange> ranges;
            ranges.reserve(columns.size());
            for (const int col : columns) {
                ranges.push_back(ComputeColumnChunkRange(
                    &metadata,
                    static_cast<int64_t>(FileSize),
                    row,
                    col));
            }

            auto coalesced = CoalesceReadRanges(std::move(ranges));
            auto& fetchRanges = outRanges[row];
            fetchRanges.reserve(coalesced.size());
            for (const auto& range : coalesced) {
                fetchRanges.push_back({
                    .Offset = static_cast<ui64>(range.offset),
                    .Length = static_cast<ui64>(range.length),
                });
            }
        }

        return outRanges;
    } catch (const parquet::ParquetException& ex) {
        return std::unexpected(TString(ex.what()));
    } catch (const std::exception& ex) {
        return std::unexpected(TString(ex.what()));
    }
}

void TParquetSparseFile::Clear() {
    Segments.clear();
    BufferedBytes_ = 0;
}

void TParquetSparseFile::ClearBefore(ui64 offset) {
    const auto first = Segments.begin() + (FindSegment(offset) - Segments.begin());
    Segments.erase(Segments.begin(), first);

    if (!Segments.empty() && Segments.front().Offset < offset) {
        auto& segment = Segments.front();
        segment.Data.erase(0, offset - segment.Offset);
        segment.Offset = offset;
    }

    BufferedBytes_ = 0;
    for (const auto& segment : Segments) {
        BufferedBytes_ += segment.Data.size();
    }
}

ui64 EstimateParquetFooterMemory(const parquet::FileMetaData& metadata, ui64 footerBytes) {
    const ui64 rowGroups = static_cast<ui64>(Max(metadata.num_row_groups(), 0));
    const ui64 columns = static_cast<ui64>(Max(metadata.num_columns(), 0));
    return footerBytes
        + rowGroups * ParquetFooterBytesPerRowGroup
        + rowGroups * columns * ParquetFooterBytesPerColumnChunk
        + columns * ParquetFooterBytesPerColumn;
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
