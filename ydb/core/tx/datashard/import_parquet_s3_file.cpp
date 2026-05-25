#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_parquet_s3_file.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/int_util_internal.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/ubsan.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/exception.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_writer.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/metadata.h>

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

static TVector<TParquetFetchRange> SubtractLoadedRanges(
    const TVector<ReadRange>& ranges,
    const TParquetSparseFile& file)
{
    TVector<TParquetFetchRange> result;
    for (auto&& range : ranges) {
        const ui64 rangeStart = static_cast<ui64>(range.offset);
        const ui64 rangeEnd = rangeStart + static_cast<ui64>(range.length);
        ui64 pos = rangeStart;

        for (const auto& segment : file.Segments) {
            const ui64 segmentStart = segment.first;
            const ui64 segmentEnd = segment.first + segment.second.size();
            if (segmentEnd <= pos) {
                continue;
            }
            if (segmentStart >= rangeEnd) {
                break;
            }

            if (pos < segmentStart) {
                result.push_back({
                    .Offset = pos,
                    .Length = Min(segmentStart, rangeEnd) - pos,
                });
            }

            pos = Max(pos, segmentEnd);
            if (pos >= rangeEnd) {
                break;
            }
        }

        if (pos < rangeEnd) {
            result.push_back({
                .Offset = pos,
                .Length = rangeEnd - pos,
            });
        }
    }

    return result;
}

class TParquetSparseRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    static std::shared_ptr<TParquetSparseRandomAccessFile> Create(std::shared_ptr<TParquetSparseFile> file) {
        return std::make_shared<TParquetSparseRandomAccessFile>(std::move(file));
    }

    explicit TParquetSparseRandomAccessFile(std::shared_ptr<TParquetSparseFile> file)
        : File(std::move(file))
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

        auto data = File->ReadBytes(static_cast<ui64>(position), static_cast<ui64>(nbytes));
        if (!data) {
            return arrow::Status::Invalid("parquet byte range is not loaded");
        }

        return arrow::Buffer::FromString(std::move(*data));
    }

private:
    std::shared_ptr<TParquetSparseFile> File;
    int64_t Position = 0;
};

} // anonymous namespace

TParquetSparseFile::TParquetSparseFile(ui64 fileSize)
    : FileSize(fileSize)
{
}

void TParquetSparseFile::PutRange(ui64 offset, TString data) {
    if (data.empty()) {
        return;
    }

    Y_ENSURE(offset + data.size() <= FileSize, "parquet range write past EOF");

    if (HasBytes(offset, data.size())) {
        return;
    }

    ui64 writeOffset = offset;
    while (!data.empty() && HasBytes(writeOffset, 1)) {
        data.erase(0, 1);
        ++writeOffset;
    }

    if (data.empty()) {
        return;
    }

    for (auto&& segment : Segments) {
        const ui64 segmentEnd = segment.first + segment.second.size();
        if (writeOffset < segmentEnd && writeOffset + data.size() > segment.first) {
            Y_ENSURE(writeOffset >= segmentEnd || writeOffset + data.size() <= segment.first,
                "parquet sparse file ranges must not overlap");
        }
    }

    BufferedBytes_ += data.size();
    Segments.emplace_back(writeOffset, std::move(data));
    SortBy(Segments, [](const auto& segment) { return segment.first; });
}

bool TParquetSparseFile::HasBytes(ui64 offset, ui64 length) const {
    if (length == 0) {
        return true;
    }

    ui64 covered = 0;
    while (covered < length) {
        const ui64 pos = offset + covered;
        bool found = false;
        for (auto&& segment : Segments) {
            const ui64 segmentStart = segment.first;
            const ui64 segmentEnd = segment.first + segment.second.size();
            if (pos < segmentStart || pos >= segmentEnd) {
                continue;
            }

            const ui64 available = segmentEnd - pos;
            covered += available;
            found = true;
            break;
        }

        if (!found) {
            return false;
        }
    }

    return true;
}

TMaybe<TString> TParquetSparseFile::ReadBytes(ui64 offset, ui64 length) const {
    if (!HasBytes(offset, length)) {
        return Nothing();
    }

    TString out;
    out.reserve(length);

    ui64 remaining = length;
    ui64 pos = offset;
    while (remaining > 0) {
        for (auto&& segment : Segments) {
            const ui64 segmentStart = segment.first;
            const ui64 segmentEnd = segment.first + segment.second.size();
            if (pos < segmentStart || pos >= segmentEnd) {
                continue;
            }

            const ui64 inSegment = pos - segmentStart;
            const ui64 toCopy = Min(remaining, segment.second.size() - inSegment);
            out.append(segment.second.data() + inSegment, toCopy);
            pos += toCopy;
            remaining -= toCopy;
            break;
        }
    }

    return out;
}

bool TParquetSparseFile::IsFullyBuffered() const {
    return HasBytes(0, FileSize);
}

std::shared_ptr<arrow::io::RandomAccessFile> TParquetSparseFile::MakeRandomAccessFile(
    const std::shared_ptr<TParquetSparseFile>& owner) const
{
    return TParquetSparseRandomAccessFile::Create(owner);
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

std::expected<TVector<TParquetFetchRange>, TString> TParquetSparseFile::PlanColumnChunkRanges(
    const std::shared_ptr<TParquetSparseFile>& owner) const
{
    try {
        auto source = MakeRandomAccessFile(owner);
        const auto metadata = parquet::ReadMetaData(source);

        TVector<ReadRange> ranges;
        ranges.reserve(metadata->num_row_groups() * metadata->num_columns());
        for (int32_t row = 0; row < metadata->num_row_groups(); ++row) {
            for (int32_t col = 0; col < metadata->num_columns(); ++col) {
                ranges.push_back(ComputeColumnChunkRange(
                    metadata.get(),
                    static_cast<int64_t>(FileSize),
                    row,
                    col));
            }
        }

        return SubtractLoadedRanges(CoalesceReadRanges(std::move(ranges)), *this);
    } catch (const parquet::ParquetException& ex) {
        return std::unexpected(TString(ex.what()));
    } catch (const std::exception& ex) {
        return std::unexpected(TString(ex.what()));
    }
}

std::expected<TVector<TVector<TParquetFetchRange>>, TString>
TParquetSparseFile::PlanColumnChunkRangesByRowGroup(
    const std::shared_ptr<TParquetSparseFile>& owner) const
{
    try {
        auto source = MakeRandomAccessFile(owner);
        const auto metadata = parquet::ReadMetaData(source);
        TVector<TVector<TParquetFetchRange>> outRanges;
        outRanges.resize(metadata->num_row_groups());

        for (int32_t row = 0; row < metadata->num_row_groups(); ++row) {
            TVector<ReadRange> ranges;
            ranges.reserve(metadata->num_columns());
            for (int32_t col = 0; col < metadata->num_columns(); ++col) {
                ranges.push_back(ComputeColumnChunkRange(
                    metadata.get(),
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
    TVector<std::pair<ui64, TString>> retained;
    retained.reserve(Segments.size());

    ui64 bufferedBytes = 0;
    for (auto& segment : Segments) {
        const ui64 segmentStart = segment.first;
        const ui64 segmentEnd = segmentStart + segment.second.size();
        if (segmentEnd <= offset) {
            continue;
        }

        if (segmentStart < offset) {
            segment.second.erase(0, offset - segmentStart);
            segment.first = offset;
        }

        bufferedBytes += segment.second.size();
        retained.push_back(std::move(segment));
    }

    Segments = std::move(retained);
    BufferedBytes_ = bufferedBytes;
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
