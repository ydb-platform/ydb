#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_parquet_s3_file.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
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

static uint32_t ParseFooterLength(const uint8_t* data, int64_t footerReadSize, ui64 sourceSize, TString& error) {
    if (footerReadSize < static_cast<int64_t>(kFooterSize)) {
        error = "parquet footer is too small";
        return 0;
    }

    if (memcmp(data + footerReadSize - 4, parquet::kParquetMagic, 4) != 0 &&
        memcmp(data + footerReadSize - 4, parquet::kParquetEMagic, 4) != 0) {
        error = "parquet magic bytes not found in footer";
        return 0;
    }

    const uint32_t metadataLen = ::arrow::util::SafeLoadAs<uint32_t>(
        reinterpret_cast<const uint8_t*>(data + footerReadSize - kFooterSize));
    if (metadataLen > sourceSize - kFooterSize) {
        error = TStringBuilder() << "parquet metadata length " << metadataLen
            << " exceeds file size " << sourceSize;
        return 0;
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

        while (pos < rangeEnd) {
            ui64 loaded = 0;
            while (pos + loaded < rangeEnd && file.HasBytes(pos + loaded, 1)) {
                ++loaded;
            }

            if (loaded > 0) {
                pos += loaded;
                continue;
            }

            ui64 gapStart = pos;
            while (pos < rangeEnd && !file.HasBytes(pos, 1)) {
                ++pos;
            }

            const ui64 gapLength = pos - gapStart;
            if (gapLength > 0) {
                TParquetFetchRange fetch;
                fetch.Offset = gapStart;
                fetch.Length = gapLength;
                fetch.Fetched = 0;
                result.push_back(fetch);
            }
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

        TString data;
        if (!File->ReadBytes(static_cast<ui64>(position), static_cast<ui64>(nbytes), data)) {
            return arrow::Status::Invalid("parquet byte range is not loaded");
        }

        return std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(data.data()),
            static_cast<int64_t>(data.size()));
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

bool TParquetSparseFile::ReadBytes(ui64 offset, ui64 length, TString& out) const {
    if (!HasBytes(offset, length)) {
        return false;
    }

    out.clear();
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

    return true;
}

bool TParquetSparseFile::IsFullyBuffered() const {
    return HasBytes(0, FileSize);
}

std::shared_ptr<arrow::io::RandomAccessFile> TParquetSparseFile::MakeRandomAccessFile(
    const std::shared_ptr<TParquetSparseFile>& owner) const
{
    if (IsFullyBuffered()) {
        TString data;
        Y_ENSURE(ReadBytes(0, FileSize, data));
        return std::make_shared<arrow::io::BufferReader>(
            arrow::Buffer::FromString(std::move(data)));
    }

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

bool TParquetSparseFile::TryParseFooterMetadataRange(TString& error, TMaybe<TParquetFetchRange>& metadataRange) const {
    const int64_t footerReadSize = GetFooterReadSize(FileSize);
    if (footerReadSize < 0) {
        error = "parquet file is too small";
        return false;
    }

    const ui64 footerOffset = FileSize - static_cast<ui64>(footerReadSize);
    TString footerData;
    if (!ReadBytes(footerOffset, static_cast<ui64>(footerReadSize), footerData)) {
        error = "parquet footer tail is not loaded";
        return false;
    }

    const uint32_t metadataLen = ParseFooterLength(
        reinterpret_cast<const uint8_t*>(footerData.data()),
        footerReadSize,
        FileSize,
        error);
    if (!error.empty()) {
        return false;
    }

    if (static_cast<ui64>(footerReadSize) >= static_cast<ui64>(metadataLen) + kFooterSize) {
        return true;
    }

    const ui64 metadataOffset = FileSize - kFooterSize - metadataLen;
    if (HasBytes(metadataOffset, metadataLen)) {
        return true;
    }

    TParquetFetchRange range;
    range.Offset = metadataOffset;
    range.Length = metadataLen;
    metadataRange = range;
    return true;
}

bool TParquetSparseFile::PlanColumnChunkRanges(
    const std::shared_ptr<TParquetSparseFile>& owner,
    TString& error,
    TVector<TParquetFetchRange>& outRanges) const
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

        outRanges = SubtractLoadedRanges(CoalesceReadRanges(std::move(ranges)), *this);
        return true;
    } catch (const parquet::ParquetException& ex) {
        error = ex.what();
        return false;
    } catch (const std::exception& ex) {
        error = ex.what();
        return false;
    }
}

void TParquetSparseFile::AppendChecksum(const std::function<void(TStringBuf)>& addChunk) const {
    static constexpr ui64 kChunkSize = 1 << 20;

    ui64 offset = 0;
    while (offset < FileSize) {
        const ui64 length = Min(kChunkSize, FileSize - offset);
        TString chunk;
        Y_ENSURE(ReadBytes(offset, length, chunk));
        addChunk(chunk);
        offset += length;
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
