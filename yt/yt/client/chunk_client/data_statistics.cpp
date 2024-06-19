#include "data_statistics.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

using namespace NCompression;
using namespace NYTree;
using namespace NYson;

using ::ToString;
using ::FromString;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

bool HasInvalidDataWeight(const TDataStatistics& statistics)
{
    return statistics.has_data_weight() && statistics.data_weight() == -1;
}

bool HasInvalidUnmergedRowCount(const TDataStatistics& statistics)
{
    return statistics.has_unmerged_row_count() && statistics.unmerged_row_count() == -1;
}

bool HasInvalidUnmergedDataWeight(const TDataStatistics& statistics)
{
    return statistics.has_unmerged_data_weight() && statistics.unmerged_data_weight() == -1;
}

TDataStatistics& operator += (TDataStatistics& lhs, const TDataStatistics& rhs)
{
    lhs.set_uncompressed_data_size(lhs.uncompressed_data_size() + rhs.uncompressed_data_size());
    lhs.set_compressed_data_size(lhs.compressed_data_size() + rhs.compressed_data_size());
    lhs.set_chunk_count(lhs.chunk_count() + rhs.chunk_count());
    lhs.set_row_count(lhs.row_count() + rhs.row_count());
    lhs.set_regular_disk_space(lhs.regular_disk_space() + rhs.regular_disk_space());
    lhs.set_erasure_disk_space(lhs.erasure_disk_space() + rhs.erasure_disk_space());
    lhs.set_encoded_row_batch_count(lhs.encoded_row_batch_count() + rhs.encoded_row_batch_count());
    lhs.set_encoded_columnar_batch_count(lhs.encoded_columnar_batch_count() + rhs.encoded_columnar_batch_count());

    if (HasInvalidDataWeight(lhs) || HasInvalidDataWeight(rhs)) {
        lhs.set_data_weight(-1);
    } else {
        lhs.set_data_weight(lhs.data_weight() + rhs.data_weight());
    }

    if (HasInvalidUnmergedRowCount(lhs) || HasInvalidUnmergedRowCount(rhs)) {
        lhs.set_unmerged_row_count(-1);
    } else {
        lhs.set_unmerged_row_count(lhs.unmerged_row_count() + rhs.unmerged_row_count());
    }

    if (HasInvalidUnmergedDataWeight(lhs) || HasInvalidUnmergedDataWeight(rhs)) {
        lhs.set_unmerged_data_weight(-1);
    } else {
        lhs.set_unmerged_data_weight(lhs.unmerged_data_weight() + rhs.unmerged_data_weight());
    }

    return lhs;
}

TDataStatistics operator + (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

bool operator == (const TDataStatistics& lhs, const TDataStatistics& rhs)
{
    return
        lhs.uncompressed_data_size() == rhs.uncompressed_data_size() &&
        lhs.compressed_data_size() == rhs.compressed_data_size() &&
        lhs.row_count() == rhs.row_count() &&
        lhs.chunk_count() == rhs.chunk_count() &&
        lhs.regular_disk_space() == rhs.regular_disk_space() &&
        lhs.erasure_disk_space() == rhs.erasure_disk_space() &&
        (HasInvalidDataWeight(lhs) || HasInvalidDataWeight(rhs) || lhs.data_weight() == rhs.data_weight()) &&
        (
            HasInvalidUnmergedRowCount(lhs) ||
            HasInvalidUnmergedRowCount(rhs) ||
            lhs.unmerged_row_count() == rhs.unmerged_row_count()) &&
        (
            HasInvalidUnmergedDataWeight(lhs) ||
            HasInvalidUnmergedDataWeight(rhs) ||
            lhs.unmerged_data_weight() == rhs.unmerged_data_weight());
}

void Serialize(const TDataStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    // TODO(max42): replace all Item with OptionalItem in order to expose only meaningful
    // fields in each particular context. This would require fixing some tests or using
    // manually constructed data statistics with some fields being explicitly set to zero
    // as a neutral element.
    BuildYsonFluently(consumer).BeginMap()
        .Item("chunk_count").Value(statistics.chunk_count())
        .Item("row_count").Value(statistics.row_count())
        .Item("uncompressed_data_size").Value(statistics.uncompressed_data_size())
        .Item("compressed_data_size").Value(statistics.compressed_data_size())
        .Item("data_weight").Value(statistics.data_weight())
        .Item("regular_disk_space").Value(statistics.regular_disk_space())
        .Item("erasure_disk_space").Value(statistics.erasure_disk_space())
        .Item("unmerged_row_count").Value(statistics.unmerged_row_count())
        .Item("unmerged_data_weight").Value(statistics.unmerged_data_weight())
        .Item("encoded_row_batch_count").Value(statistics.encoded_row_batch_count())
        .Item("encoded_columnar_batch_count").Value(statistics.encoded_columnar_batch_count())
    .EndMap();
}

void SetDataStatisticsField(TDataStatistics& statistics, TStringBuf key, i64 value)
{
    if (key == "chunk_count") {
        statistics.set_chunk_count(value);
    } else if (key == "row_count") {
        statistics.set_row_count(value);
    } else if (key == "uncompressed_data_size") {
        statistics.set_uncompressed_data_size(value);
    } else if (key == "compressed_data_size") {
        statistics.set_compressed_data_size(value);
    } else if (key == "data_weight") {
        statistics.set_data_weight(value);
    } else if (key == "regular_disk_space") {
        statistics.set_regular_disk_space(value);
    } else if (key == "erasure_disk_space") {
        statistics.set_erasure_disk_space(value);
    } else if (key == "unmerged_row_count") {
        statistics.set_unmerged_row_count(value);
    } else if (key == "unmerged_data_weight") {
        statistics.set_unmerged_data_weight(value);
    } // Else we have a strange situation on our hands but we intentionally ignore it.
}

void FormatValue(TStringBuilderBase* builder, const TDataStatistics& statistics, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{UncompressedDataSize: %v, CompressedDataSize: %v, DataWeight: %v, RowCount: %v, "
        "ChunkCount: %v, RegularDiskSpace: %v, ErasureDiskSpace: %v, "
        "UnmergedRowCount: %v, UnmergedDataWeight: %v}",
        statistics.uncompressed_data_size(),
        statistics.compressed_data_size(),
        statistics.data_weight(),
        statistics.row_count(),
        statistics.chunk_count(),
        statistics.regular_disk_space(),
        statistics.erasure_disk_space(),
        statistics.unmerged_row_count(),
        statistics.unmerged_data_weight());
}

void FormatValue(TStringBuilderBase* builder, const TDataStatistics* statistics, TStringBuf spec)
{
    if (statistics) {
        FormatValue(builder, *statistics, spec);
    } else {
        FormatValue(builder, std::nullopt, spec);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

TCodecStatistics& TCodecStatistics::Append(const TCodecDuration& codecTime)
{
    return Append(std::pair(codecTime.Codec, codecTime.CpuDuration));
}

TCodecStatistics& TCodecStatistics::Append(const std::pair<ECodec, TDuration>& codecTime)
{
    CodecToDuration_[codecTime.first] += codecTime.second;
    TotalDuration_ += codecTime.second;
    return *this;
}

TCodecStatistics& TCodecStatistics::AppendToValueDictionaryCompression(TDuration duration)
{
    ValueDictionaryCompressionDuration_ += duration;
    TotalDuration_ += duration;
    return *this;
}

TCodecStatistics& TCodecStatistics::operator+=(const TCodecStatistics& other)
{
    for (const auto& pair : other.CodecToDuration_) {
        Append(pair);
    }
    AppendToValueDictionaryCompression(other.ValueDictionaryCompressionDuration_);
    return *this;
}

TDuration TCodecStatistics::GetTotalDuration() const
{
    return TotalDuration_;
}

void FormatValue(TStringBuilderBase* builder, const TCodecStatistics& statistics, TStringBuf /* spec */)
{
    ::NYT::FormatKeyValueRange(builder, statistics.CodecToDuration(), TDefaultFormatter());
    if (statistics.ValueDictionaryCompressionDuration() != TDuration::Zero()) {
        builder->AppendFormat(", ValueDictionaryCompressionDuration: %v",
            statistics.ValueDictionaryCompressionDuration());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
