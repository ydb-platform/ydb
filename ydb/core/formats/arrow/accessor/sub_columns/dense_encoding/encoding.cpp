#include "encoding.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/data.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bit_run_reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bit_util.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bitmap_ops.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/byte_stream_split.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/endian.h>

#include <type_traits>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

void VerifyLittleEndian() {
    AFL_VERIFY(ARROW_LITTLE_ENDIAN)("endianness", "only little endian serialization/deserialization is supported");
}

size_t GetBitmapSize(const ui32 recordsCount) {
    return recordsCount / CHAR_BIT + (recordsCount % CHAR_BIT != 0);
}

size_t GetFrameMaxSize(const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    if (!codec) {
        return sizeof(ui32) + payload.size();
    }
    return sizeof(ui32) + codec->MaxCompressedLen(payload.size(), reinterpret_cast<const uint8_t*>(payload.data()));
}

size_t GetSectionMaxSize(const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    return sizeof(ui32) + GetFrameMaxSize(payload, codec);
}

// [uncompressed-size][payload], payload compressed with codec (or stored raw when codec is null).
void AppendFrameCompressed(TString& out, const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(payload.size() <= Max<ui32>())("size", payload.size());
    const ui32 rawSize = payload.size();
    out.append(reinterpret_cast<const char*>(&rawSize), sizeof(rawSize));
    if (!codec) {
        out.append(payload);
        return;
    }
    const i64 maxLen = codec->MaxCompressedLen(payload.size(), reinterpret_cast<const uint8_t*>(payload.data()));
    const size_t compressedPosition = out.size();
    out.ReserveAndResize(compressedPosition + maxLen);
    const i64 actual = TStatusValidator::GetValid(
        codec->Compress(payload.size(), reinterpret_cast<const uint8_t*>(payload.data()), maxLen,
            reinterpret_cast<uint8_t*>(out.Detach()) + compressedPosition));
    out.resize(compressedPosition + actual);
}

TString FrameCompress(const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    TString out;
    AppendFrameCompressed(out, payload, codec);
    return out;
}

std::shared_ptr<arrow::Buffer> FrameDecompress(TStringBuf blob, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(blob.size() >= sizeof(ui32))("size", blob.size());
    ui32 rawSize;
    memcpy(&rawSize, blob.data(), sizeof(rawSize));
    const TStringBuf payload = blob.SubStr(sizeof(rawSize));
    if (!codec) {
        AFL_VERIFY(payload.size() == rawSize)("payload", payload.size())("raw", rawSize);
    }
    auto raw = TStatusValidator::GetValid(arrow::AllocateBuffer(rawSize));
    if (!codec) {
        if (rawSize) {
            memcpy(raw->mutable_data(), payload.data(), rawSize);
        }
        return raw;
    }
    TStatusValidator::GetValid(codec->Decompress(
        payload.size(), reinterpret_cast<const uint8_t*>(payload.data()), rawSize, raw->mutable_data()));
    return raw;
}

// Extract arrow validity bitmap for serialization, make a non-owning memory view if possible.
class TValidityBitmap {
private:
    TString Storage;
    TStringBuf Data;

public:
    explicit TValidityBitmap(const arrow::Array& array) {
        if (array.null_count() == 0) {
            return;
        }
        const i64 bytesCount = (array.length() + CHAR_BIT - 1) / CHAR_BIT;
        // We want to reference the underlying array bitmap without copying, but it is possible
        // only if it starts at byte border (can be otherwise if it is a slice of another array - then copy).
        if (array.offset() % CHAR_BIT == 0) {
            Data = TStringBuf(reinterpret_cast<const char*>(array.null_bitmap_data()) + array.offset() / CHAR_BIT, bytesCount);
            return;
        }
        Storage.ReserveAndResize(bytesCount);
        arrow::internal::CopyBitmap(
            array.null_bitmap_data(), array.offset(), array.length(), reinterpret_cast<uint8_t*>(Storage.Detach()), 0);
        Data = Storage;
    }

    TStringBuf GetData() const {
        return Data;
    }
};

ui32 GetIndexByteWidth(const arrow::FixedWidthType& type) {
    return type.bit_width() / CHAR_BIT;
}

template <class TOutput, class TInput>
void CopyIndices(const TInput* values, const i64 length, const TStringBuf validity, char* output, size_t& outputPosition) {
    // If types are different, each value must be converted
    const auto copyRun = [&](const i64 position, const i64 count) {
        if constexpr (std::is_same_v<TOutput, TInput>) {
            const size_t size = count * sizeof(TOutput);
            memcpy(output + outputPosition, values + position, size);
            outputPosition += size;
        } else {
            for (i64 i = 0; i < count; ++i) {
                const TInput input = values[position + i];
                AFL_VERIFY(input <= Max<TOutput>())("value", input)("max", Max<TOutput>());
                const TOutput value = input;
                memcpy(output + outputPosition, &value, sizeof(value));
                outputPosition += sizeof(value);
            }
        }
    };
    if (validity.empty()) {
        copyRun(0, length);
    } else {
        arrow::internal::VisitSetBitRunsVoid(
            reinterpret_cast<const ui8*>(validity.data()), 0, length,
            [&](const i64 position, const i64 count) { copyRun(position, count); });
    }
}

template <class T>
TString EncodeLengthsImpl(const TConstArrayRef<ui32> values) {
    TVector<T> lengths(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        lengths[i] = values[i];
    }
    TString out;
    out.ReserveAndResize(1 + sizeof(T) * values.size());
    out[0] = sizeof(T);
    if (values.size()) {
        arrow::util::internal::ByteStreamSplitEncode<T>(reinterpret_cast<const uint8_t*>(lengths.data()), lengths.size(),
            reinterpret_cast<uint8_t*>(out.Detach()) + 1);
    }
    return out;
}

template <class T>
TVector<ui32> DecodeLengthsImpl(const TConstArrayRef<ui8> data, const ui32 count) {
    AFL_VERIFY(data.size() == 1ull + sizeof(T) * count)("size", data.size())("count", count);
    TVector<T> lengths(count);
    if (count) {
        arrow::util::internal::ByteStreamSplitDecode<T>(data.data() + 1, count, count, lengths.data());
    }
    TVector<ui32> values(count);
    for (ui32 i = 0; i < count; ++i) {
        values[i] = lengths[i];
    }
    return values;
}

// An independently-compressed section: [total-size][FrameCompress(raw)]. Compressing each buffer on
// its own (as arrow IPC does) gives it its own zstd entropy tables, instead of having its statistics
// swamped by a much larger neighbouring buffer.
void AppendSection(TString& out, const TStringBuf raw, const std::shared_ptr<arrow::util::Codec>& codec) {
    const size_t sizePosition = out.size();
    out.append(sizeof(ui32), '\0');
    const size_t contentPosition = out.size();
    AppendFrameCompressed(out, raw, codec);
    const size_t encodedSize = out.size() - contentPosition;
    AFL_VERIFY(encodedSize <= Max<ui32>())("size", encodedSize);
    const ui32 encodedSize32 = encodedSize;
    memcpy(out.Detach() + sizePosition, &encodedSize32, sizeof(encodedSize32));
}

std::shared_ptr<arrow::Buffer> ReadSection(const TStringBuf blob, size_t& pos, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(pos <= blob.size() && blob.size() - pos >= sizeof(ui32))("size", blob.size())("pos", pos);
    ui32 sectionSize;
    memcpy(&sectionSize, blob.data() + pos, sizeof(sectionSize));
    pos += sizeof(sectionSize);
    AFL_VERIFY(sectionSize <= blob.size() - pos)("size", blob.size())("pos", pos)("section_size", sectionSize);
    const auto result = FrameDecompress(TStringBuf(blob.data() + pos, sectionSize), codec);
    pos += sectionSize;
    return result;
}

// Inverse of StartPayload: the bitmap buffer, the present count and where the int stream starts.
struct TParsedPrefix {
    std::shared_ptr<arrow::Buffer> NullBitmap;
    ui32 PresentCount = 0;
    size_t Position = 0;
};

TParsedPrefix ParsePrefix(const std::shared_ptr<arrow::Buffer>& raw, const ui32 recordsCount) {
    TParsedPrefix result;
    AFL_VERIFY(raw->size() >= 1);
    const size_t rawSize = static_cast<size_t>(raw->size());
    size_t pos = 0;
    const char hasNulls = reinterpret_cast<const char*>(raw->data())[pos++];
    AFL_VERIFY(hasNulls == 0 || hasNulls == 1)("has_nulls", hasNulls);
    if (hasNulls) {
        const size_t bmBytes = GetBitmapSize(recordsCount);
        AFL_VERIFY(bmBytes <= rawSize - pos)("size", raw->size())("pos", pos)("bitmap_size", bmBytes);
        result.NullBitmap = arrow::SliceBuffer(raw, static_cast<i64>(pos), static_cast<i64>(bmBytes));
        result.PresentCount = arrow::internal::CountSetBits(result.NullBitmap->data(), 0, recordsCount);
        pos += bmBytes;
    } else {
        result.PresentCount = recordsCount;
    }
    result.Position = pos;
    return result;
}

std::shared_ptr<arrow::Buffer> DecodeDenseValues(
    const std::shared_ptr<arrow::Buffer>& raw, const TParsedPrefix& prefix, const ui32 recordsCount, const ui32 width) {
    const size_t rawSize = static_cast<size_t>(raw->size());
    AFL_VERIFY(prefix.Position <= rawSize)("size", raw->size())("pos", prefix.Position);
    const TStringBuf encoded(reinterpret_cast<const char*>(raw->data()) + prefix.Position, rawSize - prefix.Position);
    if (!prefix.NullBitmap) {
        if (width == sizeof(ui8)) {
            return arrow::SliceBuffer(raw, static_cast<i64>(prefix.Position), static_cast<i64>(encoded.size()));
        }
        // Wider index values require proper alignment, so copy to arrow-allocated buffer.
        auto values = TStatusValidator::GetValid(arrow::AllocateBuffer(encoded.size()));
        if (encoded.size()) {
            memcpy(values->mutable_data(), encoded.data(), encoded.size());
        }
        return values;
    }
    const size_t valuesSize = static_cast<size_t>(recordsCount) * width;
    auto values = TStatusValidator::GetValid(arrow::AllocateBuffer(valuesSize));
    char* out = reinterpret_cast<char*>(values->mutable_data());
    size_t encodedPosition = 0;
    size_t previousPosition = 0;
    arrow::internal::VisitSetBitRunsVoid(prefix.NullBitmap->data(), 0, recordsCount, [&](const i64 position, const i64 count) {
        const size_t bytesBefore = (position - previousPosition) * width;
        memset(out + previousPosition * width, 0, bytesBefore);
        const size_t bytes = count * width;
        memcpy(out + position * width, encoded.data() + encodedPosition, bytes);
        encodedPosition += bytes;
        previousPosition = position + count;
    });
    memset(out + previousPosition * width, 0, valuesSize - previousPosition * width);
    AFL_VERIFY(encodedPosition == encoded.size())("actual", encodedPosition)("expected", encoded.size());
    return values;
}

}   // namespace

TString EncodeLengths(TConstArrayRef<ui32> values) {
    VerifyLittleEndian();
    ui32 maxLength = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        maxLength = Max(maxLength, values[i]);
    }
    if (maxLength <= Max<ui8>()) {
        return EncodeLengthsImpl<ui8>(values);
    }
    if (maxLength <= Max<ui16>()) {
        return EncodeLengthsImpl<ui16>(values);
    }
    return EncodeLengthsImpl<ui32>(values);
}

TVector<ui32> DecodeLengths(TConstArrayRef<ui8> data, ui32 count) {
    VerifyLittleEndian();
    AFL_VERIFY(data.size());
    switch (data[0]) {
        case sizeof(ui8):
            return DecodeLengthsImpl<ui8>(data, count);
        case sizeof(ui16):
            return DecodeLengthsImpl<ui16>(data, count);
        case sizeof(ui32):
            return DecodeLengthsImpl<ui32>(data, count);
    }
    AFL_VERIFY(false)("width", data[0]);
    return {};
}

TString SerializeBinaryLikeArray(const arrow::BinaryArray& array, const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    TVector<ui32> lengths;
    lengths.reserve(array.length() - array.null_count());
    i64 presentValuesLength = 0;
    for (i64 i = 0; i < array.length(); ++i) {
        if (array.IsNull(i)) {
            continue;
        }
        const auto view = array.GetView(i);
        AFL_VERIFY(view.size() <= Max<ui32>())("size", view.size());
        lengths.emplace_back(static_cast<ui32>(view.size()));
        presentValuesLength += view.size();
    }

    const i64 valuesLength = array.total_values_length();
    AFL_VERIFY(presentValuesLength <= valuesLength)("present", presentValuesLength)("total", valuesLength);
    TString values;
    TStringBuf valuesData(values);
    // Null entries may have physical bytes in buffer. Borrow the values range only when it does not have any.
    // Check this by comparing sum of individual non-null lengths with total length.
    if (presentValuesLength == valuesLength) {
        if (valuesLength) {
            const auto valueData = array.value_data();
            AFL_VERIFY(valueData);
            valuesData = TStringBuf(reinterpret_cast<const char*>(valueData->data()) + array.value_offset(0), valuesLength);
        }
    } else {
        values.reserve(presentValuesLength);
        for (i64 i = 0; i < array.length(); ++i) {
            if (!array.IsNull(i)) {
                const auto view = array.GetView(i);
                values.append(view.data(), view.size());
            }
        }
        valuesData = values;
    }

    const TValidityBitmap validity(array);
    const TStringBuf validityData = validity.GetData();
    const TString encodedLengths = EncodeLengths(lengths);
    TString out;
    const char hasNulls = validityData.empty() ? 0 : 1;
    const size_t outputCapacity = 1 + (hasNulls ? GetSectionMaxSize(validityData, codec) : 0) +
        GetSectionMaxSize(encodedLengths, codec) + GetSectionMaxSize(valuesData, codec);
    out.reserve(outputCapacity);
    out.append(&hasNulls, 1);
    if (hasNulls) {
        AppendSection(out, validityData, codec);
    }
    AppendSection(out, encodedLengths, codec);
    AppendSection(out, valuesData, codec);
    return out;
}

namespace {

std::shared_ptr<arrow::ArrayData> DeserializeBinaryLikeArrayData(TStringBuf blob, ui32 recordsCount,
    const std::shared_ptr<arrow::DataType>& valueType, const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    size_t pos = 0;
    AFL_VERIFY(blob.size() >= 1);
    const char hasNulls = blob[pos++];
    AFL_VERIFY(hasNulls == 0 || hasNulls == 1)("has_nulls", static_cast<ui32>(hasNulls));

    std::shared_ptr<arrow::Buffer> nullBitmap;
    ui32 presentCount = recordsCount;
    if (hasNulls) {
        nullBitmap = ReadSection(blob, pos, codec);
        const i64 bitmapSize = GetBitmapSize(recordsCount);
        AFL_VERIFY(nullBitmap->size() == bitmapSize)("size", nullBitmap->size())("records", recordsCount);
        presentCount = arrow::internal::CountSetBits(nullBitmap->data(), 0, recordsCount);
    }
    const auto lengthsRaw = ReadSection(blob, pos, codec);
    const TConstArrayRef<ui8> lengthsData(lengthsRaw->data(), lengthsRaw->size());
    const TVector<ui32> lengths = DecodeLengths(lengthsData, presentCount);
    const auto values = ReadSection(blob, pos, codec);
    AFL_VERIFY(pos == blob.size())("pos", pos)("size", blob.size());

    // Here we directly build arrow array buffers (validity, offsets, values), because we conveniently have all the data for it.
    size_t offsetsCount = recordsCount;
    ++offsetsCount;
    const std::shared_ptr<arrow::Buffer> offsetsBuffer = TStatusValidator::GetValid(arrow::AllocateBuffer(sizeof(int32_t) * offsetsCount));
    auto* offsets = reinterpret_cast<int32_t*>(offsetsBuffer->mutable_data());
    offsets[0] = 0;
    ui32 present = 0;
    for (ui32 i = 0; i < recordsCount; ++i) {
        if (!nullBitmap || arrow::BitUtil::GetBit(nullBitmap->data(), i)) {
            AFL_VERIFY(present < lengths.size())("present", present)("lengths", lengths.size());
            AFL_VERIFY(offsets[i] >= 0)("offset", offsets[i]);
            const ui32 remaining = Max<int32_t>() - offsets[i];
            AFL_VERIFY(lengths[present] <= remaining)("length", lengths[present])("offset", offsets[i]);
            offsets[i + 1] = offsets[i] + static_cast<int32_t>(lengths[present++]);
        } else {
            offsets[i + 1] = offsets[i];
        }
    }
    AFL_VERIFY(present == lengths.size())("present", present)("lengths", lengths.size());
    AFL_VERIFY(values->size() <= Max<int32_t>())("values_len", values->size());
    const int32_t valuesSize = values->size();
    AFL_VERIFY(offsets[recordsCount] == valuesSize)("offsets_back", offsets[recordsCount])("values_len", values->size());
    return arrow::ArrayData::Make(valueType, recordsCount, { nullBitmap, offsetsBuffer, values }, nullBitmap ? arrow::kUnknownNullCount : 0);
}

}   // namespace

std::shared_ptr<arrow::BinaryArray> DeserializeBinaryLikeArray(TStringBuf blob, ui32 recordsCount,
    const std::shared_ptr<arrow::DataType>& valueType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(arrow::is_binary_like(valueType->id()))("type", valueType->ToString());
    return std::static_pointer_cast<arrow::BinaryArray>(
        arrow::MakeArray(DeserializeBinaryLikeArrayData(blob, recordsCount, valueType, codec)));
}

TString SerializeIndices(const std::shared_ptr<arrow::Array>& positions, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    const ui32 width = GetIndexByteWidth(*indexType);
    const i64 length = positions->length();

    const TValidityBitmap validity(*positions);
    const TStringBuf validityData = validity.GetData();
    const i64 encodedCount = length - positions->null_count();
    TString payload;
    payload.ReserveAndResize(1 + validityData.size() + width * encodedCount);
    char* output = payload.Detach();
    const char hasNulls = validityData.empty() ? 0 : 1;
    output[0] = hasNulls;
    size_t outputPosition = 1;
    if (hasNulls) {
        memcpy(output + outputPosition, validityData.data(), validityData.size());
        outputPosition += validityData.size();
    }
    if (length) {
        AFL_VERIFY(SwitchType(positions->type_id(), [&](const auto type) {
            if constexpr (type.IsIndexType()) {
                const auto* source = type.CastArray(positions.get());
                if (indexType->id() == arrow::Type::UINT8) {
                    CopyIndices<ui8>(source->raw_values(), length, validityData, output, outputPosition);
                    return true;
                }
                if (indexType->id() == arrow::Type::UINT16) {
                    CopyIndices<ui16>(source->raw_values(), length, validityData, output, outputPosition);
                    return true;
                }
                if (indexType->id() == arrow::Type::UINT32) {
                    CopyIndices<ui32>(source->raw_values(), length, validityData, output, outputPosition);
                    return true;
                }
            }
            return false;
        }))("positions_type", positions->type()->ToString())("index_type", indexType->ToString());
    }
    return FrameCompress(payload, codec);
}

std::shared_ptr<arrow::Array> DeserializeIndices(TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    const auto raw = FrameDecompress(blob, codec);
    const ui32 width = GetIndexByteWidth(*indexType);
    const auto prefix = ParsePrefix(raw, recordsCount);
    const ui32 encodedCount = prefix.PresentCount;
    const size_t encodedSize = static_cast<size_t>(encodedCount) * width;
    AFL_VERIFY(static_cast<size_t>(raw->size()) == prefix.Position + encodedSize)("size", raw->size())("count", encodedCount);

    const auto values = DecodeDenseValues(raw, prefix, recordsCount, width);
    auto data = arrow::ArrayData::Make(
        indexType, recordsCount, { prefix.NullBitmap, values }, prefix.NullBitmap ? arrow::kUnknownNullCount : 0);
    return arrow::MakeArray(data);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
