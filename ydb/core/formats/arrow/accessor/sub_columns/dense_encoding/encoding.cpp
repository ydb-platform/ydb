#include "encoding.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/data.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bitmap_ops.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/byte_stream_split.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/endian.h>

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
    return sizeof(ui32) + codec->MaxCompressedLen(payload.size(), (const uint8_t*)payload.data());
}

size_t GetSectionMaxSize(const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    return sizeof(ui32) + GetFrameMaxSize(payload, codec);
}

// [uncompressed-size][payload], payload compressed with codec (or stored raw when codec is null).
void AppendFrameCompressed(TString& out, const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(payload.size() <= Max<ui32>())("size", payload.size());
    const ui32 rawSize = payload.size();
    out.append((const char*)&rawSize, sizeof(rawSize));
    if (!codec) {
        out.append(payload);
        return;
    }
    const i64 maxLen = codec->MaxCompressedLen(payload.size(), (const uint8_t*)payload.data());
    const size_t compressedPosition = out.size();
    out.ReserveAndResize(compressedPosition + maxLen);
    const i64 actual = TStatusValidator::GetValid(
        codec->Compress(payload.size(), (const uint8_t*)payload.data(), maxLen, (uint8_t*)out.Detach() + compressedPosition));
    out.resize(compressedPosition + actual);
}

TString FrameCompress(const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
    TString out;
    AppendFrameCompressed(out, payload, codec);
    return out;
}

TString FrameDecompress(TStringBuf blob, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(blob.size() >= sizeof(ui32))("size", blob.size());
    ui32 rawSize;
    memcpy(&rawSize, blob.data(), sizeof(rawSize));
    const TStringBuf payload = blob.SubStr(sizeof(rawSize));
    if (!codec) {
        AFL_VERIFY(payload.size() == rawSize)("payload", payload.size())("raw", rawSize);
        return TString(payload);
    }
    TString raw;
    raw.ReserveAndResize(rawSize);
    TStatusValidator::GetValid(codec->Decompress(
        payload.size(), (const uint8_t*)payload.data(), rawSize, (uint8_t*)raw.Detach()));
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
        // only if the array is not a slice of another one with offset between byte borders (then copy).
        if (array.offset() % CHAR_BIT == 0) {
            Data = TStringBuf((const char*)array.null_bitmap_data() + array.offset() / CHAR_BIT, bytesCount);
            return;
        }
        Storage.ReserveAndResize(bytesCount);
        arrow::internal::CopyBitmap(
            array.null_bitmap_data(), array.offset(), array.length(), (uint8_t*)Storage.Detach(), 0);
        Data = Storage;
    }

    TStringBuf GetData() const {
        return Data;
    }
};

bool GetBit(const TStringBuf bitmap, const ui32 index) {
    return ((ui8)bitmap[index >> 3] >> (index & 7)) & 1;
}

ui32 CountSetBits(const TStringBuf bitmap, const ui32 count) {
    return arrow::internal::CountSetBits((const uint8_t*)bitmap.data(), 0, count);
}

ui32 GetIndexByteWidth(const arrow::FixedWidthType& type) {
    return type.bit_width() / CHAR_BIT;
}

std::shared_ptr<arrow::Buffer> CopyToBuffer(const void* data, size_t size) {
    auto buffer = TStatusValidator::GetValid(arrow::AllocateBuffer(size));
    if (size) {
        memcpy(buffer->mutable_data(), data, size);
    }
    return buffer;
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
        arrow::util::internal::ByteStreamSplitEncode<T>((const uint8_t*)lengths.data(), lengths.size(), (uint8_t*)out.Detach() + 1);
    }
    return out;
}

template <class T>
TVector<ui32> DecodeLengthsImpl(const TStringBuf data, const ui32 count) {
    AFL_VERIFY(data.size() == 1ull + sizeof(T) * count)("size", data.size())("count", count);
    TVector<T> lengths(count);
    if (count) {
        arrow::util::internal::ByteStreamSplitDecode<T>((const uint8_t*)data.data() + 1, count, count, lengths.data());
    }
    TVector<ui32> values(count);
    for (ui32 i = 0; i < count; ++i) {
        values[i] = lengths[i];
    }
    return values;
}

// A independently-compressed section: [total-size][FrameCompress(raw)]. Compressing each buffer on
// its own (as arrow IPC does) gives it its own zstd entropy tables, instead of having its statistics
// swamped by a much larger neighbouring buffer.
void AppendSection(TString& out, const TStringBuf raw, const std::shared_ptr<arrow::util::Codec>& codec) {
    const size_t sizePosition = out.size();
    out.append(sizeof(ui32), '\0');
    const size_t contentPosition = out.size();
    AppendFrameCompressed(out, raw, codec);
    const size_t encodedSize = out.size() - contentPosition;
    AFL_VERIFY(encodedSize <= Max<ui32>())("size", encodedSize);
    const ui32 totalSize = encodedSize;
    memcpy(out.Detach() + sizePosition, &totalSize, sizeof(totalSize));
}

TString ReadSection(const TStringBuf blob, size_t& pos, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(pos <= blob.size() && blob.size() - pos >= sizeof(ui32))("size", blob.size())("pos", pos);
    ui32 sectionSize;
    memcpy(&sectionSize, blob.data() + pos, sizeof(sectionSize));
    pos += sizeof(sectionSize);
    AFL_VERIFY(sectionSize <= blob.size() - pos)("size", blob.size())("pos", pos)("section_size", sectionSize);
    const TString result = FrameDecompress(TStringBuf(blob.data() + pos, sectionSize), codec);
    pos += sectionSize;
    return result;
}

// Inverse of StartPayload: the bitmap buffer, the present count and where the int stream starts.
struct TParsedPrefix {
    std::shared_ptr<arrow::Buffer> NullBitmap;
    TStringBuf Validity;
    ui32 PresentCount = 0;
    size_t Position = 0;

    bool IsValid(const ui32 index) const {
        return !NullBitmap || GetBit(Validity, index);
    }
};

TParsedPrefix ParsePrefix(const TString& raw, const ui32 recordsCount) {
    TParsedPrefix result;
    AFL_VERIFY(raw.size() >= 1);
    size_t pos = 0;
    const char hasNulls = raw[pos++];
    AFL_VERIFY(hasNulls == 0 || hasNulls == 1)("has_nulls", hasNulls);
    if (hasNulls) {
        const size_t bmBytes = GetBitmapSize(recordsCount);
        AFL_VERIFY(bmBytes <= raw.size() - pos)("size", raw.size())("pos", pos)("bitmap_size", bmBytes);
        result.Validity = TStringBuf(raw.data() + pos, bmBytes);
        result.NullBitmap = CopyToBuffer(raw.data() + pos, bmBytes);
        result.PresentCount = CountSetBits(result.Validity, recordsCount);
        pos += bmBytes;
    } else {
        result.PresentCount = recordsCount;
    }
    result.Position = pos;
    return result;
}

TString DecodeDenseValues(const TStringBuf encoded, const TParsedPrefix& prefix, const ui32 recordsCount, const ui32 width) {
    TString values;
    const size_t valuesSize = static_cast<size_t>(recordsCount) * width;
    values.ReserveAndResize(valuesSize);
    char* out = values.Detach();
    size_t encodedValueIndex = 0;
    for (size_t i = 0; i < recordsCount; ++i) {
        if (prefix.IsValid(i)) {
            memcpy(out + i * width, encoded.data() + encodedValueIndex++ * width, width);
        } else {
            memset(out + i * width, 0, width);
        }
    }
    AFL_VERIFY(encodedValueIndex == prefix.PresentCount)("actual", encodedValueIndex)("expected", prefix.PresentCount);
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

TVector<ui32> DecodeLengths(TStringBuf data, ui32 count) {
    VerifyLittleEndian();
    AFL_VERIFY(data.size());
    switch (static_cast<ui8>(data[0])) {
        case sizeof(ui8):
            return DecodeLengthsImpl<ui8>(data, count);
        case sizeof(ui16):
            return DecodeLengthsImpl<ui16>(data, count);
        case sizeof(ui32):
            return DecodeLengthsImpl<ui32>(data, count);
    }
    AFL_VERIFY(false)("width", static_cast<ui8>(data[0]));
    return {};
}

TString SerializeBinaryArray(const arrow::BinaryArray& array, const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    TVector<ui32> lengths;
    lengths.reserve(array.length() - array.null_count());
    TString values;
    values.reserve(array.total_values_length());
    for (i64 i = 0; i < array.length(); ++i) {
        if (array.IsNull(i)) {
            continue;
        }
        const auto view = array.GetView(i);
        values.append(view.data(), view.size());
        AFL_VERIFY(view.size() <= Max<ui32>())("size", view.size());
        lengths.emplace_back(static_cast<ui32>(view.size()));
    }

    const TValidityBitmap validity(array);
    const TStringBuf validityData = validity.GetData();
    const TString encodedLengths = EncodeLengths(lengths);
    TString out;
    const char hasNulls = validityData.empty() ? 0 : 1;
    const size_t outputCapacity = 1 + (hasNulls ? GetSectionMaxSize(validityData, codec) : 0) +
        GetSectionMaxSize(encodedLengths, codec) + GetSectionMaxSize(values, codec);
    out.reserve(outputCapacity);
    out.append(&hasNulls, 1);
    if (hasNulls) {
        AppendSection(out, validityData, codec);
    }
    AppendSection(out, encodedLengths, codec);
    AppendSection(out, values, codec);
    return out;
}

std::shared_ptr<arrow::BinaryArray> DeserializeBinaryArray(
    TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    size_t pos = 0;
    AFL_VERIFY(blob.size() >= 1);
    const char hasNulls = blob[pos++];
    AFL_VERIFY(hasNulls == 0 || hasNulls == 1)("has_nulls", static_cast<ui32>(hasNulls));

    std::shared_ptr<arrow::Buffer> nullBitmap;
    TString validity;
    ui32 presentCount = recordsCount;
    if (hasNulls) {
        validity = ReadSection(blob, pos, codec);
        AFL_VERIFY(validity.size() == GetBitmapSize(recordsCount))("size", validity.size())("records", recordsCount);
        nullBitmap = CopyToBuffer(validity.data(), validity.size());
        presentCount = CountSetBits(validity, recordsCount);
    }
    const TString lengthsRaw = ReadSection(blob, pos, codec);
    const TVector<ui32> lengths = DecodeLengths(lengthsRaw, presentCount);
    const TString values = ReadSection(blob, pos, codec);
    AFL_VERIFY(pos == blob.size())("pos", pos)("size", blob.size());

    // Here we directly build arrow array buffers (validity, offsets, values), because we conveniently have all the data for it.
    size_t offsetsCount = recordsCount;
    ++offsetsCount;
    TVector<int32_t> offsets(offsetsCount);
    ui32 present = 0;
    for (ui32 i = 0; i < recordsCount; ++i) {
        if (!nullBitmap || GetBit(validity, i)) {
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
    AFL_VERIFY(values.size() <= Max<int32_t>())("values_len", values.size());
    const int32_t valuesSize = values.size();
    AFL_VERIFY(offsets.back() == valuesSize)("offsets_back", offsets.back())("values_len", values.size());

    auto offsetsBuf = CopyToBuffer(offsets.data(), sizeof(int32_t) * offsets.size());
    auto valuesBuf = CopyToBuffer(values.data(), values.size());
    auto data = arrow::ArrayData::Make(
        arrow::binary(), recordsCount, { nullBitmap, offsetsBuf, valuesBuf }, nullBitmap ? arrow::kUnknownNullCount : 0);
    return std::static_pointer_cast<arrow::BinaryArray>(arrow::MakeArray(data));
}

TString SerializeIndices(const std::shared_ptr<arrow::Array>& positions, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    // reencode positions to specified arrow data type
    const auto casted = TStatusValidator::GetValid(arrow::compute::Cast(arrow::Datum(positions), indexType)).make_array();
    const ui32 width = GetIndexByteWidth(*indexType);
    const auto& data = *casted->data();
    const i64 length = casted->length();

    const TValidityBitmap validity(*casted);
    const TStringBuf validityData = validity.GetData();
    const i64 encodedCount = length - casted->null_count();
    TString payload;
    payload.reserve(1 + validityData.size() + width * encodedCount);
    const char hasNulls = validityData.empty() ? 0 : 1;
    payload.append(&hasNulls, 1);
    if (hasNulls) {
        payload.append(validityData);
    }
    if (length) {
        const ui8* values = data.buffers[1]->data() + data.offset * width;
        for (i64 i = 0; i < length; ++i) {
            if (!casted->IsNull(i)) {
                payload.append((const char*)(values + i * width), width);
            }
        }
    }
    return FrameCompress(payload, codec);
}

std::shared_ptr<arrow::Array> DeserializeIndices(TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    VerifyLittleEndian();
    const TString raw = FrameDecompress(blob, codec);
    const ui32 width = GetIndexByteWidth(*indexType);
    const auto prefix = ParsePrefix(raw, recordsCount);
    const ui32 encodedCount = prefix.PresentCount;
    size_t encodedSize = static_cast<size_t>(encodedCount) * width;
    AFL_VERIFY(raw.size() == prefix.Position + encodedSize)("size", raw.size())("count", encodedCount);

    const TString values = DecodeDenseValues(TStringBuf(raw.data() + prefix.Position, raw.size() - prefix.Position), prefix, recordsCount, width);
    auto valuesBuf = CopyToBuffer(values.data(), values.size());
    auto data = arrow::ArrayData::Make(
        indexType, recordsCount, { prefix.NullBitmap, valuesBuf }, prefix.NullBitmap ? arrow::kUnknownNullCount : 0);
    return arrow::MakeArray(data);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
