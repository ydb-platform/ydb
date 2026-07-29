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

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

// [uncompressed-size][payload], payload compressed with codec (or stored raw when codec is null).
void AppendFrameCompressed(TString& out, const TStringBuf payload, const std::shared_ptr<arrow::util::Codec>& codec) {
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
    TStatusValidator::GetValid(codec->Decompress(payload.size(), (const uint8_t*)payload.data(), rawSize, (uint8_t*)raw.Detach()));
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
        AFL_VERIFY(array.null_bitmap_data());
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
    const ui32 totalSize = out.size() - contentPosition;
    memcpy(out.Detach() + sizePosition, &totalSize, sizeof(totalSize));
}

TString ReadSection(const TStringBuf blob, size_t& pos, const std::shared_ptr<arrow::util::Codec>& codec) {
    AFL_VERIFY(blob.size() >= pos + sizeof(ui32))("size", blob.size())("pos", pos);
    ui32 totalSize;
    memcpy(&totalSize, blob.data() + pos, sizeof(totalSize));
    pos += sizeof(totalSize);
    AFL_VERIFY(blob.size() >= pos + totalSize)("size", blob.size())("need", pos + totalSize);
    const TString result = FrameDecompress(TStringBuf(blob.data() + pos, totalSize), codec);
    pos += totalSize;
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
    if (hasNulls) {
        const size_t bmBytes = (recordsCount + 7) / 8;
        AFL_VERIFY(raw.size() >= pos + bmBytes)("size", raw.size());
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

}   // namespace

TString EncodeLengths(TConstArrayRef<ui32> values) {
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
        lengths.emplace_back(view.size());
    }

    const TValidityBitmap validity(array);
    const TStringBuf validityData = validity.GetData();
    TString out;
    const char hasNulls = validityData.empty() ? 0 : 1;
    out.append(&hasNulls, 1);
    if (hasNulls) {
        AppendSection(out, validityData, codec);
    }
    AppendSection(out, EncodeLengths(lengths), codec);
    AppendSection(out, values, codec);
    return out;
}

std::shared_ptr<arrow::BinaryArray> DeserializeBinaryArray(
    TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::util::Codec>& codec) {
    size_t pos = 0;
    AFL_VERIFY(blob.size() >= 1);
    const char hasNulls = blob[pos++];

    std::shared_ptr<arrow::Buffer> nullBitmap;
    TStringBuf validity;
    TString validityHolder;
    ui32 presentCount = recordsCount;
    if (hasNulls) {
        validityHolder = ReadSection(blob, pos, codec);
        validity = validityHolder;
        nullBitmap = CopyToBuffer(validity.data(), validity.size());
        presentCount = CountSetBits(validity, recordsCount);
    }
    const TString lengthsRaw = ReadSection(blob, pos, codec);
    const TVector<ui32> lengths = DecodeLengths(lengthsRaw, presentCount);
    const TString values = ReadSection(blob, pos, codec);
    AFL_VERIFY(pos == blob.size())("pos", pos)("size", blob.size());

    TString denseOffsets;
    denseOffsets.ReserveAndResize(sizeof(int32_t) * (recordsCount + 1));
    int32_t* op = (int32_t*)denseOffsets.Detach();
    op[0] = 0;
    ui32 present = 0;
    for (ui32 i = 0; i < recordsCount; ++i) {
        if (!nullBitmap || GetBit(validity, i)) {
            op[i + 1] = op[i] + lengths[present++];
        } else {
            op[i + 1] = op[i];
        }
    }
    AFL_VERIFY(present == lengths.size())("present", present)("lengths", lengths.size());
    AFL_VERIFY(static_cast<size_t>(op[recordsCount]) == values.size())("offsets_back", op[recordsCount])("values_len", values.size());

    auto offsetsBuf = CopyToBuffer(denseOffsets.data(), denseOffsets.size());
    auto valuesBuf = CopyToBuffer(values.data(), values.size());
    auto data = arrow::ArrayData::Make(
        arrow::binary(), recordsCount, { nullBitmap, offsetsBuf, valuesBuf }, nullBitmap ? arrow::kUnknownNullCount : 0);
    return std::static_pointer_cast<arrow::BinaryArray>(arrow::MakeArray(data));
}

TString SerializeIndices(const std::shared_ptr<arrow::Array>& positions, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    const auto casted = TStatusValidator::GetValid(arrow::compute::Cast(arrow::Datum(positions), indexType)).make_array();
    const ui32 width = GetIndexByteWidth(*indexType);
    const auto& data = *casted->data();
    const ui8* values = data.buffers[1]->data() + data.offset * width;
    const i64 n = casted->length();

    const TValidityBitmap validity(*casted);
    const TStringBuf validityData = validity.GetData();
    const i64 encodedCount = n - casted->null_count();
    TString payload;
    payload.reserve(1 + validityData.size() + width * encodedCount);
    const char hasNulls = validityData.empty() ? 0 : 1;
    payload.append(&hasNulls, 1);
    if (hasNulls) {
        payload.append(validityData);
    }
    for (i64 i = 0; i < n; ++i) {
        if (!casted->IsNull(i)) {
            payload.append((const char*)(values + i * width), width);
        }
    }
    return FrameCompress(payload, codec);
}

std::shared_ptr<arrow::Array> DeserializeIndices(TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec) {
    const TString raw = FrameDecompress(blob, codec);
    const ui32 width = GetIndexByteWidth(*indexType);
    const auto prefix = ParsePrefix(raw, recordsCount);
    const ui32 encodedCount = prefix.PresentCount;
    AFL_VERIFY(raw.size() == prefix.Position + (size_t)width * encodedCount)("size", raw.size())("count", encodedCount);

    TString dense;
    dense.ReserveAndResize((size_t)width * recordsCount);
    char* out = dense.Detach();
    const char* encoded = raw.data() + prefix.Position;
    ui32 k = 0;
    for (ui32 i = 0; i < recordsCount; ++i) {
        if (prefix.IsValid(i)) {
            memcpy(out + (size_t)i * width, encoded + (size_t)(k++) * width, width);
        } else {
            memset(out + (size_t)i * width, 0, width);
        }
    }
    auto valuesBuf = CopyToBuffer(dense.data(), dense.size());
    auto data = arrow::ArrayData::Make(
        indexType, recordsCount, { prefix.NullBitmap, valuesBuf }, prefix.NullBitmap ? arrow::kUnknownNullCount : 0);
    return arrow::MakeArray(data);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
