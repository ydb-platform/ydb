#include "writer.h"
#include "detail.h"
#include "format.h"

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/error/text_yson.h>

#include <util/charset/utf8.h>

#include <util/stream/buffer.h>
#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <utility>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// Copied from <util/string/escape.cpp>
namespace {

static inline char HexDigit(char value) {
    YT_ASSERT(value < 16);
    if (value < 10)
        return '0' + value;
    else
        return 'A' + value - 10;
}

static inline char OctDigit(char value) {
    YT_ASSERT(value < 8);
    return '0' + value;
}

static inline bool IsPrintable(char c) {
    return c >= 32 && c <= 126;
}

static inline bool IsHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
}

static inline bool IsOctDigit(char c) {
    return  c >= '0' && c <= '7';
}

static const size_t ESCAPE_C_BUFFER_SIZE = 4;
static constexpr size_t IntegerToStringBufferSize = 32;
static constexpr size_t DoubleToStringBufferSize = 32;
static constexpr size_t ShortStringMaxLength = 15;

struct TEscapeNeededTable
{
    std::array<bool, 256> Data;

    constexpr TEscapeNeededTable()
        : Data()
    {
        for (int value = 0; value < 256; ++value) {
            Data[value] = value == '"' || value == '\\' || !(value >= 32 && value <= 126);
        }
    }
};

constexpr TEscapeNeededTable EscapeNeededTable;

static inline size_t EscapeC(unsigned char c, char next, char r[ESCAPE_C_BUFFER_SIZE]) {
    // (1) Printable characters go as-is, except backslash and double quote.
    // (2) Characters \r, \n, \t and \0 ... \7 replaced by their simple escape characters (if possible).
    // (3) Otherwise, character is encoded using hexadecimal escape sequence (if possible), or octal.
    if (c == '\"') {
        r[0] = '\\';
        r[1] = '\"';
        return 2;
    } else if (c == '\\') {
        r[0] = '\\';
        r[1] = '\\';
        return 2;
    } else if (IsPrintable(c)) {
        r[0] = c;
        return 1;
    } else if (c == '\r') {
        r[0] = '\\';
        r[1] = 'r';
        return 2;
    } else if (c == '\n') {
        r[0] = '\\';
        r[1] = 'n';
        return 2;
    } else if (c == '\t') {
        r[0] = '\\';
        r[1] = 't';
        return 2;
    } else if (c < 8 && !IsOctDigit(next)) {
        r[0] = '\\';
        r[1] = OctDigit(c);
        return 2;
    } else if (!IsHexDigit(next)) {
        r[0] = '\\';
        r[1] = 'x';
        r[2] = HexDigit((c & 0xF0) >> 4);
        r[3] = HexDigit((c & 0x0F) >> 0);
        return 4;
    } else {
        r[0] = '\\';
        r[1] = OctDigit((c & 0700) >> 6);
        r[2] = OctDigit((c & 0070) >> 3);
        r[3] = OctDigit((c & 0007) >> 0);
        return 4;
    }
}

void EscapeC(const char* str, size_t len, IOutputStream& output) {
    char buffer[ESCAPE_C_BUFFER_SIZE];

    size_t i, j;
    for (i = 0, j = 0; i < len; ++i) {
        size_t rlen = EscapeC(str[i], (i + 1 < len ? str[i + 1] : 0), buffer);

        if (rlen > 1) {
            output.Write(str + j, i - j);
            j = i + 1;
            output.Write(buffer, rlen);
        }
    }

    if (j > 0) {
        output.Write(str + j, len - j);
    } else {
        output.Write(str, len);
    }
}

void WriteUtf8String(const char* str, size_t len, IOutputStream& output)
{
    char buffer[ESCAPE_C_BUFFER_SIZE];
    const auto* ustr = reinterpret_cast<const unsigned char*>(str);
    for (size_t i = 0; i < len;) {
        size_t runeLen;
        YT_VERIFY(RECODE_OK == GetUTF8CharLen(runeLen, ustr + i, ustr + len));
        if (runeLen > 1) {
            output.Write(ustr + i, runeLen);
            i += runeLen;
        } else {
            YT_ASSERT(1 == runeLen);
            // `EscapeC` must be called for case of non-ascii characters like `\t` and `\n`.
            runeLen = EscapeC(ustr[i], (i + 1 < len ? ustr[i + 1] : 0), buffer);
            output.Write(buffer, runeLen);
            ++i;
        }
    }
}

Y_FORCE_INLINE char GetNextCharOrZero(const char* str, size_t len, size_t index)
{
    return index + 1 < len ? str[index + 1] : 0;
}

Y_NO_INLINE size_t EscapeShortStringToBuffer(char* buffer, TStringBuf value, size_t escapedPosition)
{
    char* current = buffer + escapedPosition + 1;
    for (size_t index = escapedPosition; index < value.length(); ++index) {
        current += EscapeC(
            static_cast<unsigned char>(value[index]),
            GetNextCharOrZero(value.data(), value.length(), index),
            current);
    }
    *current++ = '"';
    return current - buffer;
}

template <class TOutput>
void BufferedEscapeC(const char* str, size_t len, TOutput& output)
{
    size_t literalBegin = 0;
    for (size_t index = 0; index < len; ++index) {
        if (str[index] != '"' && str[index] != '\\' && IsPrintable(str[index])) {
            continue;
        }

        output.Write(str + literalBegin, index - literalBegin);
        literalBegin = index + 1;

        auto* buffer = output.GetDirectWriteBuffer();
        output.AdvanceDirectWrite(EscapeC(str[index], GetNextCharOrZero(str, len, index), buffer));
    }

    output.Write(str + literalBegin, len - literalBegin);
}

template <class TOutput>
void BufferedWriteUtf8String(const char* str, size_t len, TOutput& output)
{
    const auto* ustr = reinterpret_cast<const unsigned char*>(str);
    for (size_t i = 0; i < len;) {
        size_t runeLen;
        YT_VERIFY(RECODE_OK == GetUTF8CharLen(runeLen, ustr + i, ustr + len));
        if (runeLen > 1) {
            output.Write(ustr + i, runeLen);
            i += runeLen;
        } else {
            YT_ASSERT(1 == runeLen);
            // #EscapeC() handles single-byte characters, including control ones like `\t` and `\n`.
            auto* buffer = output.GetDirectWriteBuffer();
            output.AdvanceDirectWrite(EscapeC(ustr[i], GetNextCharOrZero(str, len, i), buffer));
            ++i;
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonWriter::TYsonWriter(
    IOutputStream* stream,
    EYsonFormat format,
    EYsonType type,
    bool enableRaw,
    int indent,
    bool passThroughUtf8Characters)
    : Stream_(stream)
    , Format_(format)
    , Type_(type)
    , EnableRaw_(enableRaw)
    , IndentSize_(indent)
    , PassThroughUtf8Characters_(passThroughUtf8Characters)
{
    YT_ASSERT(Stream_);
}

void TYsonWriter::WriteIndent()
{
    for (int i = 0; i < IndentSize_ * Depth_; ++i) {
        Stream_->Write(' ');
    }
}

void TYsonWriter::EndNode()
{
    if (Depth_ > 0 || Type_ != EYsonType::Node) {
        Stream_->Write(NDetail::ItemSeparatorSymbol);
        if ((Depth_ > 0 && Format_ == EYsonFormat::Pretty) ||
            (Depth_ == 0 && Format_ != EYsonFormat::Binary))
        {
            Stream_->Write('\n');
        }
    }
}

void TYsonWriter::BeginCollection(char ch)
{
    ++Depth_;
    EmptyCollection_ = true;
    Stream_->Write(ch);
}

void TYsonWriter::CollectionItem()
{
    if (Format_ == EYsonFormat::Pretty) {
        if (EmptyCollection_ && Depth_ > 0) {
            Stream_->Write('\n');
        }
        WriteIndent();
    }
    EmptyCollection_ = false;
}

void TYsonWriter::EndCollection(char ch)
{
    --Depth_;
    if (Format_ == EYsonFormat::Pretty && !EmptyCollection_) {
        WriteIndent();
    }
    EmptyCollection_ = false;
    Stream_->Write(ch);
}

void TYsonWriter::WriteStringScalar(TStringBuf value)
{
    if (Format_ == EYsonFormat::Binary) {
        Stream_->Write(NDetail::StringMarker);
        WriteVarInt32(Stream_, static_cast<i32>(value.length()));
        Stream_->Write(value.begin(), value.length());
    } else {
        Stream_->Write('"');
        if (PassThroughUtf8Characters_ && IsUtf(value)) {
            WriteUtf8String(value.data(), value.length(), *Stream_);
        } else {
            EscapeC(value.data(), value.length(), *Stream_);
        }
        Stream_->Write('"');
    }
}

void TYsonWriter::OnStringScalar(TStringBuf value)
{
    WriteStringScalar(value);
    EndNode();
}

void TYsonWriter::OnInt64Scalar(i64 value)
{
    if (Format_ == EYsonFormat::Binary) {
        Stream_->Write(NDetail::Int64Marker);
        WriteVarInt64(Stream_, value);
    } else {
        char buffer[32];
        Stream_->Write(buffer, ::ToString(value, buffer, sizeof(buffer)));
    }
    EndNode();
}

void TYsonWriter::OnUint64Scalar(ui64 value)
{
    if (Format_ == EYsonFormat::Binary) {
        Stream_->Write(NDetail::Uint64Marker);
        WriteVarUint64(Stream_, value);
    } else {
        char buffer[32];
        Stream_->Write(buffer, ::ToString(value, buffer, sizeof(buffer)));
        Stream_->Write("u");
    }
    EndNode();
}

void TYsonWriter::OnDoubleScalar(double value)
{
    if (Format_ == EYsonFormat::Binary) {
        Stream_->Write(NDetail::DoubleMarker);
        Stream_->Write(&value, sizeof(double));
    } else {
        char buf[256];
        auto str = TStringBuf(buf, NYT::NDetail::FloatToStringWithNanInf(value, buf, sizeof(buf)));
        Stream_->Write(str);
        if (str.find('.') == std::string::npos && str.find('e') == std::string::npos && std::isfinite(value)) {
            Stream_->Write(".");
        }
    }
    EndNode();
}

void TYsonWriter::OnBooleanScalar(bool value)
{
    if (Format_ == EYsonFormat::Binary) {
        Stream_->Write(value ? NDetail::TrueMarker : NDetail::FalseMarker);
    } else {
        Stream_->Write(value ? TStringBuf("%true") : TStringBuf("%false"));
    }
    EndNode();
}

void TYsonWriter::OnEntity()
{
    Stream_->Write(NDetail::EntitySymbol);
    EndNode();
}

void TYsonWriter::OnBeginList()
{
    BeginCollection(NDetail::BeginListSymbol);
}

void TYsonWriter::OnListItem()
{
    CollectionItem();
}

void TYsonWriter::OnEndList()
{
    EndCollection(NDetail::EndListSymbol);
    EndNode();
}

void TYsonWriter::OnBeginMap()
{
    BeginCollection(NDetail::BeginMapSymbol);
}

void TYsonWriter::OnKeyedItem(TStringBuf key)
{
    CollectionItem();

    WriteStringScalar(key);

    if (Format_ == EYsonFormat::Pretty) {
        Stream_->Write(' ');
    }
    Stream_->Write(NDetail::KeyValueSeparatorSymbol);
    if (Format_ == EYsonFormat::Pretty) {
        Stream_->Write(' ');
    }
}

void TYsonWriter::OnEndMap()
{
    EndCollection(NDetail::EndMapSymbol);
    EndNode();
}

void TYsonWriter::OnBeginAttributes()
{
    BeginCollection(NDetail::BeginAttributesSymbol);
}

void TYsonWriter::OnEndAttributes()
{
    EndCollection(NDetail::EndAttributesSymbol);
    if (Format_ == EYsonFormat::Pretty) {
        Stream_->Write(' ');
    }
}

void TYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    if (EnableRaw_) {
        Stream_->Write(yson);
        if (type == EYsonType::Node) {
            EndNode();
        }
    } else {
        TYsonConsumerBase::OnRaw(yson, type);
    }
}

void TYsonWriter::Flush()
{ }

int TYsonWriter::GetDepth() const
{
    return Depth_;
}

////////////////////////////////////////////////////////////////////////////////

TBufferedYsonWriter::TInlineBuffer::TInlineBuffer(IOutputStream* underlyingStream)
    : UnderlyingStream_(underlyingStream)
{
    YT_ASSERT(UnderlyingStream_);
}

Y_FORCE_INLINE void TBufferedYsonWriter::TInlineBuffer::Write(const void* data, size_t length)
{
    if (length == 0) [[unlikely]] {
        return;
    }

    if (length < FlushThreshold - Size_) [[likely]] {
        std::memcpy(GetDirectWriteBuffer(), data, length);
        Size_ += length;
    } else {
        WriteSlow(data, length);
    }
}

Y_FORCE_INLINE void TBufferedYsonWriter::TInlineBuffer::Write(TStringBuf value)
{
    Write(value.data(), value.length());
}

Y_FORCE_INLINE void TBufferedYsonWriter::TInlineBuffer::Write(char value)
{
    *GetDirectWriteBuffer() = value;
    if (++Size_ == FlushThreshold) [[unlikely]] {
        Flush();
    }
}

Y_FORCE_INLINE char* TBufferedYsonWriter::TInlineBuffer::GetDirectWriteBuffer()
{
    return Buffer_ + Size_;
}

Y_FORCE_INLINE void TBufferedYsonWriter::TInlineBuffer::AdvanceDirectWrite(size_t length)
{
    YT_ASSERT(Size_ < FlushThreshold);
    YT_ASSERT(length <= MaxDirectWriteSize);
    YT_ASSERT(length <= BufferCapacity - Size_);
    Size_ += length;
    if (Size_ >= FlushThreshold) [[unlikely]] {
        Flush();
    }
}

Y_FORCE_INLINE void TBufferedYsonWriter::TInlineBuffer::WriteRepeated(char value, size_t length)
{
    while (length != 0) {
        size_t chunkLength = std::min(length, MaxDirectWriteSize);
        std::memset(GetDirectWriteBuffer(), value, chunkLength);
        AdvanceDirectWrite(chunkLength);
        length -= chunkLength;
    }
}

void TBufferedYsonWriter::TInlineBuffer::WriteSlow(const void* data, size_t length)
{
    if (length >= FlushThreshold) {
        Flush();
        UnderlyingStream_->Write(data, length);
    } else {
        YT_ASSERT(length <= MaxDirectWriteSize);
        std::memcpy(GetDirectWriteBuffer(), data, length);
        AdvanceDirectWrite(length);
    }
}

Y_FORCE_INLINE void TBufferedYsonWriter::TInlineBuffer::Flush()
{
    if (Size_ != 0) {
        size_t size = std::exchange(Size_, 0);
        UnderlyingStream_->Write(Buffer_, size);
    }
}

////////////////////////////////////////////////////////////////////////////////

TBufferedYsonWriter::TBufferedYsonWriter(
    IOutputStream* stream,
    EYsonFormat format,
    EYsonType type,
    bool enableRaw,
    int indent,
    bool passThroughUtf8Characters)
    : Format_(format)
    , Type_(type)
    , EnableRaw_(enableRaw)
    , IndentSize_(std::max(indent, 0))
    , PassThroughUtf8Characters_(passThroughUtf8Characters)
    , Buffer_(stream)
{ }

TBufferedYsonWriter::~TBufferedYsonWriter()
{
    try {
        Buffer_.Flush();
    } catch (...) {
    }
}

void TBufferedYsonWriter::WriteIndent()
{
    if (Depth_ <= 0) {
        return;
    }
    Buffer_.WriteRepeated(' ', static_cast<size_t>(IndentSize_) * Depth_);
}

Y_FORCE_INLINE void TBufferedYsonWriter::EndNode()
{
    if (Depth_ == 0) {
        if (Type_ != EYsonType::Node) {
            Buffer_.Write(NDetail::ItemSeparatorSymbol);
            if (Format_ != EYsonFormat::Binary) {
                Buffer_.Write('\n');
            }
        }
    } else {
        Buffer_.Write(NDetail::ItemSeparatorSymbol);
        if (Format_ == EYsonFormat::Pretty) {
            Buffer_.Write('\n');
        }
    }
}

void TBufferedYsonWriter::BeginCollection(char ch)
{
    ++Depth_;
    EmptyPrettyCollection_ = true;
    Buffer_.Write(ch);
}

void TBufferedYsonWriter::CollectionItem()
{
    if (Format_ == EYsonFormat::Pretty) {
        if (EmptyPrettyCollection_ && Depth_ > 0) {
            Buffer_.Write('\n');
        }
        WriteIndent();
    }
    EmptyPrettyCollection_ = false;
}

void TBufferedYsonWriter::EndCollection(char ch)
{
    --Depth_;
    if (Format_ == EYsonFormat::Pretty && !EmptyPrettyCollection_) {
        WriteIndent();
    }
    EmptyPrettyCollection_ = false;
    Buffer_.Write(ch);
}

void TBufferedYsonWriter::WriteStringScalar(TStringBuf value)
{
    if (Format_ == EYsonFormat::Binary) {
        static_assert(MaxDirectWriteSize >= 1 + MaxVarInt32Size);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        buffer[0] = NDetail::StringMarker;
        size_t length = 1 + WriteVarInt32(buffer + 1, static_cast<i32>(value.length()));
        Buffer_.AdvanceDirectWrite(length);
        Buffer_.Write(value.begin(), value.length());
        return;
    }

    if (!PassThroughUtf8Characters_ && value.length() <= ShortStringMaxLength) {
        static_assert(MaxDirectWriteSize >= 2 + ESCAPE_C_BUFFER_SIZE * ShortStringMaxLength);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        buffer[0] = '"';

        size_t index = 0;
        for (; index < value.length(); ++index) {
            if (EscapeNeededTable.Data[static_cast<ui8>(value[index])]) [[unlikely]] {
                break;
            }
            buffer[index + 1] = value[index];
        }

        if (index == value.length()) [[likely]] {
            buffer[value.length() + 1] = '"';
            Buffer_.AdvanceDirectWrite(value.length() + 2);
        } else {
            Buffer_.AdvanceDirectWrite(EscapeShortStringToBuffer(buffer, value, index));
        }
    } else {
        WriteTextStringScalarSlow(value);
    }
}

Y_NO_INLINE void TBufferedYsonWriter::WriteTextStringScalarSlow(TStringBuf value)
{
    Buffer_.Write('"');
    if (PassThroughUtf8Characters_ && IsUtf(value)) {
        BufferedWriteUtf8String(value.data(), value.length(), Buffer_);
    } else {
        BufferedEscapeC(value.data(), value.length(), Buffer_);
    }
    Buffer_.Write('"');
}

void TBufferedYsonWriter::OnStringScalar(TStringBuf value)
{
    WriteStringScalar(value);
    EndNode();
}

void TBufferedYsonWriter::OnInt64Scalar(i64 value)
{
    if (Format_ != EYsonFormat::Binary || Depth_ == 0) [[unlikely]] {
        DoOnInt64Scalar(value);
        return;
    }

    static_assert(MaxDirectWriteSize >= MaxVarInt64Size + 2);
    auto* buffer = Buffer_.GetDirectWriteBuffer();
    buffer[0] = NDetail::Int64Marker;
    size_t length = 1 + WriteVarInt64(buffer + 1, value);
    buffer[length++] = NDetail::ItemSeparatorSymbol;
    Buffer_.AdvanceDirectWrite(length);
}

Y_NO_INLINE void TBufferedYsonWriter::DoOnInt64Scalar(i64 value)
{
    if (Format_ == EYsonFormat::Binary) {
        static_assert(MaxDirectWriteSize >= MaxVarInt64Size);
        Buffer_.Write(NDetail::Int64Marker);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        Buffer_.AdvanceDirectWrite(WriteVarInt64(buffer, value));
    } else {
        static_assert(MaxDirectWriteSize >= IntegerToStringBufferSize);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        Buffer_.AdvanceDirectWrite(::ToString(value, buffer, IntegerToStringBufferSize));
    }
    EndNode();
}

void TBufferedYsonWriter::OnUint64Scalar(ui64 value)
{
    if (Format_ != EYsonFormat::Binary || Depth_ == 0) [[unlikely]] {
        DoOnUint64Scalar(value);
        return;
    }

    static_assert(MaxDirectWriteSize >= MaxVarUint64Size + 2);
    auto* buffer = Buffer_.GetDirectWriteBuffer();
    buffer[0] = NDetail::Uint64Marker;
    size_t length = 1 + WriteVarUint64(buffer + 1, value);
    buffer[length++] = NDetail::ItemSeparatorSymbol;
    Buffer_.AdvanceDirectWrite(length);
}

Y_NO_INLINE void TBufferedYsonWriter::DoOnUint64Scalar(ui64 value)
{
    if (Format_ == EYsonFormat::Binary) {
        static_assert(MaxDirectWriteSize >= MaxVarUint64Size);
        Buffer_.Write(NDetail::Uint64Marker);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        Buffer_.AdvanceDirectWrite(WriteVarUint64(buffer, value));
    } else {
        static_assert(MaxDirectWriteSize >= IntegerToStringBufferSize);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        size_t length = ::ToString(value, buffer, IntegerToStringBufferSize);
        buffer[length++] = 'u';
        Buffer_.AdvanceDirectWrite(length);
    }
    EndNode();
}

void TBufferedYsonWriter::OnDoubleScalar(double value)
{
    if (Format_ != EYsonFormat::Binary || Depth_ == 0) [[unlikely]] {
        DoOnDoubleScalar(value);
        return;
    }

    static_assert(MaxDirectWriteSize >= sizeof(double) + 2);
    auto* buffer = Buffer_.GetDirectWriteBuffer();
    buffer[0] = NDetail::DoubleMarker;
    std::memcpy(buffer + 1, &value, sizeof(value));
    buffer[sizeof(value) + 1] = NDetail::ItemSeparatorSymbol;
    Buffer_.AdvanceDirectWrite(sizeof(value) + 2);
}

Y_NO_INLINE void TBufferedYsonWriter::DoOnDoubleScalar(double value)
{
    if (Format_ == EYsonFormat::Binary) {
        Buffer_.Write(NDetail::DoubleMarker);
        Buffer_.Write(&value, sizeof(double));
    } else {
        static_assert(MaxDirectWriteSize >= DoubleToStringBufferSize);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        auto str = TStringBuf(
            buffer,
            NYT::NDetail::FloatToStringWithNanInf(value, buffer, DoubleToStringBufferSize));
        const bool addDecimalPoint =
            str.find('.') == std::string::npos &&
            str.find('e') == std::string::npos &&
            std::isfinite(value);
        Buffer_.AdvanceDirectWrite(str.length());
        if (addDecimalPoint) {
            Buffer_.Write(".");
        }
    }
    EndNode();
}

void TBufferedYsonWriter::OnBooleanScalar(bool value)
{
    if (Format_ != EYsonFormat::Binary || Depth_ == 0) [[unlikely]] {
        DoOnBooleanScalar(value);
        return;
    }

    static_assert(MaxDirectWriteSize >= 2);
    auto* buffer = Buffer_.GetDirectWriteBuffer();
    buffer[0] = value ? NDetail::TrueMarker : NDetail::FalseMarker;
    buffer[1] = NDetail::ItemSeparatorSymbol;
    Buffer_.AdvanceDirectWrite(2);
}

Y_NO_INLINE void TBufferedYsonWriter::DoOnBooleanScalar(bool value)
{
    if (Format_ == EYsonFormat::Binary) {
        Buffer_.Write(value ? NDetail::TrueMarker : NDetail::FalseMarker);
    } else {
        Buffer_.Write(value ? TStringBuf("%true") : TStringBuf("%false"));
    }
    EndNode();
}

void TBufferedYsonWriter::OnEntity()
{
    if (Format_ != EYsonFormat::Binary || Depth_ == 0) [[unlikely]] {
        DoOnEntity();
        return;
    }

    static_assert(MaxDirectWriteSize >= 2);
    auto* buffer = Buffer_.GetDirectWriteBuffer();
    buffer[0] = NDetail::EntitySymbol;
    buffer[1] = NDetail::ItemSeparatorSymbol;
    Buffer_.AdvanceDirectWrite(2);
}

Y_NO_INLINE void TBufferedYsonWriter::DoOnEntity()
{
    Buffer_.Write(NDetail::EntitySymbol);
    EndNode();
}

void TBufferedYsonWriter::OnBeginList()
{
    BeginCollection(NDetail::BeginListSymbol);
}

void TBufferedYsonWriter::OnListItem()
{
    if (Format_ != EYsonFormat::Binary || Depth_ == 0) [[unlikely]] {
        CollectionItem();
    }
}

void TBufferedYsonWriter::OnEndList()
{
    EndCollection(NDetail::EndListSymbol);
    EndNode();
}

void TBufferedYsonWriter::OnBeginMap()
{
    BeginCollection(NDetail::BeginMapSymbol);
}

void TBufferedYsonWriter::OnKeyedItem(TStringBuf key)
{
    CollectionItem();

    WriteStringScalar(key);

    if (Format_ == EYsonFormat::Pretty) {
        static_assert(MaxDirectWriteSize >= 3);
        auto* buffer = Buffer_.GetDirectWriteBuffer();
        buffer[0] = ' ';
        buffer[1] = NDetail::KeyValueSeparatorSymbol;
        buffer[2] = ' ';
        Buffer_.AdvanceDirectWrite(3);
    } else {
        Buffer_.Write(NDetail::KeyValueSeparatorSymbol);
    }
}

void TBufferedYsonWriter::OnEndMap()
{
    EndCollection(NDetail::EndMapSymbol);
    EndNode();
}

void TBufferedYsonWriter::OnBeginAttributes()
{
    BeginCollection(NDetail::BeginAttributesSymbol);
}

void TBufferedYsonWriter::OnEndAttributes()
{
    EndCollection(NDetail::EndAttributesSymbol);
    if (Format_ == EYsonFormat::Pretty) {
        Buffer_.Write(' ');
    }
}

void TBufferedYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    if (!EnableRaw_) {
        TYsonConsumerBase::OnRaw(yson, type);
        return;
    }

    // A dedicated fragment path allows large writes to tail-call #WriteSlow().
    if (type != EYsonType::Node) {
        Buffer_.Write(yson);
        return;
    }

    Buffer_.Write(yson);
    EndNode();
}

void TBufferedYsonWriter::Flush()
{
    Buffer_.Flush();
}

int TBufferedYsonWriter::GetDepth() const
{
    return Depth_;
}

////////////////////////////////////////////////////////////////////////////////

TBufferedBinaryYsonWriter::TBufferedBinaryYsonWriter(
    IZeroCopyOutput* stream,
    EYsonType type,
    bool enableRaw,
    std::optional<int> nestingLevelLimit)
    : Type_(type)
    , EnableRaw_(enableRaw)
    , TokenWriter_(std::make_optional<TUncheckedYsonTokenWriter>(stream, type))
    , NestingLevelLimit_(nestingLevelLimit.value_or(std::numeric_limits<int>::max()))
{
    YT_ASSERT(stream);
}

Y_FORCE_INLINE void TBufferedBinaryYsonWriter::WriteStringScalar(TStringBuf value)
{
    TokenWriter_->WriteBinaryString(value);
}

Y_FORCE_INLINE void TBufferedBinaryYsonWriter::BeginCollection()
{
    ++Depth_;
    if (Depth_ > NestingLevelLimit_) {
        THROW_ERROR_EXCEPTION(EErrorCode::DepthLimitExceeded, "Depth limit exceeded while writing YSON")
            .With("limit", NestingLevelLimit_);
    }
}

Y_FORCE_INLINE void TBufferedBinaryYsonWriter::EndNode()
{
    if (Type_ != EYsonType::Node || Depth_ > 0) [[likely]] {
        TokenWriter_->WriteItemSeparator();
    }
}

void TBufferedBinaryYsonWriter::Flush()
{
    TokenWriter_->Flush();
}

int TBufferedBinaryYsonWriter::GetDepth() const
{
    return Depth_;
}

ui64 TBufferedBinaryYsonWriter::GetTotalWrittenSize() const
{
    return TokenWriter_ ? TokenWriter_->GetTotalWrittenSize() : 0;
}

void TBufferedBinaryYsonWriter::OnStringScalar(TStringBuf value)
{
    WriteStringScalar(value);
    EndNode();
}

void TBufferedBinaryYsonWriter::OnInt64Scalar(i64 value)
{
    TokenWriter_->WriteBinaryInt64(value);
    EndNode();
}

void TBufferedBinaryYsonWriter::OnUint64Scalar(ui64 value)
{
    TokenWriter_->WriteBinaryUint64(value);
    EndNode();
}

void TBufferedBinaryYsonWriter::OnDoubleScalar(double value)
{
    TokenWriter_->WriteBinaryDouble(value);
    EndNode();
}

void TBufferedBinaryYsonWriter::OnBooleanScalar(bool value)
{
    TokenWriter_->WriteBinaryBoolean(value);
    EndNode();
}

void TBufferedBinaryYsonWriter::OnEntity()
{
    TokenWriter_->WriteEntity();
    EndNode();
}

void TBufferedBinaryYsonWriter::OnBeginList()
{
    BeginCollection();
    TokenWriter_->WriteBeginList();
}

void TBufferedBinaryYsonWriter::OnListItem()
{ }

void TBufferedBinaryYsonWriter::OnEndList()
{
    --Depth_;
    TokenWriter_->WriteEndList();
    EndNode();
}

void TBufferedBinaryYsonWriter::OnBeginMap()
{
    BeginCollection();
    TokenWriter_->WriteBeginMap();
}

void TBufferedBinaryYsonWriter::OnKeyedItem(TStringBuf key)
{
    WriteStringScalar(key);
    TokenWriter_->WriteKeyValueSeparator();
}

void TBufferedBinaryYsonWriter::OnEndMap()
{
    --Depth_;
    TokenWriter_->WriteEndMap();
    EndNode();
}

void TBufferedBinaryYsonWriter::OnBeginAttributes()
{
    BeginCollection();
    TokenWriter_->WriteBeginAttributes();
}

void TBufferedBinaryYsonWriter::OnEndAttributes()
{
    --Depth_;
    TokenWriter_->WriteEndAttributes();
}

void TBufferedBinaryYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    if (EnableRaw_) {
        TokenWriter_->WriteRawNodeUnchecked(yson);
        if (type == EYsonType::Node) {
            EndNode();
        }
    } else {
        TYsonConsumerBase::OnRaw(yson, type);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFlushableYsonConsumer> CreateYsonWriter(
    IZeroCopyOutput* output,
    EYsonFormat format,
    EYsonType type,
    bool enableRaw,
    int indent,
    bool passThroughUtf8Characters)
{
    if (format == EYsonFormat::Binary) {
        return std::make_unique<TBufferedBinaryYsonWriter>(
            output,
            type,
            enableRaw);
    } else {
        return std::make_unique<TYsonWriter>(
            output,
            format,
            type,
            enableRaw,
            indent,
            passThroughUtf8Characters);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
