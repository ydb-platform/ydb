#include "writer.h"
#include "detail.h"
#include "format.h"

#include <library/cpp/yt/coding/varint.h>

#include <util/charset/utf8.h>

#include <util/stream/buffer.h>
#include <util/system/unaligned_mem.h>

#include <cmath>

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

size_t FloatToStringWithNanInf(double value, char* buf, size_t size)
{
    if (std::isfinite(value)) {
        return FloatToString(value, buf, size);
    }

    static const TStringBuf nanLiteral = "%nan";
    static const TStringBuf infLiteral = "%inf";
    static const TStringBuf negativeInfLiteral = "%-inf";

    TStringBuf str;
    if (std::isnan(value)) {
        str = nanLiteral;
    } else if (std::isinf(value) && value > 0) {
        str = infLiteral;
    } else {
        str = negativeInfLiteral;
    }
    YT_VERIFY(str.size() + 1 <= size);
    ::memcpy(buf, str.data(), str.size() + 1);
    return str.size();
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
        Stream_->Write(::ToString(value));
    }
    EndNode();
}

void TYsonWriter::OnUint64Scalar(ui64 value)
{
    if (Format_ == EYsonFormat::Binary) {
        Stream_->Write(NDetail::Uint64Marker);
        WriteVarUint64(Stream_, value);
    } else {
        Stream_->Write(::ToString(value));
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
        auto str = TStringBuf(buf, FloatToStringWithNanInf(value, buf, sizeof(buf)));
        Stream_->Write(str);
        if (str.find('.') == TString::npos && str.find('e') == TString::npos && std::isfinite(value)) {
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
        THROW_ERROR_EXCEPTION("Depth limit exceeded while writing YSON")
            << TErrorAttribute("limit", NestingLevelLimit_);
    }
}

Y_FORCE_INLINE void TBufferedBinaryYsonWriter::EndNode()
{
    if (Y_LIKELY(Type_ != EYsonType::Node || Depth_ > 0)) {
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
