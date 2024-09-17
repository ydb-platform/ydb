#pragma once

#include "public.h"
#include "token.h"

#include <yt/yt/core/concurrency/coroutine.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/parser_helpers.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/static_ring_queue.h>

#include <library/cpp/yt/coding/varint.h>
#include <library/cpp/yt/coding/zig_zag.h>

#include <library/cpp/yt/yson_string/format.h>

#include <util/generic/string.h>

#include <util/string/escape.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxLiteralLengthInError = 100;

inline TError CreateLiteralError(ETokenType tokenType, const char* bufferStart, size_t bufferSize)
{
    if (bufferSize < MaxLiteralLengthInError) {
        return TError("Failed to parse %v literal %Qv",
            tokenType,
            TStringBuf(bufferStart, bufferSize));
    } else {
        return TError("Failed to parse %v literal \"%v...<literal truncated>\"",
            tokenType,
            TStringBuf(bufferStart, std::min<size_t>(bufferSize, MaxLiteralLengthInError)));
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

//! Indicates end of stream.
constexpr char EndSymbol = '\0';

template <bool EnableLinePositionInfo>
class TPositionInfo;

template <>
class TPositionInfo<true>
{
private:
    i64 Offset = 0;
    int Line = 1;
    int Column = 1;

public:
    void OnRangeConsumed(const char* begin, const char* end)
    {
        Offset += end - begin;
        for (auto current = begin; current != end; ++current) {
            ++Column;
            if (*current == '\n') { //TODO: memchr
                ++Line;
                Column = 1;
            }
        }
    }

    std::vector<TErrorAttribute> GetErrorAttributes(const char* begin, const char* current) const
    {
        auto other = *this;
        other.OnRangeConsumed(begin, current);
        return {
            TErrorAttribute("offset", other.Offset),
            TErrorAttribute("line", other.Line),
            TErrorAttribute("column", other.Column),
        };
    }
};

template <>
class TPositionInfo<false>
{
private:
    i64 Offset = 0;

public:
    void OnRangeConsumed(const char* begin, const char* end)
    {
        Offset += end - begin;
    }

    std::vector<TErrorAttribute> GetErrorAttributes(const char* begin, const char* current) const
    {
        auto other = *this;
        other.OnRangeConsumed(begin, current);
        return {
            TErrorAttribute("offset", other.Offset),
        };
    }
};

static constexpr size_t MaxMarginSize = 10;

template <class TBlockStream, size_t MaxContextSize>
class TReaderWithContext
    : public TBlockStream
{
private:
    static_assert(MaxContextSize > MaxMarginSize, "MaxContextSize must be greater than MaxMarginSize.");

    // Checkpoint points to the position that was current when CheckpointContext was called.
    // If it is nullptr that means that CheckpointContext was never called or it was called inside previous block.
    const char* Checkpoint = nullptr;

    // Context keeps context if CheckpointContext was called inside one of the previous blocks.
    char Context[MaxContextSize];
    size_t ContextSize = 0;
    size_t ContextPosition = 0;

    // PrevBlockTail keeps characters from previous blocks.
    // We save MaxMarginSize characters from the end of the current block when it ends.
    // It will allow us to keep a left margin of the current context even if it starts at the beginning of the block.
    TStaticRingQueue<char, MaxMarginSize> PrevBlockTail;

public:
    TReaderWithContext(const TBlockStream& blockStream)
        : TBlockStream(blockStream)
    { }

    void CheckpointContext()
    {
        Checkpoint = TBlockStream::Current();
    }

    // Return pair <context, context_position>.
    std::pair<TString, size_t> GetContextFromCheckpoint() const
    {
        TString result(MaxContextSize, '\0');
        size_t size, position;
        SaveContext(result.Detach(), &size, &position);
        result.resize(size);
        return {result, position};
    }

    void RefreshBlock()
    {
        SaveContext(Context, &ContextSize, &ContextPosition);
        PrevBlockTail.Append(TBlockStream::Begin(), TBlockStream::End());
        TBlockStream::RefreshBlock();
        Checkpoint = nullptr;
    }

private:
    // dest must be at least of MaxContextSize capacity.
    void SaveContext(char* dest, size_t* contextSize, size_t* contextPosition) const
    {
        char* current = dest;
        if (Checkpoint != nullptr) {
            // Context inside current block.
            const size_t sizeFromBlock = std::min<size_t>(Checkpoint - TBlockStream::Begin(), MaxMarginSize);
            if (sizeFromBlock < MaxMarginSize) {
                const size_t sizeFromPrevBlock = std::min(MaxMarginSize - sizeFromBlock, PrevBlockTail.Size());
                PrevBlockTail.CopyTailTo(sizeFromPrevBlock, current);
                current += sizeFromPrevBlock;
            }
            memcpy(current, Checkpoint - sizeFromBlock, sizeFromBlock);
            current += sizeFromBlock;
            *contextPosition = current - dest;
            const size_t sizeAfterCheckpoint = std::min<size_t>(
                MaxContextSize - (current - dest),
                TBlockStream::End() - Checkpoint);
            memcpy(current, Checkpoint, sizeAfterCheckpoint);
            current += sizeAfterCheckpoint;
        } else if (ContextSize > 0) {
            // Context is inside one of the previous blocks.
            *contextPosition = ContextPosition;
            if (current != Context) {
                memcpy(current, Context, ContextSize);
            }
            current += ContextSize;
            if (ContextSize < MaxContextSize) {
                const auto toCopy = std::min<size_t>(MaxContextSize - ContextSize, TBlockStream::End() - TBlockStream::Begin());
                if (toCopy > 0) {
                    memcpy(current, TBlockStream::Begin(), toCopy);
                    current += toCopy;
                }
            }
        } else {
            // Context is the beginning of the stream.
            const auto toCopy = std::min<size_t>(MaxContextSize, TBlockStream::End() - TBlockStream::Begin());
            if (toCopy > 0) {
                memcpy(current, TBlockStream::Begin(), toCopy);
                current += toCopy;
            }
            *contextPosition = 0;
        }
        *contextSize = current - dest;
    }
};

template <class TBlockStream>
class TReaderWithContext<TBlockStream, 0>
    : public TBlockStream
{
public:
    TReaderWithContext(const TBlockStream& blockStream)
        : TBlockStream(blockStream)
    { }

    void CheckpointContext()
    { }

    std::pair<TString, size_t> GetContextFromCheckpoint() const
    {
        return {"<context is disabled>", 0};
    }
};

template <class TBlockStream, class TPositionBase>
class TCharStream
    : public TBlockStream
    , public TPositionBase
{
public:
    TCharStream(const TBlockStream& blockStream)
        : TBlockStream(blockStream)
    { }

    bool IsEmpty() const
    {
        return TBlockStream::Current() == TBlockStream::End();
    }

    template <bool AllowFinish>
    void Refresh()
    {
        while (IsEmpty() && !TBlockStream::IsFinished()) {
            TPositionBase::OnRangeConsumed(TBlockStream::Begin(), TBlockStream::Current());
            TBlockStream::RefreshBlock();
        }
        if (IsEmpty() && TBlockStream::IsFinished() && !AllowFinish) {
            THROW_ERROR_EXCEPTION("Premature end of stream")
                << *this;
        }
    }

    void Refresh()
    {
        return Refresh<false>();
    }

    template <bool AllowFinish>
    char GetChar()
    {
        if (!IsEmpty()) {
            return *TBlockStream::Current();
        }
        Refresh<AllowFinish>();
        return !IsEmpty() ? *TBlockStream::Current() : '\0';
    }

    char GetChar()
    {
        return GetChar<false>();
    }

    void Advance(size_t bytes)
    {
        TBlockStream::Advance(bytes);
    }

    size_t Length() const
    {
        return TBlockStream::End() - TBlockStream::Current();
    }

    std::vector<TErrorAttribute> GetErrorAttributes() const
    {
        return TPositionBase::GetErrorAttributes(TBlockStream::Begin(), TBlockStream::Current());
    }

    friend TError operator << (const TError& error, const TCharStream<TBlockStream, TPositionBase>& stream)
    {
        return error << stream.GetErrorAttributes();
    }
};

template <class TBaseStream>
class TCodedStream
    : public TBaseStream
{
private:
    static constexpr int MaxVarintBytes = 10;
    static constexpr int MaxVarint32Bytes = 5;

    const ui8* BeginByte() const
    {
        return reinterpret_cast<const ui8*>(TBaseStream::Current());
    }

    const ui8* EndByte() const
    {
        return reinterpret_cast<const ui8*>(TBaseStream::End());
    }

    [[noreturn]] void ThrowCannotParseVarint()
    {
        THROW_ERROR_EXCEPTION("Error parsing varint value")
            << *this;
    }

    // Following functions is an adaptation Protobuf code from coded_stream.cc
    ui32 ReadVarint32FromArray()
    {
        // Fast path:  We have enough bytes left in the buffer to guarantee that
        // this read won't cross the end, so we can skip the checks.
        const ui8* ptr = BeginByte();
        ui32 b;
        ui32 result;

        b = *(ptr++); result  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |=  b         << 28; if (!(b & 0x80)) goto done;

        // If the input is larger than 32 bits, we still need to read it all
        // and discard the high-order bits.

        for (int i = 0; i < MaxVarintBytes - MaxVarint32Bytes; i++) {
            b = *(ptr++); if (!(b & 0x80)) goto done;
        }

        // We have overrun the maximum size of a Varint (10 bytes).  Assume
        // the data is corrupt.
        ThrowCannotParseVarint();

    done:
        TBaseStream::Advance(ptr - BeginByte());
        return result;
    }

    ui32 ReadVarint32Fallback()
    {
        if (BeginByte() + MaxVarintBytes <= EndByte() ||
            // Optimization:  If the Varint ends at exactly the end of the buffer,
            // we can detect that and still use the fast path.
            (BeginByte() < EndByte() && !(EndByte()[-1] & 0x80)))
        {
            return ReadVarint32FromArray();
        } else {
            // Really slow case: we will incur the cost of an extra function call here,
            // but moving this out of line reduces the size of this function, which
            // improves the common case. In micro benchmarks, this is worth about 10-15%
            return ReadVarint32Slow();
        }
    }

    ui32 ReadVarint32Slow()
    {
        ui32 result = ReadVarint64();
        return static_cast<ui32>(result);
    }

    ui64 ReadVarint64Slow()
    {
        // Slow path:  This read might cross the end of the buffer, so we
        // need to check and refresh the buffer if and when it does.

        ui64 result = 0;
        int count = 0;
        ui32 b;

        do {
            if (count == MaxVarintBytes) {
                ThrowCannotParseVarint();
            }
            while (BeginByte() == EndByte()) {
                TBaseStream::Refresh();
            }
            b = *BeginByte();
            result |= static_cast<ui64>(b & 0x7F) << (7 * count);
            TBaseStream::Advance(1);
            ++count;
        } while (b & 0x80);

        return result;
    }

public:
    TCodedStream(const TBaseStream& baseStream)
        : TBaseStream(baseStream)
    { }

    Y_FORCE_INLINE int ReadVarint64ToArray(char* out)
    {
        if (BeginByte() + MaxVarintBytes <= EndByte() ||
            // Optimization:  If the Varint ends at exactly the end of the buffer,
            // we can detect that and still use the fast path.
            (BeginByte() < EndByte() && !(EndByte()[-1] & 0x80)))
        {
            // Fast path:  We have enough bytes left in the buffer to guarantee that
            // this read won't cross the end, so we can skip the checks.

            const ui8* ptr = BeginByte();
            for (int i = 0; i < MaxVarintBytes; ++i) {
                *out++ = *ptr;
                if (!(*ptr & 0x80U)) {
                    TBaseStream::Advance(i + 1);
                    return i + 1;
                }
                ++ptr;
            }

            // We have overrun the maximum size of a Varint (10 bytes).  The data
            // must be corrupt.
            ThrowCannotParseVarint();
        } else {
            return WriteVarUint64(out, ReadVarint64Slow());
        }
    }

    Y_FORCE_INLINE ui64 ReadVarint64()
    {
        if (BeginByte() + MaxVarintBytes <= EndByte() ||
            // Optimization:  If the Varint ends at exactly the end of the buffer,
            // we can detect that and still use the fast path.
            (BeginByte() < EndByte() && !(EndByte()[-1] & 0x80)))
        {
            // Fast path:  We have enough bytes left in the buffer to guarantee that
            // this read won't cross the end, so we can skip the checks.

            const ui8* ptr = BeginByte();
            ui64 b;
            ui64 result = 0;

            b = *(ptr++); result  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 28; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 35; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 42; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 49; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 56; if (!(b & 0x80)) goto done;
            b = *(ptr++); result |= (b & 0x7F) << 63; if (!(b & 0x80)) goto done;

            // We have overrun the maximum size of a Varint (10 bytes).  The data
            // must be corrupt.
            ThrowCannotParseVarint();

        done:
            TBaseStream::Advance(ptr - BeginByte());
            return result;
        } else {
            return ReadVarint64Slow();
        }
    }

    ui32 ReadVarint32()
    {
        if (BeginByte() < EndByte() && *BeginByte() < 0x80) {
            ui32 result = *BeginByte();
            TBaseStream::Advance(1);
            return result;
        } else {
            return ReadVarint32Fallback();
        }
    }
};

DEFINE_ENUM(ENumericResult,
    ((Int64)                 (0))
    ((Uint64)                (1))
    ((Double)                (2))
);

template <class TBlockStream, bool EnableLinePositionInfo>
class TLexerBase
    : public TCodedStream<TCharStream<TBlockStream, TPositionInfo<EnableLinePositionInfo>>>
{
private:
    using TBaseStream = TCodedStream<TCharStream<TBlockStream, TPositionInfo<EnableLinePositionInfo>>>;

    const i64 MemoryLimit_;

    std::vector<char> Buffer_;

    void Insert(const char* begin, const char* end)
    {
        ReserveAndCheckMemoryLimit(end - begin);
        Buffer_.insert(Buffer_.end(), begin, end);
    }

    void PushBack(char ch)
    {
        ReserveAndCheckMemoryLimit(1);
        Buffer_.push_back(ch);
    }

    void ReserveAndCheckMemoryLimit(size_t size)
    {
        auto minReserveSize = Buffer_.size() + size;

        // NB. some implementations of std::vector reserve exactly requested size.
        // We explicitly set exponential growth here so PushBack that uses this function
        // keep amortized complexity of O(1).
        auto reserveSize = Max(Buffer_.capacity() * 2, minReserveSize);
        if (minReserveSize > static_cast<size_t>(MemoryLimit_)) {
            THROW_ERROR_EXCEPTION(
                "Memory limit exceeded while parsing YSON stream: allocated %v, limit %v",
                minReserveSize,
                MemoryLimit_);
        }

        // MemoryLimit_ >= minReserveSize  ==>  reserveSize >= minReserveSize
        reserveSize = Min(reserveSize, static_cast<size_t>(MemoryLimit_));
        if (minReserveSize <= Buffer_.capacity()) {
            return;
        }

        YT_ASSERT(reserveSize >= minReserveSize);
        Buffer_.reserve(reserveSize);
    }

public:
    TLexerBase(
        const TBlockStream& blockStream,
        i64 memoryLimit = std::numeric_limits<i64>::max())
        : TBaseStream(blockStream)
        , MemoryLimit_(memoryLimit)
    { }

    /// Lexer routines

    template <bool AllowFinish>
    ENumericResult ReadNumeric(TStringBuf* value)
    {
        Buffer_.clear();
        auto result = ENumericResult::Int64;
        while (true) {
            char ch = TBaseStream::template GetChar<AllowFinish>();
            if (isdigit(ch) || ch == '+' || ch == '-') { // Seems like it can't be '+' or '-'
                PushBack(ch);
            } else if (ch == '.' || ch == 'e' || ch == 'E') {
                PushBack(ch);
                result = ENumericResult::Double;
            } else if (ch == 'u') {
                PushBack(ch);
                result = ENumericResult::Uint64;
            } else if (isalpha(ch)) {
                THROW_ERROR_EXCEPTION("Unexpected %Qv in numeric literal",
                    ch)
                    << *this;
            } else {
                break;
            }
            TBaseStream::Advance(1);
        }

        *value = TStringBuf(Buffer_.data(), Buffer_.size());
        return result;
    }

    template <bool AllowFinish>
    double ReadNanOrInf()
    {
        Buffer_.clear();

        static const TStringBuf nanString = "nan";
        static const TStringBuf infString = "inf";
        static const TStringBuf plusInfString = "+inf";
        static const TStringBuf minusInfString = "-inf";

        TStringBuf expectedString;
        double expectedValue;
        PushBack(TBaseStream::template GetChar<AllowFinish>());
        TBaseStream::Advance(1);
        switch (Buffer_.back()) {
            case '+':
                expectedString = plusInfString;
                expectedValue = std::numeric_limits<double>::infinity();
                break;
            case '-':
                expectedString = minusInfString;
                expectedValue = -std::numeric_limits<double>::infinity();
                break;
            case 'i':
                expectedString = infString;
                expectedValue = std::numeric_limits<double>::infinity();
                break;
            case 'n':
                expectedString = nanString;
                expectedValue = std::numeric_limits<double>::quiet_NaN();
                break;
            default:
                THROW_ERROR_EXCEPTION("Incorrect %%-literal prefix: %Qc",
                    Buffer_.back());
        }

        for (int i = 1; i < static_cast<int>(expectedString.size()); ++i) {
            PushBack(TBaseStream::template GetChar<AllowFinish>());
            TBaseStream::Advance(1);
            if (Buffer_.back() != expectedString[i]) {
                THROW_ERROR_EXCEPTION("Incorrect %%-literal prefix \"%v%c\", expected %Qv",
                    expectedString.SubStr(0, i),
                    Buffer_.back(),
                    expectedString);
            }
        }

        return expectedValue;
    }

    TStringBuf ReadQuotedString()
    {
        Buffer_.clear();
        while (true) {
            if (TBaseStream::IsEmpty()) {
                TBaseStream::Refresh();
            }
            char ch = *TBaseStream::Current();
            TBaseStream::Advance(1);
            if (ch != '"') {
                PushBack(ch);
            } else {
                // We must count the number of '\' at the end of StringValue
                // to check if it's not \"
                int slashCount = 0;
                int length = Buffer_.size();
                while (slashCount < length && Buffer_[length - 1 - slashCount] == '\\') {
                    ++slashCount;
                }
                if (slashCount % 2 == 0) {
                    break;
                } else {
                    PushBack(ch);
                }
            }
        }

        auto unquotedValue = UnescapeC(Buffer_.data(), Buffer_.size());
        Buffer_.clear();
        Insert(unquotedValue.data(), unquotedValue.data() + unquotedValue.size());
        return TStringBuf(Buffer_.data(), Buffer_.size());
    }

    template <bool AllowFinish>
    TStringBuf ReadUnquotedString()
    {
        Buffer_.clear();
        while (true) {
            char ch = TBaseStream::template GetChar<AllowFinish>();
            if (isalpha(ch) || isdigit(ch) ||
                ch == '_' || ch == '-' || ch == '%' || ch == '.')
            {
                PushBack(ch);
            } else {
                break;
            }
            TBaseStream::Advance(1);
        }
        return TStringBuf(Buffer_.data(), Buffer_.size());
    }

    TStringBuf ReadUnquotedString()
    {
        return ReadUnquotedString<false>();
    }

    TStringBuf ReadBinaryString()
    {
        ui32 ulength = TBaseStream::ReadVarint32();

        i32 length = ZigZagDecode32(ulength);
        if (length < 0) {
            THROW_ERROR_EXCEPTION("Negative binary string literal length %v",
                length)
                << *this;
        }

        if (TBaseStream::Current() + length <= TBaseStream::End()) {
            auto result = TStringBuf(TBaseStream::Current(), length);
            TBaseStream::Advance(length);
            return result;
        } else {
            size_t needToRead = length;
            Buffer_.clear();
            while (needToRead > 0) {
                if (TBaseStream::IsEmpty()) {
                    TBaseStream::Refresh();
                    continue;
                }
                size_t readingBytes = std::min(needToRead, TBaseStream::Length());
                Insert(TBaseStream::Current(), TBaseStream::Current() + readingBytes);
                needToRead -= readingBytes;
                TBaseStream::Advance(readingBytes);
            }
            return TStringBuf(Buffer_.data(), Buffer_.size());
        }
    }

    template <bool AllowFinish>
    bool ReadBoolean()
    {
        Buffer_.clear();

        static const TStringBuf trueString = "true";
        static const TStringBuf falseString = "false";

        auto throwIncorrectBoolean = [&] () {
            THROW_ERROR CreateLiteralError(ETokenType::Boolean, Buffer_.data(), Buffer_.size());
        };

        PushBack(TBaseStream::template GetChar<AllowFinish>());
        TBaseStream::Advance(1);
        if (Buffer_[0] == trueString[0]) {
            for (int i = 1; i < static_cast<int>(trueString.size()); ++i) {
                PushBack(TBaseStream::template GetChar<AllowFinish>());
                TBaseStream::Advance(1);
                if (Buffer_.back() != trueString[i]) {
                    throwIncorrectBoolean();
                }
            }
            return true;
        } else if (Buffer_[0] == falseString[0]) {
            for (int i = 1; i < static_cast<int>(falseString.size()); ++i) {
                PushBack(TBaseStream::template GetChar<AllowFinish>());
                TBaseStream::Advance(1);
                if (Buffer_.back() != falseString[i]) {
                    throwIncorrectBoolean();
                }
            }
            return false;
        } else {
            throwIncorrectBoolean();
        }

        YT_ABORT();
    }

    i64 ReadBinaryInt64()
    {
        ui64 uvalue = TBaseStream::ReadVarint64();
        return ZigZagDecode64(uvalue);
    }

    ui64 ReadBinaryUint64()
    {
        return TBaseStream::ReadVarint64();
    }

    double ReadBinaryDouble()
    {
        size_t needToRead = sizeof(double);

        double result;
        while (needToRead != 0) {
            if (TBaseStream::IsEmpty()) {
                TBaseStream::Refresh();
                continue;
            }

            size_t chunkSize = std::min(needToRead, TBaseStream::Length());
            if (chunkSize == 0) {
                THROW_ERROR_EXCEPTION("Error parsing binary double literal")
                    << *this;
            }
            std::copy(
                TBaseStream::Current(),
                TBaseStream::Current() + chunkSize,
                reinterpret_cast<char*>(&result) + (sizeof(double) - needToRead));
            needToRead -= chunkSize;
            TBaseStream::Advance(chunkSize);
        }
        return result;
    }

    /// Helpers
    void SkipCharToken(char symbol)
    {
        char ch = SkipSpaceAndGetChar();
        if (ch != symbol) {
            THROW_ERROR_EXCEPTION("Expected %Qv but found %Qv",
                symbol,
                ch)
                << *this;
        }

        TBaseStream::Advance(1);
    }

    template <bool AllowFinish>
    char SkipSpaceAndGetChar()
    {
        if (!TBaseStream::IsEmpty()) {
            char ch = *TBaseStream::Current();
            if (!IsSpace(ch)) {
                return ch;
            }
        }
        return SkipSpaceAndGetCharFallback<AllowFinish>();
    }

    char SkipSpaceAndGetChar()
    {
        return SkipSpaceAndGetChar<false>();
    }

    template <bool AllowFinish>
    char SkipSpaceAndGetCharFallback()
    {
        while (true) {
            if (TBaseStream::IsEmpty()) {
                if (TBaseStream::IsFinished()) {
                    return '\0';
                }
                TBaseStream::template Refresh<AllowFinish>();
                continue;
            }
            if (!IsSpace(*TBaseStream::Current())) {
                break;
            }
            TBaseStream::Advance(1);
        }
        return TBaseStream::template GetChar<AllowFinish>();
    }
};
////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TStringReader
{
private:
    const char* BeginPtr;
    const char* CurrentPtr;
    const char* EndPtr;

public:
    TStringReader()
        : BeginPtr(nullptr)
        , CurrentPtr(nullptr)
        , EndPtr(nullptr)
    { }

    TStringReader(const char* begin, const char* end)
        : BeginPtr(begin)
        , CurrentPtr(begin)
        , EndPtr(end)
    { }

    const char* Begin() const
    {
        return BeginPtr;
    }

    const char* Current() const
    {
        return CurrentPtr;
    }

    const char* End() const
    {
        return EndPtr;
    }

    void RefreshBlock()
    {
        YT_ABORT();
    }

    void Advance(size_t bytes)
    {
        CurrentPtr += bytes;
    }

    bool IsFinished() const
    {
        return true;
    }

    void SetBuffer(const char* begin, const char* end)
    {
        BeginPtr = begin;
        CurrentPtr = begin;
        EndPtr = end;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TParserCoroutine>
class TBlockReader
{
private:
    TParserCoroutine& Coroutine;

    const char* BeginPtr;
    const char* CurrentPtr;
    const char* EndPtr;
    bool FinishFlag;

public:
    TBlockReader(
        TParserCoroutine& coroutine,
        const char* begin,
        const char* end,
        bool finish)
        : Coroutine(coroutine)
        , BeginPtr(begin)
        , CurrentPtr(begin)
        , EndPtr(end)
        , FinishFlag(finish)
    { }

    const char* Begin() const
    {
        return BeginPtr;
    }

    const char* Current() const
    {
        return CurrentPtr;
    }

    const char* End() const
    {
        return EndPtr;
    }

    void RefreshBlock()
    {
        std::tie(BeginPtr, EndPtr, FinishFlag) = Coroutine.Yield(0);
        CurrentPtr = BeginPtr;
    }

    void Advance(size_t bytes)
    {
        CurrentPtr += bytes;
    }

    bool IsFinished() const
    {
        return FinishFlag;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
