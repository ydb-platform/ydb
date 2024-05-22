#include "protocol.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/coding/varint.h>

#include <util/generic/guid.h>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

class TKafkaProtocolReader
    : public IKafkaProtocolReader
{
public:
    explicit TKafkaProtocolReader(TSharedRef data)
        : Data_(std::move(data))
    { }

    char ReadByte() override
    {
        return DoReadInt<char>();
    }

    int16_t ReadInt16() override
    {
        return DoReadInt<int16_t>();
    }

    int32_t ReadInt32() override
    {
        return DoReadInt<int32_t>();
    }

    int64_t ReadInt64() override
    {
        return DoReadInt<int64_t>();
    }

    ui64 ReadUnsignedVarInt() override
    {
        ui64 result;
        Offset_ += ReadVarUint64(Data_.begin() + Offset_, &result);
        return result;
    }

    bool ReadBool() override
    {
        auto value = ReadByte();
        return value > 0;
    }

    TString ReadCompactString() override
    {
        TString result;

        auto length = ReadUnsignedVarInt();
        if (length <= 1) {
            return result;
        }

        ReadString(&result, length - 1);

        return result;
    }

    TString ReadString() override
    {
        TString result;

        auto length = ReadInt16();
        if (length == -1) {
            return result;
        }

        ReadString(&result, length);

        return result;
    }

    TString ReadBytes() override
    {
        TString result;

        auto length = ReadInt32();
        if (length == -1) {
            return result;
        }

        ReadString(&result, length);

        return result;
    }

    TGUID ReadUuid() override
    {
        TString value;
        ReadString(&value, 16);
        return GetGuid(value);
    }

    void ReadString(TString* result, int length) override
    {
        ValidateSizeAvailable(length);

        result->resize(length);
        auto begin = Data_.begin() + Offset_;
        std::copy(begin, begin + length, result->begin());
        Offset_ += length;
    }

    TSharedRef GetSuffix() const override
    {
        return Data_.Slice(Offset_, Data_.Size());
    }

    bool IsFinished() const override
    {
        return Offset_ == std::ssize(Data_);
    }

    void ValidateFinished() const override
    {
        if (!IsFinished()) {
            THROW_ERROR_EXCEPTION("Expected end of stream")
                << TErrorAttribute("offset", Offset_)
                << TErrorAttribute("message_size", Data_.size());
        }
    }

private:
    const TSharedRef Data_;
    i64 Offset_ = 0;

    template <typename T>
    T DoReadInt()
    {
        ValidateSizeAvailable(sizeof(T));

        union {
            T value;
            char bytes[sizeof(T)];
        } result;

        memcpy(result.bytes, Data_.begin() + Offset_, sizeof(T));
        std::reverse(result.bytes, result.bytes + sizeof(T));
        Offset_ += sizeof(T);

        return result.value;
    }

    void ValidateSizeAvailable(i64 size)
    {
        if (std::ssize(Data_) - Offset_ < size) {
            THROW_ERROR_EXCEPTION("Premature end of stream while reading %v bytes", size);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IKafkaProtocolReader> CreateKafkaProtocolReader(TSharedRef data)
{
    return std::make_unique<TKafkaProtocolReader>(std::move(data));
}

////////////////////////////////////////////////////////////////////////////////

class TKafkaProtocolWriter
    : public IKafkaProtocolWriter
{
public:
    TKafkaProtocolWriter()
        : Buffer_(AllocateBuffer(InitialBufferSize))
    { }

    void WriteByte(char value) override
    {
        DoWriteInt(value);
    }

    void WriteInt16(int16_t value) override
    {
        DoWriteInt(value);
    }

    void WriteInt32(int32_t value) override
    {
        DoWriteInt(value);
    }

    void WriteInt64(int64_t value) override
    {
        DoWriteInt(value);
    }

    void WriteUnsignedVarInt(uint64_t value) override
    {
        Size_ += WriteVarUint64(Buffer_.begin() + Size_, value);
    }

    void WriteErrorCode(EErrorCode value) override
    {
        DoWriteInt(static_cast<int16_t>(value));
    }

    void WriteBool(bool value) override
    {
        WriteByte(value ? 1 : 0);
    }

    void WriteNullableString(const std::optional<TString>& value) override
    {
        if (!value) {
            WriteInt16(-1);
            return;
        }

        WriteString(*value);
    }

    void WriteString(const TString& value) override
    {
        WriteInt16(value.size());

        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    void WriteCompactString(const TString& value) override
    {
        WriteUnsignedVarInt(value.size());

        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    void WriteBytes(const TString& value) override
    {
        WriteInt32(value.size());

        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    void StartBytes() override
    {
        WriteInt32(0);
        BytesBegins_.push_back(Size_);
    }

    void FinishBytes() override
    {
        YT_VERIFY(!BytesBegins_.empty());
        DoWriteInt<int32_t>(Size_ - BytesBegins_.back(), BytesBegins_.back() - sizeof(int32_t));
        BytesBegins_.pop_back();
    }

    TSharedRef Finish() override
    {
        return Buffer_.Slice(0, Size_);
    }

private:
    struct TKafkaProtocolWriterTag
    { };

    constexpr static i64 InitialBufferSize = 16_KB;
    constexpr static i64 BufferSizeMultiplier = 2;

    TSharedMutableRef Buffer_;
    i64 Size_ = 0;

    std::vector<i64> BytesBegins_;

    template <typename T>
    void DoWriteInt(T value, std::optional<i64> position = std::nullopt)
    {
        if (!position) {
            EnsureFreeSpace(sizeof(T));
        }

        i64 realPosition = Size_;
        if (position) {
            realPosition = *position;
        }
        memcpy(Buffer_.begin() + realPosition, &value, sizeof(T));
        std::reverse(Buffer_.begin() + realPosition, Buffer_.begin() + realPosition + sizeof(T));

        if (!position) {
            Size_+= sizeof(T);
        }
    }

    void EnsureFreeSpace(i64 size)
    {
        if (Size_ + size <= std::ssize(Buffer_)) {
            return;
        }

        auto newSize = std::max<i64>(Size_ + size, Buffer_.size() * BufferSizeMultiplier);
        auto newBuffer = AllocateBuffer(newSize);
        std::copy(Buffer_.begin(), Buffer_.begin() + Size_, newBuffer.begin());
        Buffer_ = std::move(newBuffer);
    }

    static TSharedMutableRef AllocateBuffer(i64 capacity)
    {
        return TSharedMutableRef::Allocate<TKafkaProtocolWriterTag>(capacity);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IKafkaProtocolWriter> CreateKafkaProtocolWriter()
{
    return std::make_unique<TKafkaProtocolWriter>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
