#include "protocol.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = KafkaLogger;

////////////////////////////////////////////////////////////////////////////////

class TKafkaProtocolReader
    : public IKafkaProtocolReader
{
public:
    explicit TKafkaProtocolReader(TSharedRef data)
        : Data_(std::move(data))
    { }

    bool ReadBool() override
    {
        YT_LOG_TRACE("Reading bool");
        auto value = ReadByte();
        return value > 0;
    }

    char ReadByte() override
    {
        YT_LOG_TRACE("Reading byte");
        return DoReadInt<char>();
    }

    i16 ReadInt16() override
    {
        YT_LOG_TRACE("Reading int16");
        return DoReadInt<i16>();
    }

    i32 ReadInt32() override
    {
        YT_LOG_TRACE("Reading int32");
        return DoReadInt<i32>();
    }

    i64 ReadInt64() override
    {
        YT_LOG_TRACE("Reading int64");
        return DoReadInt<i64>();
    }

    ui32 ReadUint32() override
    {
        YT_LOG_TRACE("Reading uint32");
        return DoReadInt<ui32>();
    }

    i32 ReadVarInt() override
    {
        YT_LOG_TRACE("Reading varint");
        i32 result;
        Offset_ += ReadVarInt32(Data_.begin() + Offset_, &result);
        return result;
    }

    i64 ReadVarLong() override
    {
        YT_LOG_TRACE("Reading varlong");
        i64 result;
        Offset_ += ReadVarInt64(Data_.begin() + Offset_, &result);
        return result;
    }

    ui32 ReadUnsignedVarInt() override
    {
        YT_LOG_TRACE("Reading unsigned varint");
        ui32 result;
        Offset_ += ReadVarUint32(Data_.begin() + Offset_, &result);
        return result;
    }

    std::optional<TString> ReadNullableString() override
    {
        YT_LOG_TRACE("Reading nullable string");
        auto length = ReadInt16();
        if (length == -1) {
            return {};
        }

        TString result;
        ReadString(&result, length);
        return result;
    }

    std::optional<TString> ReadCompactNullableString() override
    {
        YT_LOG_TRACE("Reading compact nullable string");
        auto length = ReadUnsignedVarInt();
        if (length == 0) {
            return {};
        }

        TString result;
        ReadString(&result, length - 1);

        return result;
    }

    TString ReadCompactString() override
    {
        YT_LOG_TRACE("Reading compact string");
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
        YT_LOG_TRACE("Reading string");
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
        YT_LOG_TRACE("Reading bytes");
        TString result;

        auto length = ReadInt32();
        if (length == -1) {
            return result;
        }

        ReadString(&result, length);

        return result;
    }

    TGuid ReadUuid() override
    {
        YT_LOG_TRACE("Reading uuid");
        TString value;
        ReadString(&value, 16);
        return TGuid::FromString(value);
    }

    void ReadString(TString* result, int length) override
    {
        YT_LOG_TRACE("Reading string with length (Length: %v, DataSize: %v, Offset: %v)",
            length,
            Data_.size(),
            Offset_);
        ValidateSizeAvailable(length);

        result->resize(length);
        auto begin = Data_.begin() + Offset_;
        std::copy(begin, begin + length, result->begin());
        Offset_ += length;
    }

    TString ReadCompactBytes() override
    {
        YT_LOG_TRACE("Reading compact bytes");
        TString result;

        auto length = ReadUnsignedVarInt();
        if (length == 0) {
            return result;
        }

        ReadString(&result, length - 1);
        return result;
    }

    i32 StartReadBytes(bool needReadCount) override
    {
        YT_LOG_TRACE("Start reading bytes");
        i32 size = 0;
        if (needReadCount) {
            size = ReadInt32();
        }
        BytesOffsets_.push_back(Offset_);
        return size;
    }

    i32 StartReadCompactBytes(bool needReadCount) override
    {
        YT_LOG_TRACE("Start reading compact bytes");
        i32 size = 0;
        if (needReadCount) {
            size = ReadUnsignedVarInt() - 1;
        }
        BytesOffsets_.push_back(Offset_);
        return size;
    }

    i32 GetReadBytesCount() override
    {
        if (!BytesOffsets_.empty()) {
            return Offset_ - BytesOffsets_.back();
        }
        return 0;
    }

    void FinishReadBytes() override
    {
        YT_LOG_TRACE("Finish reading bytes");
        if (!BytesOffsets_.empty()) {
            return BytesOffsets_.pop_back();
        }
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

    std::vector<i64> BytesOffsets_;

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
            THROW_ERROR_EXCEPTION("Premature end of stream while reading %v bytes", size)
                << TErrorAttribute("data_size", std::ssize(Data_))
                << TErrorAttribute("offset", Offset_);
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

    void WriteBool(bool value) override
    {
        WriteByte(value ? 1 : 0);
    }

    void WriteByte(char value) override
    {
        DoWriteInt(value);
    }

    void WriteInt16(i16 value) override
    {
        DoWriteInt(value);
    }

    void WriteInt32(i32 value) override
    {
        DoWriteInt(value);
    }

    void WriteInt64(i64 value) override
    {
        DoWriteInt(value);
    }

    void WriteUint32(ui32 value) override
    {
        DoWriteInt(value);
    }

    void WriteVarInt(i32 value) override
    {
        Size_ += WriteVarInt32(Buffer_.begin() + Size_, value);
    }

    void WriteVarLong(i64 value) override
    {
        Size_ += WriteVarInt64(Buffer_.begin() + Size_, value);
    }

    void WriteUnsignedVarInt(ui32 value) override
    {
        Size_ += WriteVarUint32(Buffer_.begin() + Size_, value);
    }

    void WriteUuid(TGuid value) override
    {
        WriteString(ToString(value));
    }

    void WriteErrorCode(NKafka::EErrorCode value) override
    {
        DoWriteInt(static_cast<int16_t>(value));
    }

    void WriteString(TStringBuf value) override
    {
        WriteInt16(value.size());
        WriteData(value);
    }

    void WriteNullableString(std::optional<TStringBuf> value) override
    {
        if (!value) {
            WriteInt16(-1);
            return;
        }

        WriteString(*value);
    }

    void WriteCompactString(TStringBuf value) override
    {
        WriteUnsignedVarInt(value.size() + 1);
        WriteData(value);
    }

    void WriteCompactNullableString(std::optional<TStringBuf> value) override
    {
        if (!value) {
            WriteUnsignedVarInt(0);
            return;
        }
        WriteCompactString(*value);
    }

    void WriteBytes(TStringBuf value) override
    {
        WriteInt32(value.size());
        WriteData(value);
    }

    void WriteCompactBytes(TStringBuf value) override
    {
        WriteUnsignedVarInt(value.size() + 1);
        WriteData(value);
    }

    void WriteData(TStringBuf value) override
    {
        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    void WriteData(TRef value) override
    {
        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    void StartBytes() override
    {
        WriteInt32(0);
        BytesOffsets_.push_back(Size_);
    }

    void FinishBytes() override
    {
        YT_VERIFY(!BytesOffsets_.empty());
        DoWriteInt<i32>(Size_ - BytesOffsets_.back(), BytesOffsets_.back() - sizeof(i32));
        BytesOffsets_.pop_back();
    }

    void StartCalculateChecksum() override
    {
        WriteInt32(0);
        ChecksumOffsets_.push_back(Size_);
    }

    void FinishCalculateChecksum() override
    {
        YT_VERIFY(!ChecksumOffsets_.empty());
        auto offset = ChecksumOffsets_.back();
        ChecksumOffsets_.pop_back();

        auto data = Buffer_.Slice(offset, Size_);
        auto checksum = Crc32c(data.begin(), data.size());

        DoWriteInt<ui32>(checksum, offset - sizeof(ui32));
    }

    TSharedRef Finish() override
    {
        return Buffer_.Slice(0, Size_);
    }

    i64 GetSize() const override
    {
        return Size_;
    }

private:
    struct TKafkaProtocolWriterTag
    { };

    static constexpr i64 InitialBufferSize = 16_KB;
    static constexpr i64 BufferSizeMultiplier = 2;

    TSharedMutableRef Buffer_;
    i64 Size_ = 0;

    std::vector<i64> BytesOffsets_;
    std::vector<i64> ChecksumOffsets_;

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
