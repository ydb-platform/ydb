#include "protocol.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

class TZookeeperProtocolReader
    : public IZookeeperProtocolReader
{
public:
    explicit TZookeeperProtocolReader(TSharedRef data)
        : Data_(std::move(data))
    { }

    char ReadByte() override
    {
        return DoReadInt<char>();
    }

    int ReadInt() override
    {
        return DoReadInt<int>();
    }

    i64 ReadLong() override
    {
        return DoReadInt<i64>();
    }

    bool ReadBool() override
    {
        auto value = ReadByte();
        return value > 0;
    }

    TString ReadString() override
    {
        TString result;
        ReadString(&result);

        return result;
    }

    void ReadString(TString* result) override
    {
        auto length = ReadInt();
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

std::unique_ptr<IZookeeperProtocolReader> CreateZookeeperProtocolReader(TSharedRef data)
{
    return std::make_unique<TZookeeperProtocolReader>(std::move(data));
}

////////////////////////////////////////////////////////////////////////////////

class TZookeeperProtocolWriter
    : public IZookeeperProtocolWriter
{
public:
    TZookeeperProtocolWriter()
        : Buffer_(AllocateBuffer(InitialBufferSize))
    { }

    void WriteByte(char value) override
    {
        DoWriteInt(value);
    }

    void WriteInt(int value) override
    {
        DoWriteInt(value);
    }

    void WriteLong(i64 value) override
    {
        DoWriteInt(value);
    }

    void WriteBool(bool value) override
    {
        WriteByte(value ? 1 : 0);
    }

    void WriteString(const TString& value) override
    {
        WriteInt(value.size());

        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    TSharedRef Finish() override
    {
        return Buffer_.Slice(0, Size_);
    }

private:
    struct TZookeeperProtocolWriterTag
    { };

    static constexpr i64 InitialBufferSize = 16_KB;
    static constexpr i64 BufferSizeMultiplier = 2;

    TSharedMutableRef Buffer_;
    i64 Size_ = 0;

    template <typename T>
    void DoWriteInt(T value)
    {
        EnsureFreeSpace(sizeof(T));

        memcpy(Buffer_.begin() + Size_, &value, sizeof(T));
        std::reverse(Buffer_.begin() + Size_, Buffer_.begin() + Size_ + sizeof(T));
        Size_+= sizeof(T);
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
        return TSharedMutableRef::Allocate<TZookeeperProtocolWriterTag>(capacity);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IZookeeperProtocolWriter> CreateZookeeperProtocolWriter()
{
    return std::make_unique<TZookeeperProtocolWriter>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
