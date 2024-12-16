#include "packet.h"

#include <library/cpp/yt/memory/chunked_memory_allocator.h>

namespace NYT::NKafka {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPacketPhase,
    (Header)
    (Message)
    (Finished)
);

struct TPacketDecoderTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TPacketTranscoderBase
{
protected:
    union {
        int MessageSize;
        char Data[sizeof(int)];
    } Header_;
};

////////////////////////////////////////////////////////////////////////////////

class TPacketDecoder
    : public IPacketDecoder
    , public TPacketTranscoderBase
{
public:
    TPacketDecoder()
        : Allocator_(
            PacketDecoderChunkSize,
            TChunkedMemoryAllocator::DefaultMaxSmallBlockSizeRatio,
            GetRefCountedTypeCookie<TPacketDecoderTag>())
    {
        Restart();
    }

    void Restart() override
    {
        PacketSize_ = 0;
        Message_.Reset();

        BeginPhase(
            EPacketPhase::Header,
            Header_.Data,
            sizeof(Header_.Data));
    }

    bool IsInProgress() const override
    {
        return !IsFinished();
    }

    bool IsFinished() const override
    {
        return Phase_ == EPacketPhase::Finished;
    }

    TMutableRef GetFragment() override
    {
        return TMutableRef(FragmentPtr_, FragmentRemaining_);
    }

    bool Advance(size_t size) override
    {
        YT_ASSERT(FragmentRemaining_ != 0);
        YT_ASSERT(size <= FragmentRemaining_);

        PacketSize_ += size;
        FragmentRemaining_ -= size;
        FragmentPtr_ += size;
        if (FragmentRemaining_ == 0) {
            return EndPhase();
        } else {
            return true;
        }
    }

    EPacketType GetPacketType() const override
    {
        return EPacketType::Message;
    }

    EPacketFlags GetPacketFlags() const override
    {
        return EPacketFlags::None;
    }

    TPacketId GetPacketId() const override
    {
        return {};
    }

    size_t GetPacketSize() const override
    {
        return PacketSize_;
    }

    TSharedRefArray GrabMessage() const override
    {
        return TSharedRefArray(Message_);
    }

private:
    EPacketPhase Phase_ = EPacketPhase::Finished;
    char* FragmentPtr_ = nullptr;
    size_t FragmentRemaining_ = 0;

    TChunkedMemoryAllocator Allocator_;
    static constexpr i64 PacketDecoderChunkSize = 16_KB;

    TSharedRef Message_;

    size_t PacketSize_ = 0;

    void BeginPhase(
        EPacketPhase phase,
        char* fragmentPtr,
        size_t fragmentSize)
    {
        Phase_ = phase;
        FragmentPtr_ = fragmentPtr;
        FragmentRemaining_ = fragmentSize;
    }

    bool EndPhase()
    {
        switch (Phase_) {
            case EPacketPhase::Header: {
                std::reverse(std::begin(Header_.Data), std::end(Header_.Data));
                auto messageSize = Header_.MessageSize;
                auto message = Allocator_.AllocateAligned(messageSize);
                BeginPhase(
                    EPacketPhase::Message,
                    /*fragmentPtr*/ message.begin(),
                    /*fragmentSize*/ message.size());
                Message_ = std::move(message);

                return true;
            }
            case EPacketPhase::Message:
                BeginPhase(
                    EPacketPhase::Finished,
                    /*fragmentPtr*/ nullptr,
                    /*fragmentSize*/ 0);

                return true;
            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPacketEncoder
    : public IPacketEncoder
    , public TPacketTranscoderBase
{
public:
    size_t GetPacketSize(
        EPacketType type,
        const TSharedRefArray& /*message*/,
        size_t messageSize) override
    {
        YT_ASSERT(type == EPacketType::Message);
        YT_ASSERT(messageSize > 0);

        return sizeof(Header_) + messageSize;
    }

    bool Start(
        EPacketType type,
        EPacketFlags flags,
        bool /*generateChecksums*/,
        int /*checksummedPartCount*/,
        TPacketId /*packetId*/,
        TSharedRefArray messageParts) override
    {
        YT_ASSERT(type == EPacketType::Message);
        YT_ASSERT(flags == EPacketFlags::None);
        YT_ASSERT(!messageParts.Empty());

        Header_.MessageSize = messageParts.ByteSize();
        std::reverse(std::begin(Header_.Data), std::end(Header_.Data));

        MessageParts_ = std::move(messageParts);

        Phase_ = EPacketPhase::Header;
        CurrentMessagePart_ = 0;

        return true;
    }

    TMutableRef GetFragment() override
    {
        switch (Phase_) {
            case EPacketPhase::Header:
                return TMutableRef(Header_.Data, sizeof(Header_.Data));
            case EPacketPhase::Message: {
                const auto& messagePart = MessageParts_[CurrentMessagePart_];
                return TMutableRef(
                    const_cast<char*>(messagePart.begin()),
                    messagePart.size());
            }
            default:
                YT_ABORT();
        }
    }

    bool IsFragmentOwned() const override
    {
        switch (Phase_) {
            case EPacketPhase::Header:
                return false;
            case EPacketPhase::Message:
                return true;
            default:
                YT_ABORT();
        }
    }

    void NextFragment() override
    {
        switch (Phase_) {
            case EPacketPhase::Header:
                Phase_ = EPacketPhase::Message;
                CurrentMessagePart_ = 0;
                break;
            case EPacketPhase::Message:
                if (CurrentMessagePart_ + 1 < MessageParts_.size()) {
                    ++CurrentMessagePart_;
                } else {
                    Phase_ = EPacketPhase::Finished;
                }
                break;
            default:
                YT_ABORT();
        }
    }

    bool IsFinished() const override
    {
        return Phase_ == EPacketPhase::Finished;
    }

private:
    EPacketPhase Phase_ = EPacketPhase::Finished;

    TSharedRefArray MessageParts_;
    size_t CurrentMessagePart_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TPacketTranscoderFactory
    : public IPacketTranscoderFactory
{
    std::unique_ptr<IPacketDecoder> CreateDecoder(
        const NLogging::TLogger& /*logger*/,
        bool /*verifyChecksum*/) const override
    {
        return std::make_unique<TPacketDecoder>();
    }

    std::unique_ptr<IPacketEncoder> CreateEncoder(
        const NLogging::TLogger& /*logger*/) const override
    {
        return std::make_unique<TPacketEncoder>();
    }
};

////////////////////////////////////////////////////////////////////////////////

IPacketTranscoderFactory* GetKafkaPacketTranscoderFactory()
{
    return LeakySingleton<TPacketTranscoderFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
