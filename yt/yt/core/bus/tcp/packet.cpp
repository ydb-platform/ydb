#include "packet.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/misc/checksum.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PacketSignature = 0x78616d4f;
constexpr ui32 NullPacketPartSize = 0xffffffff;

constexpr int TypicalPacketPartCount = 16;
constexpr int TypicalVariableHeaderSize = TypicalPacketPartCount * (sizeof(ui32) + sizeof(ui64));

////////////////////////////////////////////////////////////////////////////////

struct TPacketDecoderTag { };

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TPacketHeader
{
    // Should be equal to PacketSignature.
    ui32 Signature;
    EPacketType Type;
    EPacketFlags Flags;
    TPacketId PacketId;
    ui32 PartCount;
    ui64 Checksum;
};

/*
  Variable-sized header:
    ui32 PartSizes[PartCount];
    ui64 PartChecksums[PartCount];
    ui64 Checksum;
*/

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPacketPhase,
    (FixedHeader)
    (VariableHeader)
    (MessagePart)
    (Finished)
);

template <class TDerived>
class TPacketTranscoderBase
{
public:
    explicit TPacketTranscoderBase(const NLogging::TLogger& logger)
        : Logger(logger)
    { }

    TMutableRef GetFragment()
    {
        return TMutableRef(FragmentPtr_, FragmentRemaining_);
    }

    bool IsFinished() const
    {
        return Phase_ == EPacketPhase::Finished;
    }

protected:
    const NLogging::TLogger& Logger;

    EPacketPhase Phase_ = EPacketPhase::Finished;
    char* FragmentPtr_ = nullptr;
    size_t FragmentRemaining_ = 0;

    TPacketHeader FixedHeader_;

    TCompactVector<char, TypicalVariableHeaderSize> VariableHeader_;
    size_t VariableHeaderSize_;
    ui32* PartSizes_;
    ui64* PartChecksums_;

    int PartIndex_ = -1;
    TSharedRefArray Message_;

    bool IsVariablePacket() const
    {
        return FixedHeader_.Type == EPacketType::Message || FixedHeader_.PartCount > 0;
    }

    void AllocateVariableHeader()
    {
        VariableHeaderSize_ =
            (sizeof(ui32) + sizeof(ui64)) * FixedHeader_.PartCount +
            sizeof(ui64);
        VariableHeader_.reserve(VariableHeaderSize_);
        PartSizes_ = reinterpret_cast<ui32*>(VariableHeader_.data());
        PartChecksums_ = reinterpret_cast<ui64*>(PartSizes_ + FixedHeader_.PartCount);
    }

    TChecksum GetFixedChecksum()
    {
        return GetChecksum(TRef(&FixedHeader_, sizeof(FixedHeader_) - sizeof(ui64)));
    }

    TChecksum GetVariableChecksum()
    {
        return GetChecksum(TRef(VariableHeader_.data(), VariableHeaderSize_ - sizeof(ui64)));
    }

    void BeginPhase(EPacketPhase phase, void* fragment, size_t size)
    {
        Phase_ = phase;
        FragmentPtr_ = static_cast<char*>(fragment);
        FragmentRemaining_ = size;
    }

    bool EndPhase()
    {
        switch (Phase_) {
            case EPacketPhase::FixedHeader:
                return AsDerived()->EndFixedHeaderPhase();

            case EPacketPhase::VariableHeader:
                return AsDerived()->EndVariableHeaderPhase();

            case EPacketPhase::MessagePart:
                return AsDerived()->EndMessagePartPhase();

            default:
                YT_ABORT();
        }
    }

    void SetFinished()
    {
        Phase_ = EPacketPhase::Finished;
        FragmentPtr_ = nullptr;
        FragmentRemaining_ = 0;
    }

    TDerived* AsDerived()
    {
        return static_cast<TDerived*>(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPacketDecoder
    : public IPacketDecoder
    , public TPacketTranscoderBase<TPacketDecoder>
{
public:
    TPacketDecoder(
        const NLogging::TLogger& logger,
        bool verifyChecksum)
        : TPacketTranscoderBase(logger)
        , VerifyChecksum_(verifyChecksum)
    {
        Restart();
    }

    TMutableRef GetFragment() override
    {
        return TPacketTranscoderBase::GetFragment();
    }

    bool IsFinished() const override
    {
        return TPacketTranscoderBase::IsFinished();
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

    void Restart() override
    {
        Phase_ = EPacketPhase::FixedHeader;
        PacketSize_ = 0;
        Parts_.clear();
        PartIndex_ = -1;
        Message_.Reset();

        BeginPhase(EPacketPhase::FixedHeader, &FixedHeader_, sizeof(TPacketHeader));
    }

    bool IsInProgress() const override
    {
        return Phase_ != EPacketPhase::Finished && PacketSize_ > 0;
    }

    EPacketType GetPacketType() const override
    {
        return FixedHeader_.Type;
    }

    EPacketFlags GetPacketFlags() const override
    {
        return FixedHeader_.Flags;
    }

    TPacketId GetPacketId() const override
    {
        return FixedHeader_.PacketId;
    }

    TSharedRefArray GrabMessage() const override
    {
        return std::move(Message_);
    }

    size_t GetPacketSize() const override
    {
        return PacketSize_;
    }

private:
    friend class TPacketTranscoderBase<TPacketDecoder>;

    std::vector<TSharedRef> Parts_;

    size_t PacketSize_ = 0;

    const bool VerifyChecksum_;

    bool EndFixedHeaderPhase()
    {
        if (FixedHeader_.Signature != PacketSignature) {
            YT_LOG_ERROR("Packet header signature mismatch (PacketId: %v, ExpectedSignature: %X, ActualSignature: %X)",
                FixedHeader_.PacketId,
                PacketSignature,
                FixedHeader_.Signature);
            return false;
        }

        if (FixedHeader_.PartCount > MaxMessagePartCount) {
            YT_LOG_ERROR("Invalid packet part count (PacketId: %v, PartCount: %v)",
                FixedHeader_.PacketId,
                FixedHeader_.PartCount);
            return false;
        }

        if (VerifyChecksum_) {
            auto expectedChecksum = FixedHeader_.Checksum;
            if (expectedChecksum != NullChecksum) {
                auto actualChecksum = GetFixedChecksum();
                if (expectedChecksum != actualChecksum) {
                    YT_LOG_ERROR("Fixed packet header checksum mismatch (PacketId: %v)",
                        FixedHeader_.PacketId);
                    return false;
                }
            }
        }

        if (IsVariablePacket()) {
            AllocateVariableHeader();
            BeginPhase(EPacketPhase::VariableHeader, VariableHeader_.data(), VariableHeaderSize_);
        } else {
            SetFinished();
        }

        return true;
    }

    bool EndVariableHeaderPhase()
    {
        if (VerifyChecksum_) {
            auto expectedChecksum = PartChecksums_[FixedHeader_.PartCount];
            if (expectedChecksum != NullChecksum) {
                auto actualChecksum = GetVariableChecksum();
                if (expectedChecksum != actualChecksum) {
                    YT_LOG_ERROR("Variable packet header checksum mismatch (PacketId: %v)",
                        FixedHeader_.PacketId);
                    return false;
                }
            }
        }

        for (int index = 0; index < static_cast<int>(FixedHeader_.PartCount); ++index) {
            ui32 partSize = PartSizes_[index];
            if (partSize != NullPacketPartSize && partSize > MaxMessagePartSize) {
                YT_LOG_ERROR("Invalid packet part size (PacketId: %v, PartIndex: %v, PartSize: %v)",
                    FixedHeader_.PacketId,
                    index,
                    partSize);
                return false;
            }
        }

        NextMessagePartPhase();
        return true;
    }

    bool EndMessagePartPhase()
    {
        if (VerifyChecksum_) {
            auto expectedChecksum = PartChecksums_[PartIndex_];
            if (expectedChecksum != NullChecksum) {
                auto actualChecksum = GetChecksum(Parts_[PartIndex_]);
                if (expectedChecksum != actualChecksum) {
                    YT_LOG_ERROR("Packet part checksum mismatch (PacketId: %v)",
                        FixedHeader_.PacketId);
                    return false;
                }
            }
        }

        NextMessagePartPhase();
        return true;
    }

    void NextMessagePartPhase()
    {
        while (true) {
            ++PartIndex_;
            if (PartIndex_ == static_cast<int>(FixedHeader_.PartCount)) {
                Message_ = TSharedRefArray(std::move(Parts_), TSharedRefArray::TMoveParts{});
                SetFinished();
                break;
            }

            ui32 partSize = PartSizes_[PartIndex_];
            if (partSize == NullPacketPartSize) {
                Parts_.push_back(TSharedRef());
            } else if (partSize == 0) {
                Parts_.push_back(TSharedRef::MakeEmpty());
            } else {
                auto part = TSharedMutableRef::Allocate<TPacketDecoderTag>(partSize);
                BeginPhase(EPacketPhase::MessagePart, part.Begin(), part.Size());
                Parts_.push_back(std::move(part));
                break;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPacketEncoder
    : public IPacketEncoder
    , public TPacketTranscoderBase<TPacketEncoder>
{
public:
    explicit TPacketEncoder(const NLogging::TLogger& logger)
        : TPacketTranscoderBase(logger)
    {
        FixedHeader_.Signature = PacketSignature;
    }

    TMutableRef GetFragment() override
    {
        return TPacketTranscoderBase::GetFragment();
    }

    bool IsFinished() const override
    {
        return TPacketTranscoderBase::IsFinished();
    }

    size_t GetPacketSize(
        EPacketType type,
        const TSharedRefArray& message,
        size_t payloadSize) override
    {
        size_t size = sizeof(TPacketHeader);
        if (type == EPacketType::Message || !message.Empty()) {
            size +=
                message.Size() * (sizeof(ui32) + sizeof(ui64)) +
                sizeof(ui64) +
                payloadSize;
        }
        return size;
    }

    bool Start(
        EPacketType type,
        EPacketFlags flags,
        bool generateChecksums,
        int checksummedPartCount,
        TPacketId packetId,
        TSharedRefArray message) override
    {
        PartIndex_ = -1;
        Message_ = std::move(message);

        FixedHeader_.Type = type;
        FixedHeader_.Flags = flags;
        FixedHeader_.PacketId = packetId;
        FixedHeader_.PartCount = Message_.Size();
        FixedHeader_.Checksum = generateChecksums ? GetFixedChecksum() : NullChecksum;

        if (IsVariablePacket()) {
            AllocateVariableHeader();

            for (int index = 0; index < static_cast<int>(Message_.Size()); ++index) {
                const auto& part = Message_[index];
                if (part) {
                    PartSizes_[index] = part.Size();
                    PartChecksums_[index] = generateChecksums && index < checksummedPartCount
                        ? GetChecksum(part)
                        : NullChecksum;
                } else {
                    PartSizes_[index] = NullPacketPartSize;
                    PartChecksums_[index] = NullChecksum;
                }
            }

            PartChecksums_[Message_.Size()] = generateChecksums ? GetVariableChecksum() : NullChecksum;
        }

        BeginPhase(EPacketPhase::FixedHeader, &FixedHeader_, sizeof (TPacketHeader));
        return true;
    }

    bool IsFragmentOwned() const override
    {
        return Phase_ == EPacketPhase::MessagePart;
    }

    void NextFragment() override
    {
        EndPhase();
    }

private:
    friend class TPacketTranscoderBase<TPacketEncoder>;

    bool EndFixedHeaderPhase()
    {
        if (IsVariablePacket()) {
            BeginPhase(EPacketPhase::VariableHeader, VariableHeader_.data(), VariableHeaderSize_);
        } else {
            SetFinished();
        }
        return true;
    }

    bool EndVariableHeaderPhase()
    {
        NextMessagePartPhase();
        return true;
    }

    bool EndMessagePartPhase()
    {
        NextMessagePartPhase();
        return true;
    }

    void NextMessagePartPhase()
    {
        while (true) {
            ++PartIndex_;
            if (PartIndex_ == static_cast<int>(FixedHeader_.PartCount)) {
                break;
            }

            const auto& part = Message_[PartIndex_];
            if (part.Size() != 0) {
                BeginPhase(EPacketPhase::MessagePart, const_cast<char*>(part.Begin()), part.Size());
                return;
            }
        }

        Message_.Reset();
        SetFinished();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPacketTranscoderFactory
    : public IPacketTranscoderFactory
{
public:
    std::unique_ptr<IPacketDecoder> CreateDecoder(
        const NLogging::TLogger& logger,
        bool verifyChecksum) const override
    {
        return std::make_unique<TPacketDecoder>(logger, verifyChecksum);
    }

    std::unique_ptr<IPacketEncoder> CreateEncoder(
        const NLogging::TLogger& logger) const override
    {
        return std::make_unique<TPacketEncoder>(logger);
    }
};

////////////////////////////////////////////////////////////////////////////////

IPacketTranscoderFactory* GetYTPacketTranscoderFactory()
{
    return LeakySingleton<TPacketTranscoderFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

Y_DECLARE_PODTYPE(NYT::NBus::TPacketHeader);
