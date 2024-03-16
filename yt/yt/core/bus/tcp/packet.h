#pragma once

#include "private.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EPacketType, i16,
    ((Message)(0))
    ((Ack)    (1))
    ((SslAck) (2))
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EPacketFlags, ui16,
    ((None)                     (0x0000))
    ((RequestAcknowledgement)   (0x0001))
);

////////////////////////////////////////////////////////////////////////////////

struct IPacketDecoder
{
    virtual ~IPacketDecoder() = default;

    virtual void Restart() = 0;

    virtual bool IsInProgress() const = 0;
    virtual bool IsFinished() const = 0;

    virtual TMutableRef GetFragment() = 0;
    virtual bool Advance(size_t size) = 0;

    virtual EPacketType GetPacketType() const = 0;
    virtual EPacketFlags GetPacketFlags() const = 0;
    virtual TPacketId GetPacketId() const = 0;
    virtual size_t GetPacketSize() const = 0;
    virtual TSharedRefArray GrabMessage() const = 0;
};

struct IPacketEncoder
{
    virtual ~IPacketEncoder() = default;

    virtual size_t GetPacketSize(
        EPacketType type,
        const TSharedRefArray& message,
        size_t payloadSize) = 0;

    virtual bool Start(
        EPacketType type,
        EPacketFlags flags,
        bool generateChecksums,
        int checksummedPartCount,
        TPacketId packetId,
        TSharedRefArray message) = 0;

    virtual TMutableRef GetFragment() = 0;
    virtual bool IsFragmentOwned() const = 0;

    virtual void NextFragment() = 0;

    virtual bool IsFinished() const = 0;
};

struct IPacketTranscoderFactory
{
    virtual ~IPacketTranscoderFactory() = default;

    virtual std::unique_ptr<IPacketDecoder> CreateDecoder(
        const NLogging::TLogger& logger,
        bool verifyChecksum) const = 0;
    virtual std::unique_ptr<IPacketEncoder> CreateEncoder(
        const NLogging::TLogger& logger) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IPacketTranscoderFactory* GetYTPacketTranscoderFactory(IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
