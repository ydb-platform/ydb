#pragma once

#include <yt/yt/core/bus/message_handler.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/threading/event_count.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>
#include <vector>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer,
        TPacketId packetId) noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TCountingBusHandler
    : public IMessageHandler
{
public:
    int GetCount() const;
    void ResetCount();

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer,
        TPacketId packetId) noexcept override;

private:
    std::atomic<int> Count_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReplying42BusHandler
    : public IMessageHandler
{
public:
    explicit TReplying42BusHandler(int expectedPartCount);

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer,
        TPacketId packetId) noexcept override;

private:
    const int ExpectedPartCount_;
};

////////////////////////////////////////////////////////////////////////////////

class TChecking42BusHandler
    : public IMessageHandler
{
public:
    explicit TChecking42BusHandler(int expectedReplyCount);

    void WaitUntilDone();

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer,
        TPacketId packetId) noexcept override;

private:
    std::atomic<int> RemainingReplyCount_;
    NThreading::TEvent Event_;
};

////////////////////////////////////////////////////////////////////////////////

//! Reassembles an incoming message regardless of whether its trailing parts are
//! delivered inline or via a direct placement transfer. On the direct placement path it
//! exercises the full receiver-side API: queries the expected sizes, allocates
//! destination buffers and runs the transfer.
class TDirectPlacementBusHandler
    : public IMessageHandler
{
public:
    //! Blocks until a message has been fully received (including any direct placement
    //! tail) and returns all reassembled parts, in order (null parts preserved).
    std::vector<TSharedRef> WaitForReceivedParts();

    //! Blocks until a message has been fully received and returns the sizes of all
    //! reassembled parts, in order (a null part reports size zero).
    std::vector<i64> WaitForReceivedPartSizes();

    //! Whether the most recently handled message arrived with a non-null
    //! transfer (i.e. via the direct placement path rather than inline).
    bool SawDirectPlacementTransfer() const;

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer,
        TPacketId packetId) noexcept override;

private:
    std::atomic<bool> SawTransfer_ = false;
    NThreading::TEvent Event_;
    std::vector<TSharedRef> ReceivedParts_;

    void Finish(std::vector<TSharedRef> parts);
};

////////////////////////////////////////////////////////////////////////////////

//! Calls Run on the incoming direct placement transfer and exposes the resulting
//! future without waiting for it. Lets a test tear the connection down while a
//! direct placement receive is in flight and assert the future still resolves.
class TDirectPlacementRunBusHandler
    : public IMessageHandler
{
public:
    //! Blocks until a message has been handled and (for a direct placement message)
    //! Run has been called.
    void WaitForHandled();

    //! The future returned by the last Run call (or an already-set future for an
    //! inline message).
    TFuture<void> GetRunFuture();

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer,
        TPacketId packetId) noexcept override;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    NThreading::TEvent Event_;
    TFuture<void> RunFuture_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
