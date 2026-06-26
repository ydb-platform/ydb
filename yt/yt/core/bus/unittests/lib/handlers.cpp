#include "handlers.h"

#include "helpers.h"

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/direct_placement_transfer.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

void TEmptyBusHandler::HandleMessage(
    TSharedRefArray /*message*/,
    IBusPtr /*replyBus*/,
    IDirectPlacementTransferPtr /*transfer*/) noexcept
{ }

////////////////////////////////////////////////////////////////////////////////

int TCountingBusHandler::GetCount() const
{
    return Count_.load();
}

void TCountingBusHandler::ResetCount()
{
    Count_ = 0;
}

void TCountingBusHandler::HandleMessage(
    TSharedRefArray /*message*/,
    IBusPtr /*replyBus*/,
    IDirectPlacementTransferPtr /*transfer*/) noexcept
{
    ++Count_;
}

////////////////////////////////////////////////////////////////////////////////

TReplying42BusHandler::TReplying42BusHandler(int expectedPartCount)
    : ExpectedPartCount_(expectedPartCount)
{ }

void TReplying42BusHandler::HandleMessage(
    TSharedRefArray message,
    IBusPtr replyBus,
    IDirectPlacementTransferPtr /*transfer*/) noexcept
{
    EXPECT_EQ(ExpectedPartCount_, std::ssize(message));
    auto replyMessage = Serialize("42");
    YT_UNUSED_FUTURE(replyBus->Send(replyMessage));
}

////////////////////////////////////////////////////////////////////////////////

TChecking42BusHandler::TChecking42BusHandler(int expectedReplyCount)
    : RemainingReplyCount_(expectedReplyCount)
{ }

void TChecking42BusHandler::WaitUntilDone()
{
    Event_.Wait();
}

void TChecking42BusHandler::HandleMessage(
    TSharedRefArray message,
    IBusPtr /*replyBus*/,
    IDirectPlacementTransferPtr /*transfer*/) noexcept
{
    auto value = Deserialize(message);
    EXPECT_EQ("42", value);

    if (--RemainingReplyCount_ == 0) {
        Event_.NotifyAll();
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> TDirectPlacementBusHandler::WaitForReceivedParts()
{
    Event_.Wait();
    return ReceivedParts_;
}

std::vector<i64> TDirectPlacementBusHandler::WaitForReceivedPartSizes()
{
    Event_.Wait();
    std::vector<i64> partSizes;
    partSizes.reserve(ReceivedParts_.size());
    for (const auto& part : ReceivedParts_) {
        partSizes.push_back(std::ssize(part));
    }
    return partSizes;
}

bool TDirectPlacementBusHandler::SawDirectPlacementTransfer() const
{
    return SawTransfer_.load();
}

void TDirectPlacementBusHandler::HandleMessage(
    TSharedRefArray message,
    IBusPtr /*replyBus*/,
    IDirectPlacementTransferPtr transfer) noexcept
{
    SawTransfer_.store(transfer != nullptr);

    // The eager (or, on the inline/compat path, complete) parts; null parts are
    // preserved as null refs.
    std::vector<TSharedRef> parts(message.Begin(), message.End());

    if (!transfer) {
        // The transport delivered every part inline (no direct placement support or
        // none requested).
        Finish(std::move(parts));
        return;
    }

    // Direct placement path: the head parts arrived inline in `message`; allocate a
    // destination buffer for each trailing part and pull them in.
    struct TDirectPlacementBufferTag { };
    auto expectedSizes = transfer->GetExpectedBufferSizes();
    std::vector<TSharedMutableRef> buffers;
    buffers.reserve(expectedSizes.size());
    for (auto size : expectedSizes) {
        buffers.push_back(TSharedMutableRef::Allocate<TDirectPlacementBufferTag>(size));
    }

    transfer->Run(std::move(buffers)).Subscribe(BIND(
        [this_ = MakeStrong(this), parts = std::move(parts)] (const TErrorOr<std::vector<TSharedRef>>& result) mutable {
            EXPECT_TRUE(result.IsOK());
            for (const auto& part : result.Value()) {
                parts.push_back(part);
            }
            this_->Finish(std::move(parts));
        }));
}

void TDirectPlacementBusHandler::Finish(std::vector<TSharedRef> parts)
{
    ReceivedParts_ = std::move(parts);
    Event_.NotifyAll();
}

////////////////////////////////////////////////////////////////////////////////

void TDirectPlacementRunBusHandler::WaitForHandled()
{
    Event_.Wait();
}

TFuture<void> TDirectPlacementRunBusHandler::GetRunFuture()
{
    auto guard = Guard(Lock_);
    return RunFuture_;
}

void TDirectPlacementRunBusHandler::HandleMessage(
    TSharedRefArray /*message*/,
    IBusPtr /*replyBus*/,
    IDirectPlacementTransferPtr transfer) noexcept
{
    TFuture<void> future;
    if (transfer) {
        struct TDirectPlacementRunTag { };
        auto sizes = transfer->GetExpectedBufferSizes();
        std::vector<TSharedMutableRef> buffers;
        buffers.reserve(sizes.size());
        for (auto size : sizes) {
            buffers.push_back(TSharedMutableRef::Allocate<TDirectPlacementRunTag>(size, {.InitializeStorage = false}));
        }
        // The transfer takes ownership of the landing buffers and keeps them
        // alive until it resolves.
        future = transfer->Run(std::move(buffers)).AsVoid();
    } else {
        future = OKFuture;
    }

    {
        auto guard = Guard(Lock_);
        RunFuture_ = std::move(future);
    }
    Event_.NotifyAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
