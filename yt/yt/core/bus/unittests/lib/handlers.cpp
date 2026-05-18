#include "handlers.h"

#include "helpers.h"

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

void TEmptyBusHandler::HandleMessage(
    TSharedRefArray /*message*/,
    IBusPtr /*replyBus*/) noexcept
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
    IBusPtr /*replyBus*/) noexcept
{
    ++Count_;
}

////////////////////////////////////////////////////////////////////////////////

TReplying42BusHandler::TReplying42BusHandler(int expectedPartCount)
    : ExpectedPartCount_(expectedPartCount)
{ }

void TReplying42BusHandler::HandleMessage(
    TSharedRefArray message,
    IBusPtr replyBus) noexcept
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
    IBusPtr /*replyBus*/) noexcept
{
    auto value = Deserialize(message);
    EXPECT_EQ("42", value);

    if (--RemainingReplyCount_ == 0) {
        Event_.NotifyAll();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
