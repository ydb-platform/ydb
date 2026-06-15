#pragma once

#include <yt/yt/core/bus/bus.h>

#include <library/cpp/yt/threading/event_count.h>

#include <atomic>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override;
};

////////////////////////////////////////////////////////////////////////////////

class TCountingBusHandler
    : public IMessageHandler
{
public:
    int GetCount() const;
    void ResetCount();

    void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override;

private:
    std::atomic<int> Count_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReplying42BusHandler
    : public IMessageHandler
{
public:
    explicit TReplying42BusHandler(int expectedPartCount);

    void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override;

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

    void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override;

private:
    std::atomic<int> RemainingReplyCount_;
    NThreading::TEvent Event_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
