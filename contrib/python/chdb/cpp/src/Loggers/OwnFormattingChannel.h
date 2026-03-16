#pragma once
#include <atomic>
#include <CHDBPoco/AutoPtr.h>
#include <CHDBPoco/Channel.h>
#include <CHDBPoco/FormattingChannel.h>
#include "ExtendedLogChannel.h"
#include "OwnJSONPatternFormatter.h"
#include "OwnPatternFormatter.h"


namespace DB_CHDB
{
// Like CHDBPoco::FormattingChannel but supports the extended logging interface and log level filter
class OwnFormattingChannel : public CHDBPoco::Channel, public ExtendedLogChannel
{
public:
    explicit OwnFormattingChannel(
        CHDBPoco::AutoPtr<OwnPatternFormatter> pFormatter_ = nullptr, CHDBPoco::AutoPtr<CHDBPoco::Channel> pChannel_ = nullptr)
        : pFormatter(pFormatter_), pChannel(pChannel_), priority(CHDBPoco::Message::PRIO_TRACE)
    {
    }

    void setChannel(CHDBPoco::AutoPtr<CHDBPoco::Channel> pChannel_) { pChannel = pChannel_; }

    void setLevel(CHDBPoco::Message::Priority priority_) { priority = priority_; }

    // CHDBPoco::Logger::parseLevel returns ints
    void setLevel(int level) { priority = static_cast<CHDBPoco::Message::Priority>(level); }

    void open() override
    {
        if (pChannel)
            pChannel->open();
    }

    void close() override
    {
        if (pChannel)
            pChannel->close();
    }

    void setProperty(const std::string& name, const std::string& value) override
    {
        if (pChannel)
            pChannel->setProperty(name, value);
    }

    void log(const CHDBPoco::Message & msg) override;
    void logExtended(const ExtendedLogMessage & msg) override;

    ~OwnFormattingChannel() override;

private:
    CHDBPoco::AutoPtr<OwnPatternFormatter> pFormatter;
    CHDBPoco::AutoPtr<CHDBPoco::Channel> pChannel;
    std::atomic<CHDBPoco::Message::Priority> priority;
};

}
