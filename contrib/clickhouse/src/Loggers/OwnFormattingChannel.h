#pragma once
#include <atomic>
#include <DBPoco/AutoPtr.h>
#include <DBPoco/Channel.h>
#include <DBPoco/FormattingChannel.h>
#include "ExtendedLogChannel.h"
#include "OwnJSONPatternFormatter.h"
#include "OwnPatternFormatter.h"


namespace DB
{
// Like DBPoco::FormattingChannel but supports the extended logging interface and log level filter
class OwnFormattingChannel : public DBPoco::Channel, public ExtendedLogChannel
{
public:
    explicit OwnFormattingChannel(
        DBPoco::AutoPtr<OwnPatternFormatter> pFormatter_ = nullptr, DBPoco::AutoPtr<DBPoco::Channel> pChannel_ = nullptr)
        : pFormatter(pFormatter_), pChannel(pChannel_), priority(DBPoco::Message::PRIO_TRACE)
    {
    }

    void setChannel(DBPoco::AutoPtr<DBPoco::Channel> pChannel_) { pChannel = pChannel_; }

    void setLevel(DBPoco::Message::Priority priority_) { priority = priority_; }

    // DBPoco::Logger::parseLevel returns ints
    void setLevel(int level) { priority = static_cast<DBPoco::Message::Priority>(level); }

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

    void log(const DBPoco::Message & msg) override;
    void logExtended(const ExtendedLogMessage & msg) override;

    ~OwnFormattingChannel() override;

private:
    DBPoco::AutoPtr<OwnPatternFormatter> pFormatter;
    DBPoco::AutoPtr<DBPoco::Channel> pChannel;
    std::atomic<DBPoco::Message::Priority> priority;
};

}
