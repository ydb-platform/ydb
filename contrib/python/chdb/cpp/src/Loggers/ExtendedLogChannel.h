#pragma once
#include <string>

namespace CHDBPoco
{
class Message;
}

namespace DB_CHDB
{
/// CHDBPoco::Message with more ClickHouse-specific info
/// NOTE: CHDBPoco::Message is not polymorphic class, so we can't use inheritance in couple with dynamic_cast<>()
class ExtendedLogMessage
{
public:
    explicit ExtendedLogMessage(const CHDBPoco::Message & base_) : base(base_) {}

    /// Attach additional data to the message
    static ExtendedLogMessage getFrom(const CHDBPoco::Message & base);

    // Do not copy for efficiency reasons
    const CHDBPoco::Message & base;

    uint32_t time_seconds = 0;
    uint32_t time_microseconds = 0;
    uint64_t time_in_microseconds = 0;

    uint64_t thread_id = 0;
    std::string query_id;
};


/// Interface extension of CHDBPoco::Channel
class ExtendedLogChannel
{
public:
    virtual void logExtended(const ExtendedLogMessage & msg) = 0;
    virtual ~ExtendedLogChannel() = default;
};

}
