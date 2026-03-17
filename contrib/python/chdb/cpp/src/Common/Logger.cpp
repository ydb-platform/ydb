#include <CHDBPoco/Logger.h>
#include <Common/Logger.h>

LoggerPtr getLogger(const std::string & name)
{
    return CHDBPoco::Logger::getShared(name);
}

LoggerPtr createLogger(const std::string & name, CHDBPoco::Channel * channel, CHDBPoco::Message::Priority level)
{
    return CHDBPoco::Logger::createShared(name, channel, level);
}

LoggerRawPtr getRawLogger(const std::string & name)
{
    return &CHDBPoco::Logger::get(name);
}

LoggerRawPtr createRawLogger(const std::string & name, CHDBPoco::Channel * channel, CHDBPoco::Message::Priority level)
{
    return &CHDBPoco::Logger::create(name, channel, level);
}

bool hasLogger(const std::string & name)
{
    return CHDBPoco::Logger::has(name);
}

static constinit std::atomic<bool> allow_logging{true};

bool isLoggingEnabled()
{
    return allow_logging;
}

void disableLogging()
{
    allow_logging = false;
}
