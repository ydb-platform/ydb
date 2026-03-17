#include <DBPoco/Logger.h>
#include <Common/Logger.h>

LoggerPtr getLogger(const std::string & name)
{
    return DBPoco::Logger::getShared(name);
}

LoggerPtr createLogger(const std::string & name, DBPoco::Channel * channel, DBPoco::Message::Priority level)
{
    return DBPoco::Logger::createShared(name, channel, level);
}

LoggerRawPtr getRawLogger(const std::string & name)
{
    return &DBPoco::Logger::get(name);
}

LoggerRawPtr createRawLogger(const std::string & name, DBPoco::Channel * channel, DBPoco::Message::Priority level)
{
    return &DBPoco::Logger::create(name, channel, level);
}

bool hasLogger(const std::string & name)
{
    return DBPoco::Logger::has(name);
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
