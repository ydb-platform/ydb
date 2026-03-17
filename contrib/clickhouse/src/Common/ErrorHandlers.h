#pragma once

#include <DBPoco/ErrorHandler.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


/** ErrorHandler for DBPoco::Thread,
  *  that in case of unhandled exception,
  *  logs exception message and terminates the process.
  */
class KillingErrorHandler : public DBPoco::ErrorHandler
{
public:
    void exception(const DBPoco::Exception &) override { std::terminate(); }
    void exception(const std::exception &)  override { std::terminate(); }
    void exception()                        override { std::terminate(); }
};


/** Log exception message.
  */
class ServerErrorHandler : public DBPoco::ErrorHandler
{
public:
    void exception(const DBPoco::Exception &) override { logException(); }
    void exception(const std::exception &)  override { logException(); }
    void exception()                        override { logException(); }

    void logMessageImpl(DBPoco::Message::Priority priority, const std::string & msg) override
    {
        switch (priority)
        {
            case DBPoco::Message::PRIO_FATAL: [[fallthrough]];
            case DBPoco::Message::PRIO_CRITICAL:
                LOG_FATAL(trace_log, fmt::runtime(msg)); break;
            case DBPoco::Message::PRIO_ERROR:
                LOG_ERROR(trace_log, fmt::runtime(msg)); break;
            case DBPoco::Message::PRIO_WARNING:
                LOG_WARNING(trace_log, fmt::runtime(msg)); break;
            case DBPoco::Message::PRIO_NOTICE: [[fallthrough]];
            case DBPoco::Message::PRIO_INFORMATION:
                LOG_INFO(trace_log, fmt::runtime(msg)); break;
            case DBPoco::Message::PRIO_DEBUG:
                LOG_DEBUG(trace_log, fmt::runtime(msg)); break;
            case DBPoco::Message::PRIO_TRACE:
                LOG_TRACE(trace_log, fmt::runtime(msg)); break;
            case DBPoco::Message::PRIO_TEST:
                LOG_TEST(trace_log, fmt::runtime(msg)); break;
        }
    }

private:
    LoggerPtr log = getLogger("ServerErrorHandler");
    LoggerPtr trace_log = getLogger("Poco");

    void logException()
    {
        DB::tryLogCurrentException(log);
    }
};
