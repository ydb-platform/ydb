#pragma once

#include <optional>
#include <string>
#include <CHDBPoco/AutoPtr.h>
#include <CHDBPoco/FileChannel.h>
#include <CHDBPoco/Util/Application.h>
#include "OwnSplitChannel.h"


namespace CHDBPoco::Util
{
    class AbstractConfiguration;
}

class Loggers
{
public:
    void buildLoggers(CHDBPoco::Util::AbstractConfiguration & config, CHDBPoco::Logger & logger, const std::string & cmd_name = "");

    void updateLevels(CHDBPoco::Util::AbstractConfiguration & config, CHDBPoco::Logger & logger);

    /// Close log files. On next log write files will be reopened.
    void closeLogs(CHDBPoco::Logger & logger);

    virtual ~Loggers() = default;

protected:
    virtual bool allowTextLog() const { return true; }

private:
    CHDBPoco::AutoPtr<CHDBPoco::FileChannel> log_file;
    CHDBPoco::AutoPtr<CHDBPoco::FileChannel> error_log_file;
    CHDBPoco::AutoPtr<CHDBPoco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::optional<std::string> config_logger;


    CHDBPoco::AutoPtr<DB_CHDB::OwnSplitChannel> split;
};
