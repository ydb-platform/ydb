#pragma once

#include <optional>
#include <string>
#include <DBPoco/AutoPtr.h>
#include <DBPoco/FileChannel.h>
#include <DBPoco/Util/Application.h>
#include "OwnSplitChannel.h"


namespace DBPoco::Util
{
    class AbstractConfiguration;
}

class Loggers
{
public:
    void buildLoggers(DBPoco::Util::AbstractConfiguration & config, DBPoco::Logger & logger, const std::string & cmd_name = "");

    void updateLevels(DBPoco::Util::AbstractConfiguration & config, DBPoco::Logger & logger);

    /// Close log files. On next log write files will be reopened.
    void closeLogs(DBPoco::Logger & logger);

    virtual ~Loggers() = default;

protected:
    virtual bool allowTextLog() const { return true; }

private:
    DBPoco::AutoPtr<DBPoco::FileChannel> log_file;
    DBPoco::AutoPtr<DBPoco::FileChannel> error_log_file;
    DBPoco::AutoPtr<DBPoco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::optional<std::string> config_logger;


    DBPoco::AutoPtr<DB::OwnSplitChannel> split;
};
