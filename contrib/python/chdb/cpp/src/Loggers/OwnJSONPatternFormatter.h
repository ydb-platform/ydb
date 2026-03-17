#pragma once


#include <CHDBPoco/PatternFormatter.h>
#include <CHDBPoco/Util/AbstractConfiguration.h>
#include "ExtendedLogChannel.h"
#include "OwnPatternFormatter.h"


/** Format log messages own way in JSON.
  * We can't obtain some details using CHDBPoco::PatternFormatter.
  *
  * Firstly, the thread number here is peaked not from CHDBPoco::Thread
  * threads only, but from all threads with number assigned (see ThreadNumber.h)
  *
  * Secondly, the local date and time are correctly displayed.
  * CHDBPoco::PatternFormatter does not work well with local time,
  * when timestamps are close to DST timeshift moments.
  * - see Poco sources and http://thread.gmane.org/gmane.comp.time.tz/8883
  *
  * Also it's made a bit more efficient (unimportant).
  */

class Loggers;

class OwnJSONPatternFormatter : public OwnPatternFormatter
{
public:
    explicit OwnJSONPatternFormatter(CHDBPoco::Util::AbstractConfiguration & config);

    void format(const CHDBPoco::Message & msg, std::string & text) override;
    void formatExtended(const DB_CHDB::ExtendedLogMessage & msg_ext, std::string & text) const override;

private:
    std::string date_time;
    std::string thread_name;
    std::string thread_id;
    std::string level;
    std::string query_id;
    std::string logger_name;
    std::string message;
    std::string source_file;
    std::string source_line;
};
