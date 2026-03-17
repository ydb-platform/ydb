#pragma once


#include <CHDBPoco/PatternFormatter.h>
#include <Common/DateLUT.h>
#include "ExtendedLogChannel.h"


/** Format log messages own way.
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

class OwnPatternFormatter : public CHDBPoco::PatternFormatter
{
public:
    explicit OwnPatternFormatter(bool color_ = false);

    void format(const CHDBPoco::Message & msg, std::string & text) override;
    virtual void formatExtended(const DB_CHDB::ExtendedLogMessage & msg_ext, std::string & text) const;

private:
    const DateLUTImpl & server_timezone = DateLUT::serverTimezoneInstance();
    bool color;
};
