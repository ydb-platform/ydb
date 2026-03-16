#include "OwnPatternFormatter.h"

#include <functional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/HashTable/Hash.h>
#include <base/terminalColors.h>


OwnPatternFormatter::OwnPatternFormatter(bool color_)
    : CHDBPoco::PatternFormatter(""), color(color_)
{
}


void OwnPatternFormatter::formatExtended(const DB_CHDB::ExtendedLogMessage & msg_ext, std::string & text) const
{
    DB_CHDB::WriteBufferFromString wb(text);

    const CHDBPoco::Message & msg = msg_ext.base;

    /// Change delimiters in date for compatibility with old logs.
    DB_CHDB::writeDateTimeText<'.', ':'>(msg_ext.time_seconds, wb, server_timezone);

    DB_CHDB::writeChar('.', wb);
    DB_CHDB::writeChar('0' + ((msg_ext.time_microseconds / 100000) % 10), wb);
    DB_CHDB::writeChar('0' + ((msg_ext.time_microseconds / 10000) % 10), wb);
    DB_CHDB::writeChar('0' + ((msg_ext.time_microseconds / 1000) % 10), wb);
    DB_CHDB::writeChar('0' + ((msg_ext.time_microseconds / 100) % 10), wb);
    DB_CHDB::writeChar('0' + ((msg_ext.time_microseconds / 10) % 10), wb);
    DB_CHDB::writeChar('0' + ((msg_ext.time_microseconds / 1) % 10), wb);

    writeCString(" [ ", wb);
    if (color)
        writeString(setColor(intHash64(msg_ext.thread_id)), wb);
    DB_CHDB::writeIntText(msg_ext.thread_id, wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString(" ] ", wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.
    writeCString("{", wb);
    if (color)
        writeString(setColor(std::hash<std::string>()(msg_ext.query_id)), wb);
    DB_CHDB::writeString(msg_ext.query_id, wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString("} ", wb);

    writeCString("<", wb);
    int priority = static_cast<int>(msg.getPriority());
    if (color)
        writeCString(setColorForLogPriority(priority), wb);
    DB_CHDB::writeString(getPriorityName(priority), wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString("> ", wb);
    if (color)
        writeString(setColor(std::hash<std::string>()(msg.getSource())), wb);
    DB_CHDB::writeString(msg.getSource(), wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString(": ", wb);
    DB_CHDB::writeString(msg.getText(), wb);
}

void OwnPatternFormatter::format(const CHDBPoco::Message & msg, std::string & text)
{
    formatExtended(DB_CHDB::ExtendedLogMessage::getFrom(msg), text);
}
