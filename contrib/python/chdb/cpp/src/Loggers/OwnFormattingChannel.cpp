#include "OwnFormattingChannel.h"
#include "OwnPatternFormatter.h"


namespace DB_CHDB
{

void OwnFormattingChannel::logExtended(const ExtendedLogMessage & msg)
{
    if (pChannel && priority >= msg.base.getPriority())
    {
        if (pFormatter)
        {
            std::string text;
            pFormatter->formatExtended(msg, text);
            pChannel->log(CHDBPoco::Message(msg.base, text));
        }
        else
        {
            pChannel->log(msg.base);
        }
    }
}

void OwnFormattingChannel::log(const CHDBPoco::Message & msg)
{
    logExtended(ExtendedLogMessage::getFrom(msg));
}

OwnFormattingChannel::~OwnFormattingChannel() = default;

}
