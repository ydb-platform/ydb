#include "OwnFormattingChannel.h"
#include "OwnPatternFormatter.h"


namespace DB
{

void OwnFormattingChannel::logExtended(const ExtendedLogMessage & msg)
{
    if (pChannel && priority >= msg.base.getPriority())
    {
        if (pFormatter)
        {
            std::string text;
            pFormatter->formatExtended(msg, text);
            pChannel->log(DBPoco::Message(msg.base, text));
        }
        else
        {
            pChannel->log(msg.base);
        }
    }
}

void OwnFormattingChannel::log(const DBPoco::Message & msg)
{
    logExtended(ExtendedLogMessage::getFrom(msg));
}

OwnFormattingChannel::~OwnFormattingChannel() = default;

}
