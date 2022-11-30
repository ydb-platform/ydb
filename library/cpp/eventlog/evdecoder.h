#pragma once

#include <util/generic/yexception.h>
#include <util/generic/ptr.h>

#include "eventlog.h"

class TEvent;
class IInputStream;
class TEventFilter;

struct TEventDecoderError: public yexception {
};

THolder<TEvent> DecodeEvent(IInputStream& s, bool framed, ui64 frameAddr, const TEventFilter* const filter, IEventFactory* fac, bool strict = false);
bool AcceptableContent(TEventLogFormat);
