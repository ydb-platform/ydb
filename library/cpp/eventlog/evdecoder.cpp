#include <util/memory/tempbuf.h>
#include <util/string/cast.h>
#include <util/stream/output.h>

#include "evdecoder.h"
#include "logparser.h"

static const char* const UNKNOWN_EVENT_CLASS = "Unknown event class";

static inline void LogError(ui64 frameAddr, const char* msg, bool strict) {
    if (!strict) {
        Cerr << "EventDecoder warning @" << frameAddr << ": " << msg << Endl;
    } else {
        ythrow yexception() << "EventDecoder error @" << frameAddr << ": " << msg;
    }
}

static inline bool SkipData(IInputStream& s, size_t amount) {
    return (amount == s.Skip(amount));
}

// There are 2 log fomats: the one, that allows event skip without event decode (it has stored event length)
// and another, that requires each event decode just to seek over stream. needRead == true means the latter format.
static inline THolder<TEvent> DoDecodeEvent(IInputStream& s, const TEventFilter* const filter, const bool needRead, IEventFactory* fac) {
    TEventTimestamp ts;
    TEventClass c;
    THolder<TEvent> e;

    ::Load(&s, ts);
    ::Load(&s, c);

    bool needReturn = false;

    if (!filter || filter->EventAllowed(c)) {
        needReturn = true;
    }

    if (needRead || needReturn) {
        e.Reset(fac->CreateLogEvent(c));

        if (!!e) {
            e->Timestamp = ts;
            e->Load(s);
        } else if (needReturn) {
            e.Reset(new TUnknownEvent(ts, c));
        }

        if (!needReturn) {
            e.Reset(nullptr);
        }
    }

    return e;
}

THolder<TEvent> DecodeFramed(IInputStream& inp, ui64 frameAddr, const TEventFilter* const filter, IEventFactory* fac, bool strict) {
    ui32 len;
    ::Load(&inp, len);

    if (len < sizeof(ui32)) {
        ythrow TEventDecoderError() << "invalid event length";
    }

    TLengthLimitedInput s(&inp, len - sizeof(ui32));

    try {
        THolder<TEvent> e = DoDecodeEvent(s, filter, false, fac);
        if (!!e) {
            if (!s.Left()) {
                return e;
            } else if (e->Class == 0) {
                if (!SkipData(s, s.Left())) {
                    ythrow TEventDecoderError() << "cannot skip bad event";
                }

                return e;
            }

            LogError(frameAddr, "Event is not fully read", strict);
        }
    } catch (const TLoadEOF&) {
        if (s.Left()) {
            throw;
        }

        LogError(frameAddr, "Unexpected event end", strict);
    }

    if (!SkipData(s, s.Left())) {
        ythrow TEventDecoderError() << "cannot skip bad event";
    }

    return nullptr;
}

THolder<TEvent> DecodeEvent(IInputStream& s, bool framed, ui64 frameAddr, const TEventFilter* const filter, IEventFactory* fac, bool strict) {
    try {
        if (framed) {
            return DecodeFramed(s, frameAddr, filter, fac, strict);
        } else {
            THolder<TEvent> e = DoDecodeEvent(s, filter, true, fac);
            // e(0) means event, skipped by filter. Not an error.
            if (!!e && !e->Class) {
                ythrow TEventDecoderError() << UNKNOWN_EVENT_CLASS;
            }

            return e;
        }
    } catch (const TLoadEOF&) {
        ythrow TEventDecoderError() << "unexpected frame end";
    }
}
