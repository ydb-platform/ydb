#pragma once

#include "reader.h"

#include <util/generic/iterator.h>

namespace NYsonPull {
    class TStreamEventsRange: public TInputRangeAdaptor<TStreamEventsRange> {
        TReader Reader_;
        bool AtEnd;

    public:
        TStreamEventsRange(THolder<NInput::IStream> stream, EStreamType mode)
            : Reader_{std::move(stream), mode}
            , AtEnd(false)
        {
        }

        const TEvent* Last() const noexcept {
            return &Reader_.LastEvent();
        }

        const TEvent* Next() {
            if (Y_UNLIKELY(AtEnd)) {
                return nullptr;
            }

            auto* event = &Reader_.NextEvent();
            if (event->Type() == EEventType::EndStream) {
                AtEnd = true;
            }
            return event;
        }
    };
}
