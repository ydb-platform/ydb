#pragma once

#include "consumer.h"
#include "event.h"
#include "writer.h"

namespace NYsonPull {
    //! \brief Connect YSON stream producer and consumer.
    //!
    //! Useful for writing YSON stream filters.
    //! \p Producer must have a \p next_event() method (like \p NYsonPull::reader).
    //! \p Consumer must be like \p NYsonPull::consumer interface.
    template <typename Producer, typename Consumer>
    inline void Bridge(Producer&& producer, Consumer&& consumer) {
        for (;;) {
            auto& event = producer.NextEvent();
            consumer.OnEvent(event);
            if (event.Type() == EEventType::EndStream) {
                break;
            }
        }
    }

    template <typename Producer>
    inline void Bridge(Producer&& producer, TWriter& writer_) {
        Bridge(std::forward<Producer>(producer), writer_.GetConsumer());
    }

    template <typename Producer>
    inline void Bridge(Producer&& producer, TWriter&& writer_) {
        Bridge(std::forward<Producer>(producer), writer_.GetConsumer());
    }

}
