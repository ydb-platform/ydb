#pragma once

#include "cescape.h"
#include "fail.h"
#include "stream_counter.h"

#include <library/cpp/yson_pull/input.h>

namespace NYsonPull {
    namespace NDetail {
        template <class StreamCounter>
        class byte_reader {
            NYsonPull::NInput::IStream& stream_;
            StreamCounter stream_counter_;

        public:
            byte_reader(NYsonPull::NInput::IStream& stream)
                : stream_(stream)
            {
            }

            // const-ness added to prevent direct stream mutation
            const NYsonPull::NInput::IStream& stream() {
                return stream_;
            }

            template <typename... Args>
            ATTRIBUTE(noinline, cold)
            void fail[[noreturn]](const char* msg, Args&&... args) {
                NYsonPull::NDetail::fail(
                    stream_counter_.info(),
                    msg,
                    std::forward<Args>(args)...);
            }

            template <bool AllowFinish>
            void fill_buffer() {
                stream_.fill_buffer();

                if (!AllowFinish) {
                    auto& buf = stream_.buffer();
                    if (Y_UNLIKELY(buf.is_empty() && stream_.at_end())) {
                        fail("Premature end of stream");
                    }
                }
            }

            void fill_buffer() {
                return fill_buffer<true>();
            }

            template <bool AllowFinish>
            ui8 get_byte() {
                fill_buffer<AllowFinish>();
                auto& buf = stream_.buffer();
                return !buf.is_empty()
                           ? *buf.pos()
                           : ui8{'\0'};
            }

            ui8 get_byte() {
                return get_byte<true>();
            }

            void advance(size_t bytes) {
                auto& buf = stream_.buffer();
                stream_counter_.update(
                    buf.pos(),
                    buf.pos() + bytes);
                buf.advance(bytes);
            }
        };
    }
}
