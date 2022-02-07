#pragma once

#include "macros.h"

#include <library/cpp/yson_pull/output.h>

#include <util/system/types.h>

#include <cstddef>
#include <cstring>

namespace NYsonPull {
    namespace NDetail {
        template <class StreamCounter>
        class byte_writer {
            NYsonPull::NOutput::IStream& stream_;
            StreamCounter stream_counter_;

        public:
            byte_writer(NYsonPull::NOutput::IStream& stream)
                : stream_(stream)
            {
            }

            // const-ness added to prevent direct stream mutation
            const NYsonPull::NOutput::IStream& stream() {
                return stream_;
            }
            const StreamCounter& counter() {
                return stream_counter_;
            }

            void flush_buffer() {
                stream_.flush_buffer();
            }

            void advance(size_t bytes) {
                auto& buf = stream_.buffer();
                stream_counter_.update(
                    buf.pos(),
                    buf.pos() + bytes);
                buf.advance(bytes);
            }

            void write(ui8 c) {
                auto& buf = stream_.buffer();
                if (Y_LIKELY(!buf.is_full())) {
                    *buf.pos() = c;
                    advance(1);
                } else {
                    auto ptr = reinterpret_cast<char*>(&c);
                    stream_counter_.update(&c, &c + 1);
                    stream_.flush_buffer({ptr, 1});
                }
            }

            void write(const ui8* data, size_t size) {
                auto& buf = stream_.buffer();
                auto free_buf = buf.available();
                if (Y_LIKELY(size < free_buf)) {
                    ::memcpy(buf.pos(), data, size);
                    advance(size);
                } else {
                    if (!buf.is_full()) {
                        ::memcpy(buf.pos(), data, free_buf);
                        advance(free_buf);
                        data += free_buf;
                        size -= free_buf;
                    }
                    stream_counter_.update(data, data + size);
                    stream_.flush_buffer({reinterpret_cast<const char*>(data),
                                          size});
                }
            }
        };
    }
}
