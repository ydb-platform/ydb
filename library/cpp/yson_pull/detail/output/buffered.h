#pragma once

#include <library/cpp/yson_pull/detail/macros.h>

#include <library/cpp/yson_pull/output.h>

#include <util/generic/strbuf.h>

namespace NYsonPull {
    namespace NDetail {
        namespace NOutput {
            template <typename T>
            class TBuffered: public NYsonPull::NOutput::IStream {
                TArrayHolder<ui8> buffer_;
                size_t size_;

            public:
                TBuffered(size_t buffer_size)
                    : buffer_{new ui8[buffer_size]}
                    , size_{buffer_size} {
                    reset_buffer();
                }

            protected:
                void do_flush_buffer(TStringBuf extra) override {
                    auto& buf = buffer();
                    if (!buf.is_empty()) {
                        do_write({reinterpret_cast<const char*>(buf.begin()), buf.used()});
                        reset_buffer();
                    }
                    if (extra.size() >= buf.available()) {
                        do_write(extra);
                    } else if (extra.size() > 0) {
                        ::memcpy(buf.pos(), extra.data(), extra.size());
                        buf.advance(extra.size());
                    }
                }

            private:
                void do_write(TStringBuf data) {
                    // CRTP dispatch
                    static_cast<T*>(this)->write(data);
                }

                void reset_buffer() {
                    buffer().reset(buffer_.Get(), buffer_.Get() + size_);
                }
            };
        }
    }     // namespace NDetail
}
