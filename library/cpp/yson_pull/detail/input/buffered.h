#pragma once

#include <library/cpp/yson_pull/detail/macros.h>

#include <library/cpp/yson_pull/exceptions.h>
#include <library/cpp/yson_pull/input.h>

#include <cstdio>
#include <memory>

namespace NYsonPull {
    namespace NDetail {
        namespace NInput {
            class TBuffered: public NYsonPull::NInput::IStream {
                TArrayHolder<ui8> buffer_;
                size_t size_;

            public:
                explicit TBuffered(size_t buffer_size)
                    : buffer_{new ui8[buffer_size]}
                    , size_{buffer_size} {
                }

            protected:
                ui8* buffer_data() const {
                    return buffer_.Get();
                }

                size_t buffer_size() const {
                    return size_;
                }
            };
        }
    }     // namespace NDetail
}
