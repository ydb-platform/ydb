#pragma once

#include <library/cpp/yson_pull/position_info.h>

#include <cstddef>

namespace NYsonPull {
    namespace NDetail {
        template <bool EnableLinePositionInfo>
        class stream_counter;

        template <>
        class stream_counter<true> {
        private:
            size_t offset_ = 0;
            size_t line_ = 1;
            size_t column_ = 1;

        public:
            TPositionInfo info() const {
                return {offset_, line_, column_};
            }

            void update(const ui8* begin, const ui8* end) {
                offset_ += end - begin;
                for (auto current = begin; current != end; ++current) {
                    ++column_;
                    if (*current == '\n') { //TODO: memchr
                        ++line_;
                        column_ = 1;
                    }
                }
            }
        };

        template <>
        class stream_counter<false> {
        private:
            size_t offset_ = 0;

        public:
            TPositionInfo info() const {
                return {offset_, {}, {}};
            }

            void update(const ui8* begin, const ui8* end) {
                offset_ += end - begin;
            }
        };
    }
}
