#pragma once

#include <util/system/types.h>

namespace NYsonPull {
    namespace NDetail {
        enum class number_type {
            float64,
            uint64,
            int64
        };

        struct number {
            number_type type;
            union {
                double as_float64;
                ui64 as_uint64;
                i64 as_int64;
            } value;

            number(double v) {
                type = number_type::float64;
                value.as_float64 = v;
            }

            number(i64 v) {
                type = number_type::int64;
                value.as_int64 = v;
            }

            number(ui64 v) {
                type = number_type::uint64;
                value.as_uint64 = v;
            }
        };
    }
}
