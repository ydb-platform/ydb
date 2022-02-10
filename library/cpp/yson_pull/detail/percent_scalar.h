#pragma once

#include <util/generic/strbuf.h>

namespace NYsonPull::NDetail {
    enum class percent_scalar_type {
        boolean,
        float64
    };

    struct percent_scalar {
        //! Text boolean literals
        static constexpr TStringBuf true_literal = "%true";
        static constexpr TStringBuf false_literal = "%false";
        //! Text floating-point literals
        static constexpr TStringBuf nan_literal = "%nan";
        static constexpr TStringBuf positive_inf_literal = "%inf";
        static constexpr TStringBuf negative_inf_literal = "%-inf";

        percent_scalar_type type;
        union {
            double as_float64;
            bool as_boolean;
        } value;

        percent_scalar(double v) {
            type = percent_scalar_type::float64;
            value.as_float64 = v;
        }

        percent_scalar(bool v) {
            type = percent_scalar_type::boolean;
            value.as_boolean = v;
        }
    };
}
