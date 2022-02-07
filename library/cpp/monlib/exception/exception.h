#pragma once


namespace NMonitoring {

#define MONLIB_ENSURE_EX(CONDITION, THROW_EXPRESSION) \
    do {                                           \
        if (Y_UNLIKELY(!(CONDITION))) {            \
            throw THROW_EXPRESSION;                \
        }                                          \
    } while (false)

} // namespace NSolomon
