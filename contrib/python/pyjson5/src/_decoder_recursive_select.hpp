#ifndef JSON5EncoderCpp_decoder_recursive_select
#define JSON5EncoderCpp_decoder_recursive_select

// GENERATED FILE
// All changes will be lost.

#include <cstdint>

namespace JSON5EncoderCpp {
inline namespace {

enum DrsKind : std::uint8_t {
    DRS_fail, DRS_null, DRS_true, DRS_false, DRS_inf, DRS_nan, DRS_string, DRS_number, DRS_recursive
};

static const DrsKind drs_lookup[128] = {
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_string, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_string,
    DRS_fail, DRS_fail, DRS_fail, DRS_number, DRS_fail, DRS_number, DRS_number, DRS_fail,
    DRS_number, DRS_number, DRS_number, DRS_number, DRS_number, DRS_number, DRS_number, DRS_number,
    DRS_number, DRS_number, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_inf, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_nan, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_recursive, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_false, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_null, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_fail, DRS_true, DRS_fail, DRS_fail, DRS_fail,
    DRS_fail, DRS_fail, DRS_fail, DRS_recursive, DRS_fail, DRS_fail, DRS_fail, DRS_fail,
};

}  // anonymous inline namespace
}  // namespace JSON5EncoderCpp

#endif
