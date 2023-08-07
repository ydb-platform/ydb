#pragma once

#include <cstdint>
#include <cstddef>

/*
    This file presents implementation of CRC64 with polynomial
    that was previously specified in YT (0xE543279765927881).

    Implementation is based on one from ISA-l (contrib/libs/isa-l/crc) with assembly and lots of pclmulqdq.
    In ISA-l there are 6 of them: (iso poly, ecma poly, jones poly) x (normal form, reflected form).

    In order to support our own polynomial, we had to patch their asm file. There are few details to mention:
        1) The implementation, supported in YT before, was one with normal form.

        2) All 3 normal form implementations only differ in rk01, ... rk20 constants.
    Their meaning is described in `crc64_yt_norm_by8.asm`. You may also refer to https://github.com/intel/isa-l/issues/88.
    If you want to generate such constants for your polynomial utilities/caclulations_for_matlab.py may be helpful.
*/

#ifdef __x86_64__
    #define ISA_CRC64_FAST_IMPL_SUPPORTED
#endif

namespace NYT::NIsaCrc64 {

////////////////////////////////////////////////////////////////////////////////

#ifdef ISA_CRC64_FAST_IMPL_SUPPORTED
uint64_t CrcImplFast(const void* data, size_t length, uint64_t seed);
#endif

uint64_t CrcImplBase(const void* data, size_t length, uint64_t seed);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIsaCrc64
