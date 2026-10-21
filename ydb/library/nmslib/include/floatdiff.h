// -*- c++ -*-
//
// Copyright 2005, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Authors: Zhanyong Wan, Sean Mcafee
//
// Taken from The Google C++ Testing Framework (Google Test).
// Modified for the discussion by Fred Richards:
// http://www.working-software.com/node/35
//
// Leo(nid) Boytsov slightly improved the class by 
// 1) Making ULP_diff public
// 2) Allowing to specify the maximum number in ULPs to be considered equal
//    (and removing the default maxUlp)
 
#ifndef FLOATING_POINT_DIFF_H
#define FLOATING_POINT_DIFF_H
 
#include <cstdint>
#include <limits>
 
 
namespace
{
    template <size_t bytes>
    struct TypeWithSize
    {
        typedef void UInt;
    };
 
    template <>
    struct TypeWithSize<4>
    {
        typedef uint32_t UInt;
    };
 
    template <>
    struct TypeWithSize<8>
    {
        typedef uint64_t UInt;
    };
}
 
 
template <typename RawType>
class FloatingPointDiff
{
public:
    typedef typename TypeWithSize<sizeof(RawType)>::UInt Bits;
 
    static const size_t kBitCount = 8*sizeof(RawType);
    static const size_t kFracBitCount = std::numeric_limits<RawType>::digits - 1;
    static const size_t kExpBitCount = kBitCount - 1 - kFracBitCount;
 
    static const Bits kSignBitMask = static_cast<Bits>(1) << (kBitCount - 1);
    static const Bits kFracBitMask = ~static_cast<Bits>(0) >> (kExpBitCount + 1);
    static const Bits kExpBitMask = ~(kSignBitMask | kFracBitMask);
 
    explicit FloatingPointDiff(const RawType& x) : value_(x) {}
 
    //
    // Now checking for NaN to match == behavior.
    //
    bool AlmostEquals(const FloatingPointDiff& rhs, size_t maxUlps) const {
        if (is_nan() || rhs.is_nan()) return false;
        return ULP_diff(bits_, rhs.bits_) <= maxUlps;
    }

    Bits ULP_diff(const FloatingPointDiff& rhs) const {
        return ULP_diff(bits_, rhs.bits_);
    }
 
    bool is_nan() const {
        return ((kExpBitMask & bits_) == kExpBitMask) &&
	        ((kFracBitMask & bits_) != 0);
    }
 
private:
    Bits SignAndMagnitudeToBiased(const Bits& sam) const {
        if (kSignBitMask & sam) {
            return ~sam + 1;  // two's complement
        } else {
            return kSignBitMask | sam;  // * 2
        }
    }
 
    Bits ULP_diff(const Bits& sam1, const Bits& sam2) const
    {
        const Bits biased1 = SignAndMagnitudeToBiased(sam1);
        const Bits biased2 = SignAndMagnitudeToBiased(sam2);
 
        return (biased1 >= biased2) ? (biased1 - biased2) : (biased2 - biased1);
    }
 
    union {
        RawType value_;
        Bits bits_;
    };
};
 
#endif // FLOATING_POINT_DIFF_H
