#pragma once

#ifndef SKIFF_H
#error "Direct inclusion of this file is not allowed, include skiff.h"
// For the sake of sane code completion.
#include "skiff.h"
#endif
#undef SKIFF_H

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

template <EWireType wireType>
constexpr auto TUnderlyingIntegerType<wireType>::F() {
    if constexpr (wireType == EWireType::Int8) {
        return i8{};
    } else if constexpr (wireType == EWireType::Int16) {
        return i16{};
    } else if constexpr (wireType == EWireType::Int32) {
        return i32{};
    } else if constexpr (wireType == EWireType::Int64) {
        return i64{};
    } else if constexpr (wireType == EWireType::Uint8) {
        return ui8{};
    } else if constexpr (wireType == EWireType::Uint16) {
        return ui16{};
    } else if constexpr (wireType == EWireType::Uint32) {
        return ui32{};
    } else if constexpr (wireType == EWireType::Uint64) {
        return ui64{};
    } else {
        static_assert(wireType == EWireType::Int8, "expected integer wire type");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
