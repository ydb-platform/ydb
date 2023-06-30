#pragma once

#include <util/generic/serialized_enum.h>

////////////////////////////////////////////////////////////////////////////////
// TEnumTraits interop for Arcadia enums

#define YT_DEFINE_ARCADIA_ENUM_TRAITS(enumType) \
    [[maybe_unused]] inline ::NYT::NDetail::TArcadiaEnumTraitsImpl<enumType> GetEnumTraitsImpl(enumType) \
    { \
        return {}; \
    }

////////////////////////////////////////////////////////////////////////////////

#define ARCADIA_ENUM_INL_H_
#include "arcadia_enum-inl.h"
#undef ARCADIA_ENUM_INL_H_
