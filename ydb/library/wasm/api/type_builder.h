#pragma once

#include "public.h"

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWebAssemblyValueType,
    ((UintPtr) (0))
    ((Int64)   (1))
    ((Int32)   (2))
    ((Float64) (3))
    ((Float32) (4))
    ((Void)    (5))
);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TWebAssemblyRuntimeType, void*);

template <bool IsIntrinsic, class TSignature>
struct TFunctionTypeBuilder
{ };

template <bool IsIntrinsic, typename TResult, typename... TArguments>
struct TFunctionTypeBuilder<IsIntrinsic, TResult(TArguments...)>
{
    static TWebAssemblyRuntimeType Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm

#define WASM_TYPE_BUILDER_INL_H_
#include "type_builder-inl.h"
#undef WASM_TYPE_BUILDER_INL_H_
