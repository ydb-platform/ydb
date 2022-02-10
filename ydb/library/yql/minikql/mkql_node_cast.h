#pragma once

#include "mkql_node.h"

#include <util/system/src_location.h>


#define AS_VALUE(type, node) ::NKikimr::NMiniKQL::AsValue<type>((node), __LOCATION__)
#define AS_TYPE(type, node) ::NKikimr::NMiniKQL::AsType<type>((node), __LOCATION__)
#define AS_CALLABLE(name, node) ::NKikimr::NMiniKQL::AsCallable(TStringBuf(name), node, __LOCATION__)


namespace NKikimr {
namespace NMiniKQL {

template <typename T>
T* AsValue(TRuntimeNode node, const TSourceLocation& location);

template <typename T>
T* AsType(TType* type, const TSourceLocation& location);

template <typename T>
const T* AsType(const TType* type, const TSourceLocation& location);


template <typename T>
T* AsType(TRuntimeNode node, const TSourceLocation& location) {
    return AsType<T>(node.GetStaticType(), location);
}

TCallable* AsCallable(
        const TStringBuf& name,
        TRuntimeNode node,
        const TSourceLocation& location);

} // namespace NMiniKQL
} // namespace NKikimr
