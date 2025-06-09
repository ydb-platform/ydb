#pragma once

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {

template<bool Embedded>
void NativeToUnboxed(const ui64 value, NUdf::TUnboxedValuePod& result)
{
    result = NUdf::TUnboxedValuePod(value);
}

template<bool Embedded>
void NativeToUnboxed(const std::string& value, NUdf::TUnboxedValuePod& result)
{
    if constexpr (Embedded) {
        result = NUdf::TUnboxedValuePod::Embedded(value);
    } else {
        result = NUdf::TUnboxedValuePod(NUdf::TStringValue(value));
    }
}

template<typename T>
T UnboxedToNative(const NUdf::TUnboxedValue& result)
{
    return result.template Get<T>();
}

template<>
std::string UnboxedToNative(const NUdf::TUnboxedValue& result);

}
}
