#pragma once

#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

struct TValidateErrorPolicyNone;
struct TValidateErrorPolicyThrow;
struct TValidateErrorPolicyFail;

template <class TErrorPolicy>
struct TValidateModeLazy;

template <class TErrorPolicy>
struct TValidateModeGreedy;

template <class TErrorPolicy, class TValidateMode = TValidateModeLazy<TErrorPolicy>>
struct TValidate {
    static NUdf::TUnboxedValue Value(const NUdf::IValueBuilder* valueBuilder, const TType* type,
                                     NUdf::TUnboxedValue&& value, const TString& message, bool* wrapped = nullptr);
    static void WrapCallable(const TCallableType* callableType, NUdf::TUnboxedValue& callable, const TString& message);
};

} // namespace NKikimr::NMiniKQL

#include "mkql_validate_impl.h"
