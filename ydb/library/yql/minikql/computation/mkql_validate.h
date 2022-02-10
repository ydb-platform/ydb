#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>

namespace NKikimr {
namespace NMiniKQL {

struct TValidateErrorPolicyNone;
struct TValidateErrorPolicyThrow;
struct TValidateErrorPolicyFail;

template<class TErrorPolicy>
struct TValidateModeLazy;

template<class TErrorPolicy>
struct TValidateModeGreedy;

template<class TErrorPolicy, class TValidateMode = TValidateModeLazy<TErrorPolicy>>
struct TValidate {
    static NUdf::TUnboxedValue Value(const NUdf::IValueBuilder* valueBuilder, const TType* type, NUdf::TUnboxedValue&& value, const TString& message, bool* wrapped = nullptr);
    static void WrapCallable(const TCallableType* callableType, NUdf::TUnboxedValue& callable, const TString& message);
};

} // namespace MiniKQL
} // namespace NKikimr

#include "mkql_validate_impl.h"
