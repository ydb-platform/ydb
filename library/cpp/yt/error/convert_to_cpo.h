#pragma once

#include <library/cpp/yt/misc/tag_invoke_cpo.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// NB(arkady-e1ppa): This file intentionally is unaware of possible "yson-implementations"
// e.g. INode and TYsonString(Buf). This is done for two reasons:
// 1) We can't have dep on INodePtr here anyway.
// 2) We would like to eventually remove dep on yson of this module.

////////////////////////////////////////////////////////////////////////////////

// NB(arkady-e1ppa): We intentionally generate a separate namespace
// where ConvertToImpl functions would reside
// without polluting general-use namespaces.
namespace NConvertToImpl {

template <class T>
struct TFn : public NYT::TTagInvokeCpoBase<TFn<T>>
{ };

} // NConvertToImpl

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline constexpr NConvertToImpl::TFn<T> ConvertTo = {};

////////////////////////////////////////////////////////////////////////////////

template <class TTo, class TFrom>
concept CConvertsTo = requires (const TFrom& from) {
    { NYT::ConvertTo<TTo>(from) } -> std::same_as<TTo>;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
