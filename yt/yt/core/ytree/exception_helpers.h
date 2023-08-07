#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

[[noreturn]] void ThrowInvalidNodeType(
    const IConstNodePtr& node,
    ENodeType expectedType,
    ENodeType actualType);
void ValidateNodeType(
    const IConstNodePtr& node,
    const THashSet<ENodeType>& expectedTypes,
    const TString& expectedTypesStringRepresentation);

[[noreturn]] void ThrowNoSuchChildKey(const IConstNodePtr& node, TStringBuf key);
[[noreturn]] void ThrowNoSuchChildKey(TStringBuf key);
[[noreturn]] void ThrowNoSuchChildIndex(const IConstNodePtr& node, int index);
[[noreturn]] void ThrowNoSuchAttribute(TStringBuf key);
[[noreturn]] void ThrowNoSuchBuiltinAttribute(TStringBuf key);
[[noreturn]] void ThrowNoSuchCustomAttribute(TStringBuf key);
[[noreturn]] void ThrowMethodNotSupported(
    TStringBuf method,
    const std::optional<TString>& resolveType = {});
[[noreturn]] void ThrowCannotHaveChildren(const IConstNodePtr& node);
[[noreturn]] void ThrowAlreadyExists(const IConstNodePtr& node);
[[noreturn]] void ThrowCannotRemoveNode(const IConstNodePtr& node);
[[noreturn]] void ThrowCannotReplaceNode(const IConstNodePtr& node);
[[noreturn]] void ThrowCannotRemoveAttribute(TStringBuf key);
[[noreturn]] void ThrowCannotSetBuiltinAttribute(TStringBuf key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
