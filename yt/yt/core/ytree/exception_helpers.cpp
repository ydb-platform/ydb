#include "exception_helpers.h"
#include "attributes.h"
#include "node.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NYTree {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetNodePath(const IConstNodePtr& node)
{
    auto path = node->GetPath();
    return path.empty() ? "Root node" : Format("Node %v", path);
}

} // namespace

void ThrowInvalidNodeType(const IConstNodePtr& node, ENodeType expectedType, ENodeType actualType)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has invalid type: expected %Qlv, actual %Qlv",
        GetNodePath(node),
        expectedType,
        actualType);
}

void ValidateNodeType(
    const IConstNodePtr& node,
    const THashSet<ENodeType>& expectedTypes,
    const TString& expectedTypesStringRepresentation)
{
    if (!expectedTypes.contains(node->GetType())) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "%v has invalid type: expected one of %v, actual %Qlv",
            GetNodePath(node),
            expectedTypesStringRepresentation,
            node->GetType());
    }
}

void ThrowNoSuchChildKey(const IConstNodePtr& node, TStringBuf key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has no child with key %Qv",
        GetNodePath(node),
        ToYPathLiteral(key));
}

void ThrowNoSuchChildKey(TStringBuf key)
{
    THROW_ERROR_EXCEPTION("Node has no child with key %Qv",
        ToYPathLiteral(key));
}

void ThrowNoSuchChildIndex(const IConstNodePtr& node, int index)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "%v has no child with index %v",
        GetNodePath(node),
        index);
}

void ThrowNoSuchAttribute(TStringBuf key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowNoSuchCustomAttribute(TStringBuf key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Custom attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowNoSuchBuiltinAttribute(TStringBuf key)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Builtin attribute %Qv is not found",
        ToYPathLiteral(key));
}

void ThrowMethodNotSupported(TStringBuf method, const std::optional<TString>& resolveType)
{
    auto error = TError(
        NRpc::EErrorCode::NoSuchMethod,
        "%Qv method is not supported",
        method);
    if (resolveType) {
        error.MutableAttributes()->Set("resolve_type", *resolveType);
    }
    THROW_ERROR(error);
}

void ThrowCannotHaveChildren(const IConstNodePtr& node)
{
    THROW_ERROR_EXCEPTION("%v cannot have children",
        GetNodePath(node));
}

void ThrowAlreadyExists(const IConstNodePtr& node)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "%v already exists",
        GetNodePath(node));
}

void ThrowCannotRemoveNode(const IConstNodePtr& node)
{
    THROW_ERROR_EXCEPTION("%v cannot be removed",
        GetNodePath(node));
}

void ThrowCannotReplaceNode(const IConstNodePtr& node)
{
    THROW_ERROR_EXCEPTION("%v cannot be replaced",
        GetNodePath(node));
}

void ThrowCannotRemoveAttribute(TStringBuf key)
{
    THROW_ERROR_EXCEPTION("Attribute %Qv cannot be removed",
        ToYPathLiteral(key));
}

void ThrowCannotSetBuiltinAttribute(TStringBuf key)
{
    THROW_ERROR_EXCEPTION("Builtin attribute %Qv cannot be set",
        ToYPathLiteral(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
