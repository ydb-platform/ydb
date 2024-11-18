#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
// For the sake of sane code completion.
#include "node.h"
#endif

#include <library/cpp/yt/error/convert_to_cpo.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T INode::GetValue() const
{
    return ConvertTo<T>(INodePtr(const_cast<INode*>(this)));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T IMapNode::GetChildValueOrThrow(const std::string& key) const
{
    return GetChildOrThrow(key)->GetValue<T>();
}

template <class T>
T IMapNode::GetChildValueOrDefault(const std::string& key, const T& defaultValue) const
{
    auto child = FindChild(key);
    return child ? child->GetValue<T>() : defaultValue;
}

template <class T>
std::optional<T> IMapNode::FindChildValue(const std::string& key) const
{
    auto child = FindChild(key);
    return child ? std::make_optional(child->GetValue<T>()) : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T IListNode::GetChildValueOrThrow(int index) const
{
    return GetChildOrThrow(index)->GetValue<T>();
}

template <class T>
T IListNode::GetChildValueOrDefault(int index, const T& defaultValue) const
{
    auto child = FindChild(index);
    return child ? child->GetValue<T>() : defaultValue;
}

template <class T>
std::optional<T> IListNode::FindChildValue(int index) const
{
    auto child = FindChild(index);
    return child ? std::make_optional(child->GetValue<T>()) : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
