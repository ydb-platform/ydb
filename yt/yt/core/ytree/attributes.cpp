#include "attributes.h"
#include "helpers.h"
#include "ephemeral_node_factory.h"
#include "exception_helpers.h"

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonString IAttributeDictionary::GetYson(TStringBuf key) const
{
    auto result = FindYson(key);
    if (!result) {
        ThrowNoSuchAttribute(key);
    }
    return result;
}

TYsonString IAttributeDictionary::GetYsonAndRemove(const TString& key)
{
    auto result = GetYson(key);
    Remove(key);
    return result;
}

void IAttributeDictionary::MergeFrom(const IMapNodePtr& other)
{
    for (const auto& [key, value] : other->GetChildren()) {
        // TODO(babenko): migrate to std::string
        SetYson(TString(key), ConvertToYsonString(value));
    }
}

void IAttributeDictionary::MergeFrom(const IAttributeDictionary& other)
{
    for (const auto& [key, value] : other.ListPairs()) {
        SetYson(key, value);
    }
}

IAttributeDictionaryPtr IAttributeDictionary::Clone() const
{
    auto attributes = CreateEphemeralAttributes();
    attributes->MergeFrom(*this);
    return attributes;
}

void IAttributeDictionary::Clear()
{
    for (const auto& key : ListKeys()) {
        Remove(key);
    }
}

bool IAttributeDictionary::Contains(TStringBuf key) const
{
    return FindYson(key).operator bool();
}

IAttributeDictionaryPtr IAttributeDictionary::FromMap(const IMapNodePtr& node)
{
    auto attributes = CreateEphemeralAttributes();
    auto children = node->GetChildren();
    for (int index = 0; index < std::ssize(children); ++index) {
        // TODO(babenko): migrate to std::string
        attributes->SetYson(TString(children[index].first), ConvertToYsonString(children[index].second));
    }
    return attributes;
}

IMapNodePtr IAttributeDictionary::ToMap() const
{
    auto map = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [key, value] : ListPairs()) {
        map->AddChild(key, ConvertToNode(value));
    }
    return map;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
