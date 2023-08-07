#include "system_attribute_provider.h"

#include <yt/yt/core/yson/writer.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

EPermission ISystemAttributeProvider::GetCustomAttributeModifyPermission()
{
    return EPermission::Write;
}

void ISystemAttributeProvider::ReserveAndListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->reserve(64);
    ListSystemAttributes(descriptors);
}

void ISystemAttributeProvider::ListSystemAttributes(std::map<TInternedAttributeKey, TAttributeDescriptor>* descriptors)
{
    std::vector<TAttributeDescriptor> attributes;
    ReserveAndListSystemAttributes(&attributes);

    for (const auto& descriptor : attributes) {
        YT_VERIFY(descriptors->emplace(descriptor.InternedKey, descriptor).second);
    }
}

void ISystemAttributeProvider::ListBuiltinAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    std::vector<TAttributeDescriptor> systemAttributes;
    ReserveAndListSystemAttributes(&systemAttributes);

    for (const auto& attribute : systemAttributes) {
        if (!attribute.Custom) {
            (*descriptors).push_back(attribute);
        }
    }
}

std::optional<ISystemAttributeProvider::TAttributeDescriptor> ISystemAttributeProvider::FindBuiltinAttributeDescriptor(
    TInternedAttributeKey key)
{
    std::vector<TAttributeDescriptor> builtinAttributes;
    ListBuiltinAttributes(&builtinAttributes);

    auto it = std::find_if(
        builtinAttributes.begin(),
        builtinAttributes.end(),
        [&] (const ISystemAttributeProvider::TAttributeDescriptor& info) {
            // Suppress operator== overload for enums.
            return static_cast<int>(info.InternedKey) == static_cast<int>(key);
        });
    return it == builtinAttributes.end() ? std::nullopt : std::make_optional(*it);
}

TYsonString ISystemAttributeProvider::FindBuiltinAttribute(TInternedAttributeKey key)
{
    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream);
    if (!GetBuiltinAttribute(key, &writer)) {
        return TYsonString();
    }
    writer.Flush();
    return TYsonString(stream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
