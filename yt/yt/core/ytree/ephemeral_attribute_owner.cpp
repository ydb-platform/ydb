#include "ephemeral_attribute_owner.h"
#include "helpers.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

const IAttributeDictionary& TEphemeralAttributeOwner::Attributes() const
{
    if (!HasAttributes()) {
        return EmptyAttributes();
    }
    return *Attributes_;
}

IAttributeDictionary* TEphemeralAttributeOwner::MutableAttributes()
{
    if (!HasAttributes()) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return Attributes_.Get();
}

bool TEphemeralAttributeOwner::HasAttributes() const
{
    return static_cast<bool>(Attributes_);
}

void TEphemeralAttributeOwner::SetAttributes(IAttributeDictionaryPtr attributes)
{
    Attributes_ = std::move(attributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
