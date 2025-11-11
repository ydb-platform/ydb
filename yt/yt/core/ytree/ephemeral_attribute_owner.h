#pragma once

#include "attribute_owner.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeOwner
    : public virtual IAttributeOwner
{
public:
    const IAttributeDictionary& Attributes() const override;
    IAttributeDictionary* MutableAttributes() override;

protected:
    bool HasAttributes() const;
    void SetAttributes(IAttributeDictionaryPtr attributes);

private:
    IAttributeDictionaryPtr Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
