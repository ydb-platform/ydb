#pragma once

#include "public.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeOwner
{
    virtual ~IAttributeOwner() = default;

    virtual const IAttributeDictionary& Attributes() const = 0;
    virtual IAttributeDictionary* MutableAttributes() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
