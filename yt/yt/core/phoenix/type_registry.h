#pragma once

#include "descriptors.h"

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

struct ITypeRegistry
{
    static ITypeRegistry* Get();

    virtual void RegisterTypeDescriptor(std::unique_ptr<TTypeDescriptor> typeDescriptor) = 0;
    virtual const TUniverseDescriptor& GetUniverseDescriptor() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
