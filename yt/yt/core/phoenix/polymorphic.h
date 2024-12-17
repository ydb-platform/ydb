#pragma once

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

//! Inheriting from this class marks the type as "polymorphic" for
//! the purpose of serialization.
/*!
 *  For polymorphic types, when serializing a pointer to it
 *  Phoenix will also write the type tag of the actual runtime type.
 *  This enables reconstructing the proper instance upon deserializing it
 *  via a base pointer.
 */
struct TPolymorphicBase
{
    virtual ~TPolymorphicBase() = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

#define POLYMORPHIC_INL_H_
#include "polymorphic-inl.h"
#undef POLYMORPHIC_INL_H_
