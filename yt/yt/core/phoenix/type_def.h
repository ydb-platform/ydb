#pragma once

#include "public.h"

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

/*
 * These macros are used to generate definitions of Phoenix-enabled types.
 * See type_decl.h for more information.
 */

//! Defines a Phoenix-enabled type.
#define PHOENIX_DEFINE_TYPE(type)

//! Defines a Phoenix-enabled template type. One must provide some
//! (arbitrary valid) sequence of type arguments (enclosed into parenthesis)
//! to instantiate the template.
//!
//! As a convention, the library provides an opaque type ``_`` to be used
//! in this context as a placeholder.
#define PHOENIX_DEFINE_TEMPLATE_TYPE(type, typeArgs)

struct _
{ };

//! Defines a Phoenix-enabled opaque class.
#define PHOENIX_DEFINE_OPAQUE_TYPE(type, typeTagValue)

//! A handy helper for registering Phoenix fields.
#define PHOENIX_REGISTER_FIELD(fieldTag, fieldName)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

#define TYPE_DEF_INL_H_
#include "type_def-inl.h"
#undef TYPE_DEF_INL_H_
