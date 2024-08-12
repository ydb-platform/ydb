#pragma once

#include "public.h"

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

/*
 *  Phoenix enables reflection and persistence and capabilities for user types.
 *  The following naming convention applies:
 *
 *  1) DECLARE macros must be placed inside class declaration (in .h or .cpp file).
 *  These macros accept a unique tag used as a key for the type.
 *  Note that these macros will add some members to your class and will alter the current
 *  visibility.
 *
 *  2) DEFINE macros must be placed outside of the class (in .cpp or -inl.h file).
 *  These will provide definitions for the previously declared members and
 *  also static initializers to register the type within Phoenix type registry.
 *
 *  This file contains lightweight PHOENIX_DECLARE_* macros and must be included whenever a
 *  Phoenix-enabled type is declared. For PHOENIX_DEFINE_* macros, see type_def.h.
 */

////////////////////////////////////////////////////////////////////////////////
// Regular types
// For these, Phoenix provides persistence methods (|Save|, |Load|) and
// also runtime constructors.

//! Declares a Phoenix-enabled class with non-virtual persistence methods.
#define PHOENIX_DECLARE_TYPE(type, typeTag)

//! Similar to PHOENIX_DECLARE_TYPE, declares a Phoenix-enabled class with
//! virtual (overriden) persistence methods.
#define PHOENIX_DECLARE_POLYMORPHIC_TYPE(type, typeTag)

////////////////////////////////////////////////////////////////////////////////
// Template types
// For these types, Phoenix provides persistence methods (|Save|, |Load|).
// Note that Phoenix registers, in a sense, the template type itself, not its
// concrete instatiations. Hence no runtime constructors are provided.

//! Declares a Phoenix-enabled template class with non-virtual persistence methods.
//! It also provides inline implementations for all of the declared methods.
#define PHOENIX_DECLARE_TEMPLATE_TYPE(type, typeTag)

//! Similar to PHOENIX_DECLARE_TEMPLATE_TYPE, declares a Phoenix-enabled template class with
//! virtual (overriden) persistence methods.
#define PHOENIX_DECLARE_POLYMORPHIC_TEMPLATE_TYPE(type, typeTag)

////////////////////////////////////////////////////////////////////////////////
// Opaque types
// These are arbitrary types for which Phoenix provides no persistence support
// but still registers them in type registry and provides runtime constructors.
// Opaque type cannot be a template.

//! Declares a Phoenix-enabled opaque class.
#define PHOENIX_DECLARE_OPAQUE_TYPE(type, typeTagValue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

#define TYPE_DECL_INL_H_
#include "type_decl-inl.h"
#undef TYPE_DECL_INL_H_
