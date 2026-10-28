#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <concepts>
#include <type_traits>

namespace NYT::NPhoenix {

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
 *
 *  Persistence methods are declared for |TSaveContext| and |TLoadContext| as visible at the
 *  point of declaration; these need not be NPhoenix contexts. Any stream context providing
 *  |GetVersion| will do. Load contexts not derived from TLoadContext, though, lack schema
 *  support (see TLoadContext::SetSchema) and load solely by version. Pointer serialization
 *  requires NPhoenix contexts.
 */

////////////////////////////////////////////////////////////////////////////////
// Regular types
// For these, Phoenix provides persistence methods (|Save|, |Load|) and
// also runtime constructors.

// The DECLARE macro for a regular or template type ends with the declaration of
// |static void RegisterMetadata(auto&& registrar)|. Follow it either with ';' and
// define RegisterMetadata out of line (preferred for non-trivial types), or directly
// with the body:
//
//   struct TFoo
//   {
//       int A = 0;
//
//       PHOENIX_DECLARE_TYPE(TFoo, 0x12345678)
//       {
//           PHOENIX_REGISTER_FIELD(1, A);
//       }
//   };
//
// Either way, the matching DEFINE macro is still needed.

//! Declares a Phoenix-enabled class with non-virtual persistence methods.
#define PHOENIX_DECLARE_TYPE(type, typeTag)

//! Similar to PHOENIX_DECLARE_TYPE, declares a Phoenix-enabled class with
//! virtual (overriden) persistence methods.
#define PHOENIX_DECLARE_POLYMORPHIC_TYPE(type, typeTag)

//! Similar to PHOENIX_DECLARE_TYPE, declares a Phoenix-enabled class introducing
//! virtual persistence methods, i.e. the root of a hierarchy whose descendants
//! use PHOENIX_DECLARE_POLYMORPHIC_TYPE. Unlike TPolymorphicBase, this
//! does not affect how pointers to the type are serialized.
#define PHOENIX_DECLARE_VIRTUAL_TYPE(type, typeTag)

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
// External types
// For classes that cannot be altered, metadata is declared outside of the class.
// Both macros must appear at global scope; the registration body is written
// exactly as an in-class one, and may reach protected members.
// Handler signatures stay TThis* (the registered type); a handler reaches
// a protected member as this_->*(&TAccessor::Field_).

//! Declares Phoenix metadata for an already existing class.
#define PHOENIX_DECLARE_EXTERNAL_TYPE(type, typeTagValue, saveContext, loadContext)

////////////////////////////////////////////////////////////////////////////////
// Field registration
// To be used inside RegisterMetadata.

//! Registers a field persisted with its regular Save/Load.
#define PHOENIX_REGISTER_FIELD(fieldTag, fieldName, ...)

//! Registers a field of type |fieldType| that is no longer persisted since |version|;
//! older snapshots still have it, and it is skipped on load.
#define PHOENIX_REGISTER_DELETED_FIELD(fieldTag, fieldType, fieldName, version, ...)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix

#define TYPE_DECL_INL_H_
#include "type_decl-inl.h"
#undef TYPE_DECL_INL_H_
