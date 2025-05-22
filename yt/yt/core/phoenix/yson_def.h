#pragma once

#include "public.h"

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

/*
 * These macros are used to generate definitions of YSON dump mixin.
 * See yson_decl.h for more information.
 */

//! Defines YSON serialization mixin for a type.
#define PHOENIX_DEFINE_YSON_DUMPABLE_TYPE_MIXIN(type)

//! Declares (and also inline-defines) YSON serialization mixin for a template type.
//! No matching PHOENIX_DEFINE_YSON_* macro is needed.
#define PHOENIX_DECLARE_YSON_DUMPABLE_TEMPLATE_MIXIN(type)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix

#define YSON_DEF_INL_H_
#include "yson_def-inl.h"
#undef YSON_DEF_INL_H_
