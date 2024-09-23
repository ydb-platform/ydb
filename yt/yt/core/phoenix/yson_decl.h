#pragma once

#include "public.h"

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

/*
 * Generates |Serialize| function for serializing (dumping) objects into YSON.
 * Must be used in conjunction with PHOENIX_DECLARE_* / PHOENIX_DEFINE_* macros.
 *
 * This file contains lightweight PHOENIX_DECLARE_YSON_* macros.
 * For PHOENIX_DEFINE_YSON_* macros, see yson_def.h.
 */

//! Declares YSON serialization mixin for a type.
#define PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN(type)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

#define YSON_DECL_INL_H_
#include "yson_decl-inl.h"
#undef YSON_DECL_INL_H_
