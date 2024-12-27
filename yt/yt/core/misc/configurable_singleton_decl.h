#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#define YT_DECLARE_CONFIGURABLE_SINGLETON(configType)
#define YT_DECLARE_RECONFIGURABLE_SINGLETON(configType, dynamicConfigType)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONFIGURABLE_SINGLETON_DECL_INL_H_
#include "configurable_singleton_decl-inl.h"
#undef CONFIGURABLE_SINGLETON_DECL_INL_H_
