#ifndef CONFIGURABLE_SINGLETON_DECL_INL_H_
#error "Direct inclusion of this file is not allowed, include configurable_singleton_decl.h"
// For the sake of sane code completion.
#include "configurable_singleton_decl.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TConfig, bool Static>
struct TSingletonConfigTag
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#undef YT_DECLARE_CONFIGURABLE_SINGLETON
#undef YT_DECLARE_RECONFIGURABLE_SINGLETON

#define YT_DECLARE_CONFIGURABLE_SINGLETON(configType) \
    void CheckSingletonConfigRegistered(::NYT::NDetail::TSingletonConfigTag<configType, true>) \

#define YT_DECLARE_RECONFIGURABLE_SINGLETON(configType, dynamicConfigType) \
    void CheckSingletonConfigRegistered(::NYT::NDetail::TSingletonConfigTag<configType, true>); \
    void CheckSingletonConfigRegistered(::NYT::NDetail::TSingletonConfigTag<dynamicConfigType, false>)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
