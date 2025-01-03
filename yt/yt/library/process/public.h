#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNamedPipe)
DECLARE_REFCOUNTED_CLASS(TNamedPipeConfig)

DECLARE_REFCOUNTED_CLASS(TIODispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TIODispatcherDynamicConfig)


YT_DECLARE_RECONFIGURABLE_SINGLETON(TIODispatcherConfig, TIODispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
