#pragma once

#include "runtime_settings.h"

#include <yql/essentials/providers/common/config/yql_dispatch.h>

namespace NYql {

// Introspection for the runtime settings.
// Must be logically stateless since it may be constructed multiple times from any line of code.
class TRuntimeSettingsConfiguration: public NYql::NCommon::TSettingDispatcher, public TRuntimeSettings {
public:
    using TPtr = TSharedPtr<TRuntimeSettingsConfiguration, TAtomicCounter>;
    using TConstPtr = TSharedPtr<const TRuntimeSettingsConfiguration, TAtomicCounter>;

    TRuntimeSettingsConfiguration();
    explicit TRuntimeSettingsConfiguration(const TRuntimeSettings& settings);
};

TRuntimeSettingsConfiguration::TConstPtr MakeRuntimeSettingsConfiguration(auto&&... args) {
    return MakeShared<const TRuntimeSettingsConfiguration, TAtomicCounter>(std::forward<decltype(args)>(args)...);
}

TRuntimeSettingsConfiguration::TPtr MakeRuntimeSettingsConfigurationMutable(auto&&... args) {
    return MakeShared<TRuntimeSettingsConfiguration, TAtomicCounter>(std::forward<decltype(args)>(args)...);
}

} // namespace NYql
