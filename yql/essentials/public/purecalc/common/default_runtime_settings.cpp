#include "default_runtime_settings.h"

#include <util/generic/singleton.h>

namespace NYql::NPureCalc::NPrivate {

NYql::TRuntimeSettings::TConstPtr GetDefaultRuntimeSettings() {
    struct TDefaultSettings {
        NYql::TRuntimeSettings::TConstPtr Ptr = []() {
            return MakeRuntimeSettingsMutable();
        }();
    };
    return Singleton<TDefaultSettings>()->Ptr;
}

} // namespace NYql::NPureCalc::NPrivate
