#include "ut_helpers.h"

#include <ydb/core/driver_lib/version/version.h>

namespace NKikimr {

TString MakeData(ui32 dataSize, ui32 step) {
    TString data(dataSize, '\0');
    for (ui32 i = 0; i < dataSize; ++i) {
        data[i] = 'A' + i * step % ('Z' - 'A' + 1);
    }
    return data;
}

ui64 TInflightActor::Cookie = 1;

NKikimrConfig::TCurrentCompatibilityInfo MakeCompatibilityInfo(const std::optional<TVersion>& version,
        EComponentId componentId) {
    TString build = "ydb";
    TCurrentConstructor ctor;
    if (version) {
        ctor = TCurrentConstructor{
            .Application = build,
            .Version = TYdbVersion{
                .Year = std::get<0>(*version),
                .Major = std::get<1>(*version),
                .Minor = std::get<2>(*version),
                .Hotfix = std::get<3>(*version)
            }
        };
    } else {
        ctor = TCurrentConstructor{
            .Application = "trunk",
        };
    }

    static std::vector<EComponentId> compatibilityComponents = {
        NKikimrConfig::TCompatibilityRule::PDisk,
        NKikimrConfig::TCompatibilityRule::VDisk,
        NKikimrConfig::TCompatibilityRule::BlobStorageController,
    };

    // Disable compatibility checks for all other components
    for (auto component : compatibilityComponents) {
        if (component != componentId) {
            auto newRule = TCompatibilityRule{
                .Application = build,
                .LowerLimit = TYdbVersion{ .Year = 0, .Major = 0, .Minor = 0, .Hotfix = 0 },
                .UpperLimit = TYdbVersion{ .Year = 1000, .Major = 1000, .Minor = 1000, .Hotfix = 1000 },
                .ComponentId = component,
            };
            ctor.CanLoadFrom.push_back(newRule);
            ctor.StoresReadableBy.push_back(newRule);
        }
    }
    return ctor.ToPB();
}

} // namespace NKikimr
