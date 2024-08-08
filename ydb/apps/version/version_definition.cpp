#include <ydb/core/driver_lib/version/version.h>

NKikimrConfig::TCurrentCompatibilityInfo NKikimr::TCompatibilityInfo::MakeCurrent() {
    using TCurrentConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
    using TVersionConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TVersion;
    using TCompatibilityRuleConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;

    return TCurrentConstructor{
        .Application = "ydb",
        .Version = TVersionConstructor{
            .Year = 24,
            .Major = 2,
        },
        .CanConnectTo = {
            TCompatibilityRuleConstructor{
                .Application = "nbs",
                .LowerLimit = TVersionConstructor{ .Year = 23, .Major = 3 },
                .UpperLimit = TVersionConstructor{ .Year = 24, .Major = 2 },
            },
        },
    }.ToPB();
}
