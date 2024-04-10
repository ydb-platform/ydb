#include <ydb/core/driver_lib/version/version.h>

NKikimrConfig::TCurrentCompatibilityInfo NKikimr::TCompatibilityInfo::MakeCurrent() {
    using TCurrentConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
    // using TVersionConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TVersion;
    // using TCompatibilityRuleConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;

    return TCurrentConstructor{
        .Application = "ydb",
    }.ToPB();
}
