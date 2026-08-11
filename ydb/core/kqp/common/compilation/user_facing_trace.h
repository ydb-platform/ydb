#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

enum class EUserFacingCompileDependency {
    SchemeCache,
    StatisticsService,
};

struct TUserFacingCompileSpan {
    EUserFacingCompileDependency Dependency;
    TString Target;
    TInstant Start;
    TInstant End;
};

struct TUserFacingCompileActorSpan {
    TInstant Start;
    TInstant End;
};

} // namespace NKikimr::NKqp
