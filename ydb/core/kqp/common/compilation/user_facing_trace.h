#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <vector>

namespace NKikimr::NKqp {

enum class EUserFacingCompileDependency {
    SchemeCache,
    StatisticsService,
};

enum class EUserFacingCompileStatus {
    Unknown,
    Ok,
    Error,
};

struct TUserFacingCompileSpan {
    EUserFacingCompileDependency Dependency;
    TString Target;
    TInstant Start;
    TInstant End;
    EUserFacingCompileStatus Status = EUserFacingCompileStatus::Unknown;
};

struct TUserFacingCompileActorSpan {
    TInstant Start;
    TInstant End;
};

struct TUserFacingCompileTrace {
    std::vector<TUserFacingCompileSpan> Spans;
    size_t Dropped = 0;
};

} // namespace NKikimr::NKqp
