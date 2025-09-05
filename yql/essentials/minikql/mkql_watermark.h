#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NKikimr::NMiniKQL {

struct TWatermark {
    TMaybe<TInstant> WatermarkIn;
};

} // namespace NKikimr::NMiniKQL
