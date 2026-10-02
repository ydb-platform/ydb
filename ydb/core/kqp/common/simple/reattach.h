#pragma once

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

struct TReattachInfo {
    TDuration Delay;
    TInstant Deadline;
    bool Reattaching = false;
};

bool ShouldReattach(TInstant now, TReattachInfo& reattachInfo);

}
