#pragma once

#include <ydb/library/yql/utils/range_walker.h>
#include <library/cpp/messagebus/network.h>

namespace NYql {
TVector<NBus::TBindResult> BindInRange(TRangeWalker<int>& portWalker);
}
