#pragma once

// TODO(babenko): Drop once contrib/ydb and contrib/libs/ydb-cpp-sdk include library/cpp/yt/system/event_count.h.

#include <library/cpp/yt/system/event_count.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TEvent;
using ::NYT::TEventCount;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
