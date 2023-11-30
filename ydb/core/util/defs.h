#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/util/defs.h

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>

#include <util/system/defaults.h>
#include <util/generic/bt_exception.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/align.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>
#include <util/generic/ylimits.h>

namespace NKikimr {
using namespace NActors;
} // namespace NKikimr
