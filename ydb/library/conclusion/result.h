#pragma once
#include "status.h"

#include <ydb/library/conclusion/generic/result.h>

namespace NKikimr {

template <typename TResult>
using TConclusion = TConclusionImpl<TConclusionStatus, TResult>;

}   // namespace NKikimr
