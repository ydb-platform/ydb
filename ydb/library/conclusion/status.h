#pragma once
#include <ydb/library/conclusion/generic/status.h>

namespace NKikimr {

using TConclusionStatus = TConclusionStatusImpl<::TNull, ::TNull{}, ::TNull{}>;

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
using TConclusionSpecialStatus = TConclusionStatusImpl<TStatus, StatusOk, DefaultError>;

}
