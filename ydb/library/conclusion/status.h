#pragma once

#include <ydb/library/conclusion/generic/string_status.h>
#include <ydb/library/conclusion/generic/yql_status.h>

namespace NKikimr {

using TConclusionStatus = TConclusionStatusImpl<::TNull, ::TNull{}, ::TNull{}>;

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
using TConclusionSpecialStatus = TConclusionStatusImpl<TStatus, StatusOk, DefaultError>;

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
using TYQLConclusionSpecialStatus = TYQLConclusionStatusImpl<TStatus, StatusOk, DefaultError>;

}
