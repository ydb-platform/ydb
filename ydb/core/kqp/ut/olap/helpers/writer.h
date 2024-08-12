#pragma once
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

void WriteTestData(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, bool withSomeNulls = false);

}