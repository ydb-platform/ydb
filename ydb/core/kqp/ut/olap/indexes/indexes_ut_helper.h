#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

void ExecQuery(TKikimrRunner& kikimr, bool useQueryService, const TString& query);
void ExecQueryExpectErrorContains(TKikimrRunner& kikimr, bool useQueryService, const TString& query, TStringBuf needle);

} // namespace NKikimr::NKqp
