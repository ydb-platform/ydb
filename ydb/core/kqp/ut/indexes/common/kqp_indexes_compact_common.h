#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NKqp {

TString FormatFulltextIndex(TKikimrRunner& kikimr, const TString& name = "TestTable/json_idx", bool withRelevance = false);

} // namespace NKikimr::NKqp
