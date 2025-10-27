#pragma once

#include <util/generic/string.h>

namespace NKikimr::NPQ {

static const TString CLIENTID_COMPACTION_CONSUMER = "__ydb_compaction_consumer";
static const TString CLIENTID_WITHOUT_CONSUMER = "$without_consumer";

static constexpr ui32 METRICS_LEVEL_DISABLED = 0;
static constexpr ui32 METRICS_LEVEL_DATABASE = 1;
static constexpr ui32 METRICS_LEVEL_OBJECT = 2;
static constexpr ui32 METRICS_LEVEL_DETAILED = 3;

}
