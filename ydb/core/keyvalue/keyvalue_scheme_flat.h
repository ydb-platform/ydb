#pragma once
#include "defs.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NKeyValue {

constexpr ui32 TABLE_ID = 0; // Table 0: [TSmallBoundedString key KEY, TString value]
constexpr ui32 KEY_TAG = 1;
constexpr ui32 VALUE_TAG = 2;
constexpr ui32 KEY_COLUMN_IDX = 0;
constexpr ui32 VALUE_COLUMN_IDX = 0;

} // NKeyValue
} // NKikimr
