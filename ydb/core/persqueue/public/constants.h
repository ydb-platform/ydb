#pragma once

#include <util/generic/string.h>

namespace NKikimr::NPQ {

const TString CLIENTID_COMPACTION_CONSUMER = "__ydb_compaction_consumer";
const TString CLIENTID_WITHOUT_CONSUMER = "$without_consumer";

constexpr TStringBuf MESSAGE_ATTRIBUTE_KEY = "__key";
constexpr TStringBuf MESSAGE_ATTRIBUTE_DEDUPLICATION_ID = "message_deduplication_id";
constexpr TStringBuf MESSAGE_ATTRIBUTE_ATTRIBUTES = "__message_attributes";
constexpr TStringBuf MESSAGE_ATTRIBUTE_DELAY_SECONDS = "__delay_seconds";

constexpr ui32 METRICS_LEVEL_DISABLED = 0;
constexpr ui32 METRICS_LEVEL_DATABASE = 1;
constexpr ui32 METRICS_LEVEL_OBJECT = 2;
constexpr ui32 METRICS_LEVEL_DETAILED = 3;

}
