#pragma once

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

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

constexpr i64 DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS = TDuration::Days(16).MilliSeconds();
constexpr ui64 DEFAULT_PARTITION_SPEED = 1_MB;
constexpr i32 MAX_READ_RULES_COUNT = 3000;
constexpr i32 MAX_SUPPORTED_CODECS_COUNT = 100;

}
