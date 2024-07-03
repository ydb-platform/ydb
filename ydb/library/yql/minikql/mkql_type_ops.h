#pragma once
#include "defs.h"
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_type_ops.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>

#include <util/stream/output.h>

namespace NKikimr {
namespace NMiniKQL {

//TODO remove
TStringBuf AdaptLegacyYqlType(const TStringBuf& type);

bool IsValidValue(NUdf::EDataSlot type, const NUdf::TUnboxedValuePod& value);

bool IsLeapYear(i32 year);

ui32 GetMonthLength(ui32 month, bool isLeap);

bool IsValidStringValue(NUdf::EDataSlot type, NUdf::TStringRef buf);

NUdf::TUnboxedValuePod ValueFromString(NUdf::EDataSlot type, NUdf::TStringRef buf);
NUdf::TUnboxedValuePod SimpleValueFromYson(NUdf::EDataSlot type, NUdf::TStringRef buf);

NUdf::TUnboxedValuePod ValueToString(NUdf::EDataSlot type, NUdf::TUnboxedValuePod value);

NUdf::TUnboxedValuePod ParseUuid(NUdf::TStringRef buf, bool shortForm=false);
bool ParseUuid(NUdf::TStringRef buf, void* output, bool shortForm=false);

bool IsValidDecimal(NUdf::TStringRef buf);

bool MakeDate(ui32 year, ui32 month, ui32 day, ui16& value);
bool MakeDate32(i32 year, ui32 month, ui32 day, i32& value);
bool MakeTime(ui32 hour, ui32 minute, ui32 second, ui32& value);
bool SplitDate(ui16 value, ui32& year, ui32& month, ui32& day);
bool SplitDate32(i32 value, i32& year, ui32& month, ui32& day);
bool SplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec);
bool SplitTimestamp(ui64 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec, ui32& usec);
bool SplitInterval(i64 value, bool& sign, ui32& day, ui32& hour, ui32& min, ui32& sec, ui32& usec);

bool SplitTzDate(ui16 value, ui32& year, ui32& month, ui32& day, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId);
bool SplitTzDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId);

bool MakeTzDatetime(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec, ui32& value, ui16 tzId);
bool SplitTzDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec, ui16 tzId);
bool EnrichDate(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek);
bool EnrichDate32(i32 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek);
bool GetTimezoneShift(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec, ui16 tzId, i32& value);

ui16 InitTimezones();
bool IsValidTimezoneId(ui16 id);
TMaybe<ui16> FindTimezoneId(TStringBuf ianaName);
ui16 GetTimezoneId(TStringBuf ianaName);
TMaybe<TStringBuf> FindTimezoneIANAName(ui16 id);
TStringBuf GetTimezoneIANAName(ui16 id);
std::vector<ui16> GetTzBlackList();

void ToLocalTime(ui32 utcSeconds, ui16 tzId, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec);
ui32 FromLocalTime(ui16 tzId, ui32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec);

void SerializeTzDate(ui16 date, ui16 tzId, IOutputStream& out);
void SerializeTzDatetime(ui32 datetime, ui16 tzId, IOutputStream& out);
void SerializeTzTimestamp(ui64 timestamp, ui16 tzId, IOutputStream& out);
bool DeserializeTzDate(TStringBuf buf, ui16& date, ui16& tzId);
bool DeserializeTzDatetime(TStringBuf buf, ui32& datetime, ui16& tzId);
bool DeserializeTzTimestamp(TStringBuf buf, ui64& timestamp, ui16& tzId);

void SerializeTzDate32(i32 date, ui16 tzId, IOutputStream& out);
void SerializeTzDatetime64(i64 datetime, ui16 tzId, IOutputStream& out);
void SerializeTzTimestamp64(i64 timestamp, ui16 tzId, IOutputStream& out);
bool DeserializeTzDate32(TStringBuf buf, i32& date, ui16& tzId);
bool DeserializeTzDatetime64(TStringBuf buf, i64& datetime, ui16& tzId);
bool DeserializeTzTimestamp64(TStringBuf buf, i64& timestamp, ui16& tzId);
} // namespace NMiniKQL
} // namespace NKikimr
