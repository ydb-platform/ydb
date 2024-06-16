#pragma once
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

namespace NKikimr::NKqp {

void PrintValue(IOutputStream& out, const NYdb::TValue& v);
void PrintRow(IOutputStream& out, const THashMap<TString, NYdb::TValue>& fields);
void PrintRows(IOutputStream& out, const TVector<THashMap<TString, NYdb::TValue>>& rows);

ui64 GetUint32(const NYdb::TValue& v);
ui64 GetUint64(const NYdb::TValue& v);
TString GetUtf8(const NYdb::TValue& v);
TInstant GetTimestamp(const NYdb::TValue& v);

}