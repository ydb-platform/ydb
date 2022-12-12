#pragma once
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

namespace NKikimr::NMetadata::NInternal {

class TYDBColumn {
public:
    static Ydb::Column Bytes(const TString& columnId);
    static Ydb::Column Boolean(const TString& columnId);
    static Ydb::Column UInt64(const TString& columnId);
    static Ydb::Column UInt32(const TString& columnId);
};

class TYDBValue {
public:
    static bool IsSameType(const Ydb::Value& l, const Ydb::Type& type);
    static bool IsSameType(const Ydb::Value& l, const Ydb::Value& r);
    static bool Compare(const Ydb::Value& l, const Ydb::Value& r);
    static TString TypeToString(const Ydb::Type& type);
    static Ydb::Value NullValue();
    static Ydb::Value Bytes(const char* value);
    static Ydb::Value Bytes(const TString& value);
    static Ydb::Value Bytes(const TStringBuf& value);
    static Ydb::Value UInt64(const ui64 value);
    static Ydb::Value UInt32(const ui32 value);
};

}
