#pragma once
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <util/generic/vector.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NMetadata::NInternal {

class TYDBColumn {
public:
    static Ydb::Column RawBytes(const TString& columnId);
    static Ydb::Column Utf8(const TString& columnId);
    static Ydb::Column Boolean(const TString& columnId);
    static Ydb::Column UInt64(const TString& columnId);
    static Ydb::Column UInt32(const TString& columnId);
};

class TYDBType {
public:
    static Ydb::Type Primitive(const Ydb::Type::PrimitiveTypeId type);
    static std::optional<NScheme::TTypeId> ConvertYDBToYQL(const Ydb::Type::PrimitiveTypeId type);
    static std::optional<Ydb::Type::PrimitiveTypeId> ConvertYQLToYDB(const NScheme::TTypeId type);
    static std::optional<Ydb::Type::PrimitiveTypeId> ConvertArrowToYDB(const arrow::Type::type type);

    static std::optional<TVector<std::pair<TString, NScheme::TTypeInfo>>> ConvertYDBToYQL(const std::vector<std::pair<TString, Ydb::Type>>& input);
};

class TYDBValue {
public:
    static bool IsSameType(const Ydb::Value& l, const Ydb::Type& type);
    static bool IsSameType(const Ydb::Value& l, const Ydb::Value& r);
    static bool Compare(const Ydb::Value& l, const Ydb::Value& r);
    static TString TypeToString(const Ydb::Type& type);
    static Ydb::Value NullValue();
    static Ydb::Value RawBytes(const char* value);
    static Ydb::Value RawBytes(const TString& value);
    static Ydb::Value RawBytes(const TStringBuf& value);
    static Ydb::Value Utf8(const char* value);
    static Ydb::Value Utf8(const TString& value);
    static Ydb::Value Utf8(const TStringBuf& value);
    static Ydb::Value UInt64(const ui64 value);
    static Ydb::Value Int64(const i64 value);
    static Ydb::Value UInt32(const ui32 value);
    static Ydb::Value Bool(const bool value);
};

}
