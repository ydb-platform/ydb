#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

namespace NKikimr::NMiniKQL {

class THolderFactory;

void ExportTypeToProto(TType* type, NKikimrMiniKQL::TType& res, const TVector<ui32>* columnOrder = nullptr);
void ExportValueToProto(TType* type, const NUdf::TUnboxedValuePod& value, NKikimrMiniKQL::TValue& res, const TVector<ui32>* columnOrder = nullptr);

void ExportPrimitiveTypeToProto(ui32 schemeType, Ydb::Type& output);

void ExportTypeToProto(TType* type, Ydb::Type& res, const TVector<ui32>* columnOrder = nullptr);
void ExportValueToProto(TType* type, const NUdf::TUnboxedValuePod& value, Ydb::Value& res, const TVector<ui32>* columnOrder = nullptr);


TType* ImportTypeFromProto(const NKikimrMiniKQL::TType& type, const TTypeEnvironment& env);

std::pair<TType*, NUdf::TUnboxedValue> ImportValueFromProto(const Ydb::Type& type, const Ydb::Value& value,
    const TTypeEnvironment& env, const THolderFactory& factory);
NUdf::TUnboxedValue ImportValueFromProto(TType* type, const Ydb::Value& value,
    const TTypeEnvironment& env, const THolderFactory& factory);
std::pair<TType*, NUdf::TUnboxedValue> ImportValueFromProto(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
    const TTypeEnvironment& env, const THolderFactory& factory);
TRuntimeNode ImportValueFromProto(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
                                  const TTypeEnvironment& env);
TRuntimeNode ImportValueFromProto(const NKikimrMiniKQL::TParams& params, const TTypeEnvironment& env);

inline void UuidToMkqlProto(const char* str, size_t sz, NKikimrMiniKQL::TValue& res) {
    union {
        ui64 half[2];
        char bytes[sizeof(ui64) * 2];
    } buf;
    Y_VERIFY(sizeof(buf) == sz);
    memcpy(buf.bytes, str, sizeof(buf));
    res.SetLow128(buf.half[0]);
    res.SetHi128(buf.half[1]);
}

inline void UuidToYdbProto(const char* str, size_t sz, Ydb::Value& res) {
    union {
        ui64 half[2];
        char bytes[sizeof(ui64) * 2];
    } buf;
    Y_VERIFY(sizeof(buf) == sz);
    memcpy(buf.bytes, str, sizeof(buf));
    res.set_low_128(buf.half[0]);
    res.set_high_128(buf.half[1]);
}

}
