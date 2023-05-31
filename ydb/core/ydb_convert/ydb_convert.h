#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NKikimr {

void ConvertMiniKQLTypeToYdbType(const NKikimrMiniKQL::TType& input, Ydb::Type& output);
void ConvertMiniKQLValueToYdbValue(const NKikimrMiniKQL::TType& inputType,
    const NKikimrMiniKQL::TValue& inputValue,
    Ydb::Value& output);
void ConvertYdbTypeToMiniKQLType(const Ydb::Type& input, NKikimrMiniKQL::TType& output);
void ConvertYdbValueToMiniKQLValue(const Ydb::Type& inputType,
    const Ydb::Value& inputValue,
    NKikimrMiniKQL::TValue& output);

void ConvertYdbResultToKqpResult(const Ydb::ResultSet& input, NKikimrMiniKQL::TResult& output);

void ConvertYdbParamsToMiniKQLParams(const ::google::protobuf::Map<TString, Ydb::TypedValue>& input,
    NKikimrMiniKQL::TParams& output);

void ConvertAclToYdb(const TString& owner, const TString& acl, bool isContainer,
    google::protobuf::RepeatedPtrField<Ydb::Scheme::Permissions> *permissions);

struct TACLAttrs {
    ui32 AccessMask;
    ui32 InheritanceType;

    TACLAttrs(ui32 access, ui32 inheritance);
    TACLAttrs(ui32 access);
};

TACLAttrs ConvertYdbPermissionNameToACLAttrs(const TString& name);

TVector<TString> ConvertACLMaskToYdbPermissionNames(ui32);
TString ConvertShortYdbPermissionNameToFullYdbPermissionName(const TString& name);

void ConvertDirectoryEntry(const NKikimrSchemeOp::TDirEntry& from, Ydb::Scheme::Entry* to, bool processAcl);
void ConvertDirectoryEntry(const NKikimrSchemeOp::TPathDescription& from, Ydb::Scheme::Entry* to, bool processAcl);

} // namespace NKikimr
