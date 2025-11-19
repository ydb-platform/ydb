#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb-cpp-sdk/client/value/value.h>

#include <util/memory/pool.h>

namespace NYql::NNodes {
    class TExprBase;
}

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

bool CellFromProtoVal(const NScheme::TTypeInfo& type, i32 typmod, const Ydb::Value* vp, bool allowCastFromString,
                                TCell& c, TString& err, TMemoryPool& valueDataPool, bool allowInfDouble = false);

void ProtoValueFromCell(NYdb::TValueBuilder& vb, const NScheme::TTypeInfo& typeInfo, const TCell& cell);

bool FillACL(NKikimrSchemeOp::TModifyScheme& out,
    const TMaybeFail<Ydb::Scheme::ModifyPermissionsRequest>& in,
    TString& error);

void FillOwner(NKikimrScheme::TEvModifySchemeTransaction& out,
    const TMaybeFail<Ydb::Scheme::ModifyPermissionsRequest>& in);

struct TConversionTypeInfo {
    NScheme::TTypeInfo TypeInfo;
    TString TypeMod;
    bool IsNotNull;
};

bool CellsFromTuple(
    const Ydb::Type* tupleType, // nullptr if no type verification needed
    const Ydb::Value& tupleValue,
    const TConstArrayRef<TConversionTypeInfo>& types,
    bool allowCastFromString,
    bool checkNullability,
    TVector<TCell>& key,
    TString& errStr,
    TMemoryPool& valueDataPool);

} // namespace NKikimr
