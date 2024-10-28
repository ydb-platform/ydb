#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme_types/scheme_types.h>

namespace NKikimrProto {
    class TTypeInfo;
}

namespace NKikimr::NScheme {

::TString TypeName(const TTypeInfo typeInfo, const ::TString& typeMod = {});

bool GetTypeInfo(const NScheme::IType* type, const NKikimrProto::TTypeInfo& typeInfoProto, const TStringBuf& typeName, const TStringBuf& columnName, NScheme::TTypeInfo &typeInfo, ::TString& errorStr);

} // NKikimr::NScheme
