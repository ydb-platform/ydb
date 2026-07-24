#pragma once

#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>

namespace NFuzzHelpers {

inline NKikimr::TConversionTypeInfo BuildConvInfo(const Ydb::Type& type, bool isNotNullDefault) {
    NKikimr::NScheme::TTypeInfoMod mod;
    TString err;
    NKikimr::TConversionTypeInfo info;
    info.IsNotNull = isNotNullDefault;

    if (NKikimr::NScheme::TypeInfoFromProto(type, mod, err)) {
        info.TypeInfo = mod.TypeInfo;
        info.TypeMod = mod.TypeMod;
    } else {
        info.TypeInfo = NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Utf8);
    }

    return info;
}

inline bool IsSimpleDataType(const NKikimrMiniKQL::TType& t) {
    return t.HasKind() && t.GetKind() == NKikimrMiniKQL::Data && t.HasData();
}

} // namespace NFuzzHelpers
