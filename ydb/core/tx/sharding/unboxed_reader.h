#pragma once
#include "hash.h"
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <util/generic/map.h>

namespace NKikimr::NMiniKQL {
class TStructType;
}

namespace NKikimr::NSharding {

struct TExternalTableColumn {
    ui32 Id;
    NScheme::TTypeInfo Type;
    TString TypeMod;
    bool NotNull;
    bool IsBuildInProgress = false;
    bool IsCheckingNotNullInProgress = false;
};

struct TColumnUnboxedPlaceInfo: public TExternalTableColumn {
private:
    using TBase = TExternalTableColumn;
public:
    const ui32 Idx;
    const TString Name;

    TColumnUnboxedPlaceInfo(const TExternalTableColumn& baseInfo, const ui32 idx, const TString& name)
        : TBase(baseInfo)
        , Idx(idx)
        , Name(name) {

    }
};

class TUnboxedValueReader {
private:
    YDB_READONLY_DEF(std::vector<TColumnUnboxedPlaceInfo>, ColumnsInfo);
    template <class T>
    static void FieldToHashString(const NYql::NUdf::TUnboxedValue& value, NArrow::NHash::NXX64::TStreamStringHashCalcer& hashCalcer) {
        static_assert(std::is_arithmetic<T>::value);
        const T result = value.Get<T>();
        hashCalcer.Update((const ui8*)&result, sizeof(result));
    }
public:
    void BuildStringForHash(const NKikimr::NUdf::TUnboxedValue& value, NArrow::NHash::NXX64::TStreamStringHashCalcer& hashCalcer) const;
    TUnboxedValueReader(const NMiniKQL::TStructType* structInfo, const TMap<TString, TExternalTableColumn>& columnsRemap, const std::vector<TString>& shardingColumns);
};

}
