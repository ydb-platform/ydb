#include "unboxed_reader.h"
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NKikimr::NSharding {

void TUnboxedValueReader::BuildStringForHash(const NKikimr::NUdf::TUnboxedValue& value, NArrow::NHash::NXX64::TStreamStringHashCalcer& hashCalcer) const {
    for (auto&& i : ColumnsInfo) {
        auto columnValue = value.GetElement(i.Idx);
        if (columnValue.IsString()) {
            hashCalcer.Update((const ui8*)columnValue.AsStringRef().Data(), columnValue.AsStringRef().Size());
        } else if (columnValue.IsEmbedded()) {
            switch (i.Type.GetTypeId()) {
                case NScheme::NTypeIds::Uint16:
                    FieldToHashString<ui16>(columnValue, hashCalcer);
                    continue;
                case NScheme::NTypeIds::Uint32:
                    FieldToHashString<ui32>(columnValue, hashCalcer);
                    continue;
                case NScheme::NTypeIds::Uint64:
                    FieldToHashString<ui64>(columnValue, hashCalcer);
                    continue;
                case NScheme::NTypeIds::Int16:
                    FieldToHashString<i16>(columnValue, hashCalcer);
                    continue;
                case NScheme::NTypeIds::Int32:
                    FieldToHashString<i32>(columnValue, hashCalcer);
                    continue;
                case NScheme::NTypeIds::Int64:
                    FieldToHashString<i64>(columnValue, hashCalcer);
                    continue;
            }
            YQL_ENSURE(false, "incorrect column type for shard calculation");
        } else {
            YQL_ENSURE(false, "incorrect column type for shard calculation");
        }
    }
}

TUnboxedValueReader::TUnboxedValueReader(const NMiniKQL::TStructType* structInfo,
    const TMap<TString, TExternalTableColumn>& columnsRemap, const std::vector<TString>& shardingColumns) {
    YQL_ENSURE(shardingColumns.size());
    for (auto&& i : shardingColumns) {
        auto it = columnsRemap.find(i);
        YQL_ENSURE(it != columnsRemap.end());
        ColumnsInfo.emplace_back(TColumnUnboxedPlaceInfo(it->second, structInfo->GetMemberIndex(i), i));
    }
}

}
