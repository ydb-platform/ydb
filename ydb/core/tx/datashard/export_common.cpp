#include "export_common.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NDataShard {

static void ResortColumns(
        google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& columns,
        const TMap<ui32, TUserTable::TUserColumn>& order)
{
    THashMap<TString, ui32> nameToTag;
    for (const auto& [tag, column] : order) {
        Y_VERIFY(nameToTag.emplace(column.Name, tag).second);
    }

    SortBy(columns, [&nameToTag](const auto& column) {
        auto it = nameToTag.find(column.name());
        Y_VERIFY(it != nameToTag.end());
        return it->second;
    });
}

TMaybe<Ydb::Table::CreateTableRequest> GenYdbScheme(
        const TMap<ui32, TUserTable::TUserColumn>& columns,
        const NKikimrSchemeOp::TPathDescription& pathDesc)
{
    if (!pathDesc.HasTable()) {
        return Nothing();
    }

    Ydb::Table::CreateTableRequest scheme;

    const auto& tableDesc = pathDesc.GetTable();
    NKikimrMiniKQL::TType mkqlKeyType;

    try {
        FillColumnDescription(scheme, mkqlKeyType, tableDesc);
    } catch (const yexception&) {
        return Nothing();
    }

    ResortColumns(*scheme.mutable_columns(), columns);

    scheme.mutable_primary_key()->CopyFrom(tableDesc.GetKeyColumnNames());

    try {
        FillTableBoundary(scheme, tableDesc, mkqlKeyType);
    } catch (const yexception&) {
        return Nothing();
    }

    FillIndexDescription(scheme, tableDesc);
    FillStorageSettings(scheme, tableDesc);
    FillColumnFamilies(scheme, tableDesc);
    FillAttributes(scheme, pathDesc);
    FillPartitioningSettings(scheme, tableDesc);
    FillKeyBloomFilter(scheme, tableDesc);
    FillReadReplicasSettings(scheme, tableDesc);

    return scheme;
}

TString DecimalToString(const std::pair<ui64, i64>& loHi) {
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    return ToString(val, NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
}

TString DyNumberToString(TStringBuf data) {
    auto result = NDyNumber::DyNumberToString(data);
    Y_VERIFY(result.Defined(), "Invalid DyNumber binary representation");
    return *result;
}

} // NDataShard
} // NKikimr
