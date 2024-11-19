#include "export_common.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
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
        Y_ABORT_UNLESS(nameToTag.emplace(column.Name, tag).second);
    }

    SortBy(columns, [&nameToTag](const auto& column) {
        auto it = nameToTag.find(column.name());
        Y_ABORT_UNLESS(it != nameToTag.end());
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
        FillIndexDescription(scheme, tableDesc);
    } catch (const yexception&) {
        return Nothing();
    }

    FillStorageSettings(scheme, tableDesc);
    FillColumnFamilies(scheme, tableDesc);
    FillAttributes(scheme, pathDesc);
    FillPartitioningSettings(scheme, tableDesc);
    FillKeyBloomFilter(scheme, tableDesc);
    FillReadReplicasSettings(scheme, tableDesc);

    TString error;
    Ydb::StatusIds::StatusCode status;
    if (!FillSequenceDescription(scheme, tableDesc, status, error)) {
        return Nothing();
    }

    return scheme;
}

TString DecimalToString(const std::pair<ui64, i64>& loHi) {
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    return ToString(val, NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
}

TString DyNumberToString(TStringBuf data) {
    TString result;
    TStringOutput out(result);
    TStringBuilder err;

    bool success = DyNumberToStream(data, out, err);
    Y_ABORT_UNLESS(success);

    return result;
}

bool DecimalToStream(const std::pair<ui64, i64>& loHi, IOutputStream& out, TString& err) {
    Y_UNUSED(err);
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    out << ToString(val, NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
    return true;
}

bool DyNumberToStream(TStringBuf data, IOutputStream& out, TString& err) {
    auto result = NDyNumber::DyNumberToString(data);
    if (!result.Defined()) {
        err = "Invalid DyNumber binary representation";
        return false;
    }
    out << *result;
    return true;
}

bool PgToStream(TStringBuf data, void* typeDesc, IOutputStream& out, TString& err) {
    const NPg::TConvertResult& pgResult = NPg::PgNativeTextFromNativeBinary(data, typeDesc);
    if (pgResult.Error) {
        err = *pgResult.Error;
        return false;
    }
    out << '"' << CGIEscapeRet(pgResult.Str) << '"';
    return true;
}

bool UuidToStream(const std::pair<ui64, ui64>& loHi, IOutputStream& out, TString& err) {
    Y_UNUSED(err);

    NYdb::TUuidValue uuid(loHi.first, loHi.second);
    
    out << uuid.ToString();
    
    return true;
}

} // NDataShard
} // NKikimr
