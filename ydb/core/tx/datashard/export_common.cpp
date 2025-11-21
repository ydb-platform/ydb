#include "export_common.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NDataShard {

static void ResortColumns(
        google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& columns,
        const TMap<ui32, TUserTable::TUserColumn>& order)
{
    THashMap<TString, ui32> nameToTag;
    for (const auto& [tag, column] : order) {
        Y_ENSURE(nameToTag.emplace(column.Name, tag).second);
    }

    SortBy(columns, [&nameToTag](const auto& column) {
        auto it = nameToTag.find(column.name());
        Y_ENSURE(it != nameToTag.end());
        return it->second;
    });
}
static TMaybe<Ydb::Table::CreateTableRequest> GenRowTableScheme(const TMap<ui32, TUserTable::TUserColumn>& columns,
                                                             const NKikimrSchemeOp::TPathDescription& pathDesc) {
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

static TMaybe<Ydb::Table::CreateTableRequest> GenColumnTableScheme(const TMap<ui32, TUserTable::TUserColumn>& columns,
                                                                   const NKikimrSchemeOp::TPathDescription& pathDesc) {
    if (!pathDesc.HasColumnTableDescription()) {
        return Nothing();
    }

    Ydb::Table::CreateTableRequest scheme;

    const auto& tableDesc = pathDesc.GetColumnTableDescription();
    NKikimrMiniKQL::TType mkqlKeyType;

    try {
        FillColumnDescription(scheme, tableDesc);
    } catch (const yexception&) {
        return Nothing();
    }

    ResortColumns(*scheme.mutable_columns(), columns);

    FillColumnFamilies(scheme, tableDesc);
    FillAttributes(scheme, pathDesc);
    FillPartitioningSettings(scheme, tableDesc);

    return scheme;
}

TMaybe<Ydb::Table::CreateTableRequest> GenYdbScheme(
        const TMap<ui32, TUserTable::TUserColumn>& columns,
        const NKikimrSchemeOp::TPathDescription& pathDesc)
{
    if (pathDesc.HasTable()) {
        return GenRowTableScheme(columns, pathDesc);
    }
    if (pathDesc.HasColumnTableDescription()) {
        return GenColumnTableScheme(columns, pathDesc);
    }
    return Nothing();
}

TMaybe<Ydb::Scheme::ModifyPermissionsRequest> GenYdbPermissions(const NKikimrSchemeOp::TPathDescription& pathDesc) {
    if (!pathDesc.HasSelf()) {
        return Nothing();
    }

    Ydb::Scheme::ModifyPermissionsRequest permissions;

    const auto& selfDesc = pathDesc.GetSelf();
    permissions.mutable_actions()->Add()->set_change_owner(selfDesc.GetOwner());

    NProtoBuf::RepeatedPtrField<Ydb::Scheme::Permissions> toGrant;
    ConvertAclToYdb(selfDesc.GetOwner(), selfDesc.GetACL(), false, &toGrant);
    for (const auto& permission : toGrant) {
        *permissions.mutable_actions()->Add()->mutable_grant() = permission;
    }

    return permissions;
}

} // NDataShard
} // NKikimr
