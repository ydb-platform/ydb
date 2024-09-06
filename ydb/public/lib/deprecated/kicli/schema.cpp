#include "kicli.h"
#include <ydb/public/lib/deprecated/client/msgbus_client.h>

namespace NKikimr {
namespace NClient {

TColumn::TColumn(const TString& name, const TType& type)
    : Name(name)
    , Type(type)
    , Key(false)
    , Partitions(0)
{}

TColumn::TColumn(const TString& name, const TType& type, bool key, ui32 partitions)
    : Name(name)
    , Type(type)
    , Key(key)
    , Partitions(partitions)
{}

TKeyColumn::TKeyColumn(const TString& name, const TType& type)
    : TColumn(name, type, true, 0)
{}

TKeyColumn::TKeyColumn(const TString& name, const TType& type, ui32 partitions)
    : TColumn(name, type, true, partitions)
{}

TKeyPartitioningColumn::TKeyPartitioningColumn(const TString& name, const TType& type, ui32 partitions)
    : TKeyColumn(name, type, partitions)
{}

TType::TType(const TString& typeName, NScheme::TTypeId typeId)
    : TypeName(typeName)
    , TypeId(typeId)
{}

TType::TType(NScheme::TTypeId typeId)
    : TypeName(NScheme::TypeName(typeId))
    , TypeId(typeId)
{}

const TType TType::Int64(NScheme::NTypeIds::Int64);
const TType TType::Uint64(NScheme::NTypeIds::Uint64);
const TType TType::Int32(NScheme::NTypeIds::Int32);
const TType TType::Uint32(NScheme::NTypeIds::Uint32);
const TType TType::Bool(NScheme::NTypeIds::Bool);
const TType TType::Double(NScheme::NTypeIds::Double);
const TType TType::Float(NScheme::NTypeIds::Float);
const TType TType::Utf8(NScheme::NTypeIds::Utf8);
const TType TType::String(NScheme::NTypeIds::String);
const TType TType::String4k(NScheme::NTypeIds::String4k);
const TType TType::String2m(NScheme::NTypeIds::String2m);
const TType TType::Yson(NScheme::NTypeIds::Yson);
const TType TType::Json(NScheme::NTypeIds::Json);
const TType TType::JsonDocument(NScheme::NTypeIds::JsonDocument);
const TType TType::Timestamp(NScheme::NTypeIds::Timestamp);

const TString& TType::GetName() const {
    return TypeName;
}

ui16 TType::GetId() const {
    return TypeId;
}

TSchemaObject::TSchemaObject(TKikimr& kikimr, const TString& path, const TString& name, ui64 pathId, EPathType pathType)
    : Kikimr(kikimr)
    , Path(path.EndsWith('/') ? path + name : path + "/" + name)
    , Name(name)
    , PathId(pathId)
    , PathType(pathType)
{
    static_assert((ui32)NKikimrSchemeOp::EPathTypeDir == (ui32)EPathType::Directory, "EPathType::Directory");
    static_assert((ui32)NKikimrSchemeOp::EPathTypeTable == (ui32)EPathType::Table, "EPathType::Table");
    static_assert((ui32)NKikimrSchemeOp::EPathTypePersQueueGroup == (ui32)EPathType::PersQueueGroup, "EPathType::PersQueueGroup");
    static_assert((ui32)NKikimrSchemeOp::EPathTypeSubDomain == (ui32)EPathType::SubDomain, "EPathType::SubDomain");
}

void TSchemaObject::ModifySchema(const TModifyScheme& schema) {
    NThreading::TFuture<TResult> future = Kikimr.ModifySchema(schema);
    TResult result = future.GetValue(TDuration::Max());
    result.GetError().Throw();
}

void TSchemaObject::Drop() {
    TModifyScheme drop;
    switch (PathType) {
    case EPathType::Directory:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpRmDir);
        break;
    case EPathType::Table:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
        break;
    case EPathType::PersQueueGroup:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
        break;
    case EPathType::BlockStoreVolume:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume);
        break;
    case EPathType::FileStore:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore);
        break;
    case EPathType::Kesus:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus);
        break;
    case EPathType::SolomonVolume:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume);
        break;
    case EPathType::OlapStore:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore);
        break;
    case EPathType::OlapTable:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable);
        break;
    case EPathType::Sequence:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence);
        break;
    case EPathType::Replication:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication);
        break;
    case EPathType::BlobDepot:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropBlobDepot);
        break;
    case EPathType::ExternalTable:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable);
        break;
    case EPathType::ExternalDataSource:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource);
        break;
    case EPathType::View:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropView);
        break;
    case EPathType::ResourcePool:
        drop.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropResourcePool);
        break;
    case EPathType::Unknown:
    case EPathType::SubDomain:
    case EPathType::RtmrVolume:
        throw yexception() << "Wrong drop";
        break;
    }

    drop.SetWorkingDir(Path);
    drop.MutableDrop()->SetId(PathId);
    ModifySchema(drop);
}

TSchemaObject TSchemaObject::MakeDirectory(const TString& name) {
    NThreading::TFuture<TResult> future = Kikimr.MakeDirectory(*this, name);
    TResult result = future.GetValue(TDuration::Max());
    result.GetError().Throw();

    const NKikimrClient::TResponse& response = result.GetResult<NKikimrClient::TResponse>();
    ui64 pathId = response.GetFlatTxId().GetPathId();
    Y_ABORT_UNLESS(pathId);
    return TSchemaObject(Kikimr, Path, name, pathId, EPathType::Directory);
}

TSchemaObject TSchemaObject::CreateTable(const TString& name, const TVector<TColumn>& columns) {
    return DoCreateTable(name, columns, nullptr);
}

TSchemaObject TSchemaObject::CreateTable(const TString& name, const TVector<TColumn>& columns,
                                         const TTablePartitionConfig& partitionConfig)
{
    return DoCreateTable(name, columns, &partitionConfig);
}

TSchemaObject TSchemaObject::DoCreateTable(const TString& name, const TVector<TColumn>& columns,
                                         const TTablePartitionConfig* partitionConfig)
{
    NThreading::TFuture<TResult> future = Kikimr.CreateTable(*this, name, columns, partitionConfig);
    TResult result = future.GetValue(TDuration::Max());
    result.GetError().Throw();

    const NKikimrClient::TResponse& response = result.GetResult<NKikimrClient::TResponse>();
    ui64 pathId = response.GetFlatTxId().GetPathId();
    Y_ABORT_UNLESS(pathId);
    return TSchemaObject(Kikimr, Path, name, pathId, EPathType::Table);
}

TSchemaObject TSchemaObject::GetChild(const TString& name) const {
    auto children = GetChildren();
    auto child = FindIf(children.begin(), children.end(), [&](const TSchemaObject& c) { return c.GetName() == name; });
    if (child == children.end()) {
        throw yexception() << "Schema object '" << name << "' not found";
    }
    return *child;
}

static TSchemaObject::EPathType GetType(const NKikimrSchemeOp::TDirEntry& entry) {
    switch (entry.GetPathType()) {
    case NKikimrSchemeOp::EPathTypeDir:
        return TSchemaObject::EPathType::Directory;
    case NKikimrSchemeOp::EPathTypeTable:
        return TSchemaObject::EPathType::Table;
    case NKikimrSchemeOp::EPathTypePersQueueGroup:
        return TSchemaObject::EPathType::PersQueueGroup;
    case NKikimrSchemeOp::EPathTypeSubDomain:
        return TSchemaObject::EPathType::SubDomain;
    case NKikimrSchemeOp::EPathTypeRtmrVolume:
        return TSchemaObject::EPathType::RtmrVolume;
    case NKikimrSchemeOp::EPathTypeBlockStoreVolume:
        return TSchemaObject::EPathType::BlockStoreVolume;
    case NKikimrSchemeOp::EPathTypeFileStore:
        return TSchemaObject::EPathType::FileStore;
    case NKikimrSchemeOp::EPathTypeKesus:
        return TSchemaObject::EPathType::Kesus;
    case NKikimrSchemeOp::EPathTypeSolomonVolume:
        return TSchemaObject::EPathType::SolomonVolume;
    case NKikimrSchemeOp::EPathTypeColumnStore:
        return TSchemaObject::EPathType::OlapStore;
    case NKikimrSchemeOp::EPathTypeColumnTable:
        return TSchemaObject::EPathType::OlapTable;
    case NKikimrSchemeOp::EPathTypeSequence:
        return TSchemaObject::EPathType::Sequence;
    case NKikimrSchemeOp::EPathTypeReplication:
        return TSchemaObject::EPathType::Replication;
    case NKikimrSchemeOp::EPathTypeBlobDepot:
        return TSchemaObject::EPathType::BlobDepot;
    case NKikimrSchemeOp::EPathTypeExternalTable:
        return TSchemaObject::EPathType::ExternalTable;
    case NKikimrSchemeOp::EPathTypeExternalDataSource:
        return TSchemaObject::EPathType::ExternalDataSource;
    case NKikimrSchemeOp::EPathTypeView:
        return TSchemaObject::EPathType::View;
    case NKikimrSchemeOp::EPathTypeResourcePool:
        return TSchemaObject::EPathType::ResourcePool;
    case NKikimrSchemeOp::EPathTypeTableIndex:
    case NKikimrSchemeOp::EPathTypeExtSubDomain:
    case NKikimrSchemeOp::EPathTypeCdcStream:
    case NKikimrSchemeOp::EPathTypeInvalid:
        return TSchemaObject::EPathType::Unknown;
    }
    return TSchemaObject::EPathType::Unknown;
}

TVector<TSchemaObject> TSchemaObject::GetChildren() const {
    NThreading::TFuture<TResult> future = Kikimr.DescribeObject(*this);
    TResult result = future.GetValue(TDuration::Max());
    result.GetError().Throw();
    const NKikimrClient::TResponse& objects = result.GetResult<NKikimrClient::TResponse>();
    TVector<TSchemaObject> children;
    children.reserve(objects.GetPathDescription().ChildrenSize());
    for (const auto& child : objects.GetPathDescription().GetChildren()) {
        children.push_back(TSchemaObject(Kikimr, Path, child.GetName(), child.GetPathId(), GetType(child)));
    }
    return children;
}

TVector<TColumn> TSchemaObject::GetColumns() const {
    NThreading::TFuture<TResult> future = Kikimr.DescribeObject(*this);
    TResult result = future.GetValue(TDuration::Max());
    result.GetError().Throw();
    const NKikimrClient::TResponse& objects = result.GetResult<NKikimrClient::TResponse>();
    Y_ABORT_UNLESS(objects.GetPathDescription().HasTable());
    const auto& table = objects.GetPathDescription().GetTable();

    TMap<ui32, NKikimrSchemeOp::TColumnDescription> columnsMap;
    for (const auto& column : table.GetColumns()) {
        columnsMap[column.GetId()] = column;
    }

    TVector<TColumn> columns;
    columns.reserve(table.ColumnsSize());
    for (ui32 keyColumnId : table.GetKeyColumnIds()) {
        auto column = columnsMap.FindPtr(keyColumnId);
        Y_ABORT_UNLESS(column);
        columns.push_back(TKeyColumn(column->GetName(), TType(column->GetType(), column->GetTypeId())));
        columnsMap.erase(keyColumnId);
    }
    for (const auto& pair : columnsMap) {
        auto& column = pair.second;
        columns.push_back(TColumn(column.GetName(), TType(column.GetType(), column.GetTypeId())));
    }

    return columns;
}

TSchemaObjectStats TSchemaObject::GetStats() const {
    NThreading::TFuture<TResult> future = Kikimr.DescribeObject(*this);
    TResult result = future.GetValue(TDuration::Max());
    result.GetError().Throw();
    const NKikimrClient::TResponse& objects = result.GetResult<NKikimrClient::TResponse>();
    Y_ABORT_UNLESS(objects.GetPathDescription().HasTable());
    TSchemaObjectStats stats;
    stats.PartitionsCount = objects.GetPathDescription().TablePartitionsSize();
    return stats;
}

TString TSchemaObject::GetPath() const {
    return Path;
}

TString TSchemaObject::GetName() const {
    return Name;
}

}
}
