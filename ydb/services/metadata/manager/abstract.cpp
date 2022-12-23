#include "abstract.h"
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

TTableSchema::TTableSchema(const THashMap<ui32, TSysTables::TTableColumnInfo>& description) {
    std::map<TString, Ydb::Column> columns;
    std::map<ui32, Ydb::Column> pkColumns;
    for (auto&& [_, i] : description) {
        Ydb::Column column;
        column.set_name(i.Name);
        column.mutable_type()->set_type_id(::Ydb::Type::PrimitiveTypeId(i.PType.GetTypeId()));
        if (i.KeyOrder >= 0) {
            Y_VERIFY(pkColumns.emplace(i.KeyOrder, std::move(column)).second);
        } else {
            Y_VERIFY(columns.emplace(i.Name, std::move(column)).second);
        }
    }
    for (auto&& i : pkColumns) {
        AddColumn(true, i.second);
    }
    for (auto&& i : columns) {
        AddColumn(false, i.second);
    }
}

NKikimr::NMetadata::NModifications::TTableSchema& TTableSchema::AddColumn(const bool primary, const Ydb::Column& info) noexcept {
    Columns.emplace_back(primary, info);
    YDBColumns.emplace_back(info);
    if (primary) {
        PKColumns.emplace_back(info);
        PKColumnIds.emplace_back(info.name());
    }
    return *this;
}

NThreading::TFuture<NKikimr::NMetadata::NModifications::TObjectOperatorResult> IOperationsManager::DropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId, IClassBehaviour::TPtr manager, const TModificationContext& context) const {
    if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
        return NThreading::MakeFuture<TObjectOperatorResult>("metadata provider service is disabled");
    }
    return DoDropObject(settings, nodeId, manager, context);
}

NThreading::TFuture<NKikimr::NMetadata::NModifications::TObjectOperatorResult> IOperationsManager::AlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId, IClassBehaviour::TPtr manager, const TModificationContext& context) const {
    if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
        return NThreading::MakeFuture<TObjectOperatorResult>("metadata provider service is disabled");
    }
    return DoAlterObject(settings, nodeId, manager, context);
}

NThreading::TFuture<NKikimr::NMetadata::NModifications::TObjectOperatorResult> IOperationsManager::CreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId, IClassBehaviour::TPtr manager, const TModificationContext& context) const {
    if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
        return NThreading::MakeFuture<TObjectOperatorResult>("metadata provider service is disabled");
    }
    return DoCreateObject(settings, nodeId, manager, context);
}

}
