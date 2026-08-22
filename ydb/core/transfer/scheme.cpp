#include "scheme.h"

#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NReplication::NTransfer {

TString TSchemeColumn::TypeName() const {
    return NScheme::TypeName(PType);
}


TScheme::TPtr BuildScheme(const TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& nav) {
    const auto& entry = nav->ResultSet.at(0);

    TScheme::TPtr result = std::make_shared<TScheme>();

    result->TableColumns.reserve(entry.Columns.size());
    result->ColumnsMetadata.reserve(entry.Columns.size());
    result->StructMetadata.reserve(entry.Columns.size() + 1);
    result->ReadIndex.reserve(entry.Columns.size());
    result->WriteIndex.reserve(entry.Columns.size());

    size_t keyColumns = CountIf(entry.Columns, [](auto& c) {
        return c.second.KeyOrder >= 0;
    });

    result->TableColumns.resize(keyColumns);

    for (const auto& [_, column] : entry.Columns) {
        auto notNull = entry.NotNullColumns.contains(column.Name);
        if (column.KeyOrder >= 0) {
            result->TableColumns[column.KeyOrder] = {column.Name, column.Id, column.PType, column.KeyOrder >= 0, !notNull};
        } else {
            result->TableColumns.emplace_back(column.Name, column.Id, column.PType, column.KeyOrder >= 0, !notNull);
        }
    }

    std::map<TString, TSysTables::TTableColumnInfo> columns;
    for (const auto& [_, column] : entry.Columns) {
        columns[column.Name] = column;
    }
    columns[SystemColumns::TargetTable] = TSysTables::TTableColumnInfo{SystemColumns::TargetTable, 0, NScheme::TTypeInfo(NScheme::NTypeIds::String)};

    size_t i = keyColumns;
    size_t j = 0;
    for (const auto& [name, column] : columns) {
        result->StructMetadata.emplace_back();
        auto& c = result->StructMetadata.back();

        c.SetName(column.Name);
        c.SetId(column.Id);
        c.SetTypeId(column.PType.GetTypeId());
        c.SetNotNull(entry.NotNullColumns.contains(column.Name));

        if (NScheme::NTypeIds::IsParametrizedType(column.PType.GetTypeId())) {
            NScheme::ProtoFromTypeInfo(column.PType, "", *c.MutableTypeInfo());
        }

        if (name == SystemColumns::TargetTable) {
            result->TargetTableIndex = j;
        } else {
            result->ColumnsMetadata.push_back(c);
            result->WriteIndex.push_back(column.KeyOrder >= 0 ? column.KeyOrder : i++);
            result->ReadIndex.push_back(j);

            Ydb::Type type;
            if (column.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
                auto* decimalType = type.mutable_decimal_type();
                decimalType->set_precision(column.PType.GetDecimalType().GetPrecision());
                decimalType->set_scale(column.PType.GetDecimalType().GetScale());
            } else {
                type.set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(column.PType.GetTypeId()));
            }
            result->Types->emplace_back(column.Name, type);
        }

        ++j;
    }

    return result;
}

namespace {

NYT::TNode CreateTypeNode(const NScheme::TTypeInfo& fieldType) {
    if (fieldType.GetTypeId() == NScheme::NTypeIds::Decimal) {
        const auto& decimal = fieldType.GetDecimalType();
        return NYT::TNode::CreateList()
            .Add("DataType")
            .Add("Decimal")
            .Add(Sprintf("%u", decimal.GetPrecision()))
            .Add(Sprintf("%u", decimal.GetScale()));
    }

    return NYT::TNode::CreateList()
        .Add("DataType")
        .Add(NScheme::TypeName(fieldType));
}

NYT::TNode CreateOptionalTypeNode(const NScheme::TTypeInfo& fieldType) {
    return NYT::TNode::CreateList()
        .Add("OptionalType")
        .Add(CreateTypeNode(fieldType));
}

void AddField(NYT::TNode& node, const TString& fieldName, const NScheme::TTypeInfo& fieldType, bool nullable) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(nullable ? CreateOptionalTypeNode(fieldType) : CreateTypeNode(fieldType))
    );
}

}

NYT::TNode MakeOutputSchema(const TVector<TSchemeColumn>& columns) {
    auto structMembers = NYT::TNode::CreateList();

    AddField(structMembers, SystemColumns::TargetTable, NScheme::TTypeInfo(NScheme::NTypeIds::String), true);
    for (const auto& column : columns) {
        AddField(structMembers, column.Name, column.PType, column.Nullable);
    }

    auto rootMembers = NYT::TNode::CreateList();
    rootMembers.Add(
        NYT::TNode::CreateList()
            .Add(SystemColumns::Root)
            .Add(NYT::TNode::CreateList()
                .Add("StructType")
                .Add(std::move(structMembers)))
    );

    return NYT::TNode::CreateList()
        .Add("StructType")
        .Add(std::move(rootMembers));
}

}
