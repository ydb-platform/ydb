#include "scheme.h"

#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NReplication::NTransfer {

TString TSchemeColumn::TypeName() const {
    return NScheme::TypeName(PType);
}


TScheme BuildScheme(const TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& nav) {
    const auto& entry = nav->ResultSet.at(0);

    TScheme result;

    result.TableColumns.reserve(entry.Columns.size());
    result.ColumnsMetadata.reserve(entry.Columns.size());
    result.StructMetadata.reserve(entry.Columns.size() + 1);
    result.WriteIndex.reserve(entry.Columns.size());

    size_t keyColumns = CountIf(entry.Columns, [](auto& c) {
        return c.second.KeyOrder >= 0;
    });

    result.TableColumns.resize(keyColumns);

    for (const auto& [_, column] : entry.Columns) {
        auto notNull = entry.NotNullColumns.contains(column.Name);
        if (column.KeyOrder >= 0) {
            result.TableColumns[column.KeyOrder] = {column.Name, column.Id, column.PType, column.KeyOrder >= 0, !notNull};
        } else {
            result.TableColumns.emplace_back(column.Name, column.Id, column.PType, column.KeyOrder >= 0, !notNull);
        }
    }

    std::map<TString, TSysTables::TTableColumnInfo> columns;
    for (const auto& [_, column] : entry.Columns) {
        columns[column.Name] = column;
    }
    columns[SystemColumns::TargetTable] = TSysTables::TTableColumnInfo{SystemColumns::TargetTable, 0, NScheme::TTypeInfo(NScheme::NTypeIds::String)};

    size_t i = keyColumns;
    for (const auto& [name, column] : columns) {
        result.StructMetadata.emplace_back();
        auto& c = result.StructMetadata.back();

        c.SetName(column.Name);
        c.SetId(column.Id);
        c.SetTypeId(column.PType.GetTypeId());
        c.SetNotNull(entry.NotNullColumns.contains(column.Name));

        if (NScheme::NTypeIds::IsParametrizedType(column.PType.GetTypeId())) {
            NScheme::ProtoFromTypeInfo(column.PType, "", *c.MutableTypeInfo());
        }

        if (name != SystemColumns::TargetTable) {
            result.ColumnsMetadata.push_back(c);
            result.WriteIndex.push_back(column.KeyOrder >= 0 ? column.KeyOrder : i++);

            Ydb::Type type;
            type.set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(column.PType.GetTypeId()));
            result.Types->emplace_back(column.Name, type);
        } else {
            ++i;
        }
    }

    return result;
}

}
