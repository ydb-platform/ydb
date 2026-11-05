#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

inline bool IsFulltextRowIdIndex(const Ydb::Table::TableIndexDescription& index) {
    return index.name() == NTableIndex::NFulltext::RowIdUniqueIndexName
        && index.has_global_unique_index() && index.index_columns_size() == 1
        && index.index_columns(0) == NTableIndex::NFulltext::RowIdColumn;
}

inline bool UsesFulltextRowId(const Ydb::Table::DescribeTableResult& table) {
    for (const auto& index : table.indexes()) {
        if (index.use_row_id_as_doc_id() || IsFulltextRowIdIndex(index)) {
            return true;
        }
    }
    return false;
}

inline bool IsFulltextRowIdIndex(const NKikimrReplication::TSchemaChange::TIndex& index) {
    return index.GetName() == NTableIndex::NFulltext::RowIdUniqueIndexName
        && index.GetType() == "GlobalUnique" && index.IndexColumnsSize() == 1
        && index.GetIndexColumns(0) == NTableIndex::NFulltext::RowIdColumn
        && !index.DataColumnsSize();
}

inline bool IsFulltextIndex(const Ydb::Table::TableIndexDescription& index) {
    return index.has_global_fulltext_plain_index()
        || index.has_global_fulltext_relevance_index()
        || index.has_global_json_index();
}

inline bool HasBuildingFulltextIndex(const Ydb::Table::DescribeTableResult& table) {
    for (const auto& index : table.indexes()) {
        if ((IsFulltextIndex(index) || IsFulltextRowIdIndex(index))
            && index.status() != Ydb::Table::TableIndexDescription::STATUS_READY)
        {
            return true;
        }
    }
    return false;
}

inline bool HasSupportedFulltextLayout(const Ydb::Table::TableIndexDescription& index) {
    return index.fulltext_storage_layout() == Ydb::Table::TableIndexDescription::FULLTEXT_STORAGE_LAYOUT_FLAT
        || index.fulltext_storage_layout() == Ydb::Table::TableIndexDescription::FULLTEXT_STORAGE_LAYOUT_COMPACT;
}

inline NKikimrSchemeOp::EIndexType GetFulltextIndexType(const Ydb::Table::TableIndexDescription& index) {
    const bool compact = index.fulltext_storage_layout() == Ydb::Table::TableIndexDescription::FULLTEXT_STORAGE_LAYOUT_COMPACT;
    if (index.has_global_fulltext_plain_index()) {
        return compact ? NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact : NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
    }
    if (index.has_global_fulltext_relevance_index()) {
        return compact ? NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance : NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
    }
    return compact ? NKikimrSchemeOp::EIndexTypeGlobalJsonCompact : NKikimrSchemeOp::EIndexTypeGlobalJson;
}

inline NKikimrSchemeOp::EIndexType GetSchemaIndexType(const NKikimrReplication::TSchemaChange::TIndex& index) {
    if (index.GetType() == "GlobalSync") {
        return NKikimrSchemeOp::EIndexTypeGlobal;
    }
    NKikimrSchemeOp::EIndexType type;
    return NKikimrSchemeOp::EIndexType_Parse("EIndexType" + index.GetType(), &type)
        ? type : NKikimrSchemeOp::EIndexTypeInvalid;
}

// Existing targets store the index path. Fulltext/JSON targets store the exact
// backing table path, making several streams per index possible without changing
// the persisted representation of ordinary index targets.
inline TString GetSourceIndexPath(const TString& path, bool srcIndexTable) {
    auto parts = SplitPath(path);
    if (srcIndexTable && !parts.empty()) {
        parts.pop_back();
    }
    return CanonizePath(parts);
}

inline TString GetSourceIndexTablePath(const TString& path, bool srcIndexTable) {
    auto parts = SplitPath(path);
    if (!srcIndexTable) {
        parts.emplace_back(NTableIndex::ImplTable);
    }
    return CanonizePath(parts);
}

}
