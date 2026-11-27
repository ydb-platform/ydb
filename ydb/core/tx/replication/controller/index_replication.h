#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/replication.pb.h>

#include <google/protobuf/util/message_differencer.h>

namespace NKikimr::NReplication::NController {

inline bool EqualFulltextSettings(const Ydb::Table::FulltextIndexSettings& left,
        const Ydb::Table::FulltextIndexSettings& right)
{
    return google::protobuf::util::MessageDifferencer::Equivalent(left, right);
}

inline bool IsFulltextRowIdIndex(const NKikimrReplication::TSchemaChange::TIndex& index) {
    return index.GetType() == "GlobalUnique" && index.IndexColumnsSize() == 1
        && index.GetIndexColumns(0) == NTableIndex::NFulltext::RowIdColumn;
}

inline bool IsOnlineReplicationIndex(const Ydb::Table::TableIndexDescription& index) {
    return index.has_global_index()
        || index.has_global_unique_index()
        || index.has_global_fulltext_plain_index()
        || index.has_global_fulltext_relevance_index()
        || index.has_global_json_index();
}

inline TVector<TString> GetBuildingReplicationIndexes(const Ydb::Table::DescribeTableResult& table) {
    TVector<TString> result;
    for (const auto& index : table.indexes()) {
        if (IsOnlineReplicationIndex(index)
            && index.status() != Ydb::Table::TableIndexDescription::STATUS_READY)
        {
            result.emplace_back(index.name());
        }
    }
    return result;
}

inline NKikimrSchemeOp::EIndexType GetSchemaIndexType(const NKikimrReplication::TSchemaChange::TIndex& index) {
    if (index.GetType() == "GlobalSync") {
        return NKikimrSchemeOp::EIndexTypeGlobal;
    }
    NKikimrSchemeOp::EIndexType type;
    return NKikimrSchemeOp::EIndexType_Parse("EIndexType" + index.GetType(), &type)
        ? type : NKikimrSchemeOp::EIndexTypeInvalid;
}

}
