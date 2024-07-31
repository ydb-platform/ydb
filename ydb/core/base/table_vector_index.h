#pragma once

namespace NKikimr::NTableIndex::NTableVectorKmeansTreeIndex {

// Vector KmeansTree index tables description

// Level table
inline constexpr const char* LevelTable = "indexImplLevelTable";
inline constexpr const char* LevelTable_ParentIdColumn = "__ydb_parent";
inline constexpr const char* LevelTable_IdColumn = "__ydb_id";
inline constexpr const char* LevelTable_EmbeddingColumn = "__ydb_embedding";

// Posting table
inline constexpr const char* PostingTable = "indexImplPostingTable";
inline constexpr const char* PostingTable_ParentIdColumn = LevelTable_ParentIdColumn;

inline constexpr const char* TmpPostingTableSuffix0 = "0tmp";
inline constexpr const char* TmpPostingTableSuffix1 = "1tmp";

}
