#pragma once

namespace NKikimr::NTableIndex::NTableVectorKmeansTreeIndex {

// Vector KmeansTree index tables description

// Level table
inline constexpr const char* LevelTable = "indexImplLevelTable";
inline constexpr const char* LevelTable_ParentColumn = "__ydb_parent";
inline constexpr const char* LevelTable_IdColumn = "__ydb_id";
inline constexpr const char* LevelTable_EmbeddingColumn = "__ydb_embedding";

// Posting table
inline constexpr const char* PostingTable = "indexImplPostingTable";
inline constexpr const char* PostingTable_ParentColumn = LevelTable_ParentColumn;

inline constexpr const char* BuildPostingTableSuffix0 = "0build";
inline constexpr const char* BuildPostingTableSuffix1 = "1build";

}
