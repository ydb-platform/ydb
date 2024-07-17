#pragma once

namespace NKikimr::NTableIndex::NTableVectorKmeansTreeIndex {

// Vector KmeansTree index tables description

// Levels table
inline constexpr const char* LevelTable = "indexImplLevelTable";
inline constexpr const char* LevelTable_ParentIdColumn = "-parent";
inline constexpr const char* LevelTable_IdColumn = "-id";
inline constexpr const char* LevelTable_EmbeddingColumn = "-embedding";

// Posting table
inline constexpr const char* PostingTable = "indexImplPostingTable";
inline constexpr const char* PostingTable_ParentIdColumn = "-parent";


}

