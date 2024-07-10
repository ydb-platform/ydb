#pragma once

namespace NKikimr {
namespace NTableIndex {
namespace NTableVectorKmeansTreeIndex {

// Vector KmeansTree index tables description

// Levels table
inline constexpr char LevelTable[] = "indexImplLevelTable";
inline constexpr char LevelTable_ParentIdColumn[] = "-parent";
inline constexpr char LevelTable_IdColumn[] = "-id";
inline constexpr char LevelTable_EmbeddingColumn[] = "-embedding";

// Posting table
inline constexpr char PostingTable[] = "indexImplPostingTable";
inline constexpr char PostingTable_ParentIdColumn[] = "-parent";


}
}
}
