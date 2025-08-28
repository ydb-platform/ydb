#pragma once

namespace NKikimr::NTableIndex {

// Vector KmeansTree index tables description
namespace NTableVectorKmeansTreeIndex {

// Level and Posting tables
inline constexpr const char* ParentColumn = "__ydb_parent";

// Level table
inline constexpr const char* LevelTable = "indexImplLevelTable";
inline constexpr const char* IdColumn = "__ydb_id";
inline constexpr const char* CentroidColumn = "__ydb_centroid";

// Posting table
inline constexpr const char* PostingTable = "indexImplPostingTable";

inline constexpr const char* BuildSuffix0 = "0build";
inline constexpr const char* BuildSuffix1 = "1build";

// Prefix table
inline constexpr const char* PrefixTable = "indexImplPrefixTable";

inline constexpr const int DefaultKMeansRounds = 3;

}

namespace NTableFulltextIndex {

inline constexpr const char* TokenColumn = "__ydb_token";

}

}
