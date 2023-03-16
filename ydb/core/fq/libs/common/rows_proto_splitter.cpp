#include "rows_proto_splitter.h"

#include <util/string/builder.h>

namespace NFq {

TRowsProtoSplitter::TRowsProtoSplitter(
    const Ydb::ResultSet& resultSet,
    const ui64 chunkLimit,
    const ui64 headerProtoByteSize,
    const ui64 maxRowsCountPerChunk)
    : ResultSet(resultSet)
    , ChunkLimit(chunkLimit)
    , MaxRowsCountPerChunk(maxRowsCountPerChunk)
    {
        ui64 colsBytes = 0; 
        for (const auto& column : resultSet.columns()) {
            colsBytes += column.ByteSizeLong();
        }
        BaseProtoBytesSize = colsBytes + headerProtoByteSize;
    }

TSplittedResultSets TRowsProtoSplitter::MakeResultWithIssues(const TString& msg) {
            Issues.AddIssue(msg);
            Success = false;
            return TSplittedResultSets{{}, std::move(Issues), Success};
}

TString TRowsProtoSplitter::CheckLimits(ui64 curRowBytes, size_t rowInd) {
    TString issueMsg;

    if (curRowBytes  + BaseProtoBytesSize > ChunkLimit) {
        issueMsg += TStringBuilder() << "Can not write Row["<< rowInd << "] with size: "
            << curRowBytes + BaseProtoBytesSize << " bytes (> " << ChunkLimit / (1024 * 1024) << "_MB)\n";
    }

    return issueMsg;    
}

void TRowsProtoSplitter::MakeNewChunk(Ydb::ResultSet& resultSet, ui64& chunkRowsCounter, ui64& chunkSize) {
    resultSet.Clear();
    resultSet.mutable_columns()->CopyFrom(ResultSet.columns());

    chunkRowsCounter  = 0;

    chunkSize = BaseProtoBytesSize;
}

TSplittedResultSets TRowsProtoSplitter::Split() {
    size_t rowInd = 0;
    size_t curChunkRowsCounter = 0;

    Ydb::ResultSet curResultSet;
    ui64 curChunkSize = 0;
    MakeNewChunk(curResultSet, curChunkRowsCounter, curChunkSize);

    for (const auto& row : ResultSet.rows()) {
        if (const auto issueMsg = CheckLimits(row.ByteSizeLong(), rowInd++)) {
            return MakeResultWithIssues(issueMsg);
        }

        if (row.ByteSizeLong() + curChunkSize <= ChunkLimit && ++curChunkRowsCounter <= MaxRowsCountPerChunk) {
            curChunkSize += row.ByteSizeLong();
            *curResultSet.add_rows() = row;
            continue;
        }

        SplittedResultSets.emplace_back(curResultSet);
        MakeNewChunk(curResultSet, curChunkRowsCounter, curChunkSize);
        *curResultSet.add_rows() = row;

    }
    SplittedResultSets.emplace_back(curResultSet); //last rs
    return TSplittedResultSets{std::move(SplittedResultSets), std::move(Issues), Success};
}

} //NFq
