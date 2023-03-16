#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NFq {

struct TSplittedResultSets {
    TVector<Ydb::ResultSet> ResultSets;
    NYql::TIssues Issues;
    bool Success = true;
};

class TRowsProtoSplitter {
public:
    TRowsProtoSplitter(
        const Ydb::ResultSet& resultSet,
        const ui64 chunkLimit,
        const ui64 headerProtoByteSize = 0,
        const ui64 maxRowsCountPerChunk = 100'000
    );

    void MakeNewChunk(Ydb::ResultSet& resultSet, ui64& chunkRowsCounter, ui64& chunkSize);

    TString CheckLimits(ui64 curRowBytes, size_t rowInd);

    TSplittedResultSets MakeResultWithIssues(const TString& msg);

    TSplittedResultSets Split();


private:
    Ydb::ResultSet ResultSet;
    const ui64 ChunkLimit = 0;
    ui64 BaseProtoBytesSize = 0;
    const ui64 MaxRowsCountPerChunk = 0;

    TVector<Ydb::ResultSet> SplittedResultSets;
    NYql::TIssues Issues;
    bool Success = true;
};

} //NFq
