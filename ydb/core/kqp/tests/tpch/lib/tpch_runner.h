#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/map.h>


namespace NYdb::NTpch {

class TTpchRunner {
public:
    TTpchRunner(const TDriver& driver, const TString& tablesPath);

    void CreateTables(const TMap<TString, ui32>& partitions, bool force);
    void UploadBundledData(ui32 partitionsCount = 2, bool dropTables = true);
    void UploadData(const TString& dir, ui32 partitionSizeMb, size_t batchSize = 1000);

    void DropTables(NTable::TTableClient& client, bool removeDir = false);

    void PrepareDataForPostgresql(const TString& dir, const TString& outFileName);

    NQuery::TExecuteQueryIterator RunQuery(ui32 number, bool profile = false);

    NQuery::TExecuteQueryIterator RunQuery01(TInstant startDate = TInstant::ParseIso8601("1998-12-01"),
                                                  TDuration startDateShift = TDuration::Days(3),
                                                  bool profile = false);

    // todo: replace hardcoded query parameters with function's arguments
    NQuery::TExecuteQueryIterator RunQuery02(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery03(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery04(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery05(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery06(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery07(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery08(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery09(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery10(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery11(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery12(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery13(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery14(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery15(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery16(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery17(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery18(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery19(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery20(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery21(bool profile = false);
    NQuery::TExecuteQueryIterator RunQuery22(bool profile = false);

private:
    using TRowProvider = std::function<bool(TString&)>;
    static void UploadTable(const TString& table, ui32 expectedRows, TRowProvider&& rowProvider,
                            const NTable::TTableDescription& descr, NTable::TTableClient& client);

    static TVector<ui32> CalcTableSplitBoundaries(const TString& table, ui32 partitionSizeMb, const TString& filename);

    NQuery::TExecuteQueryIterator RunQueryGeneric(ui32 number, bool profile);

    const TDriver& Driver;
    const TString TablesPath;
};

} // namespace NYdb::NTpch
