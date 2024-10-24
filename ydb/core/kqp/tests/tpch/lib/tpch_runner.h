#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

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

    NTable::TScanQueryPartIterator RunQuery(ui32 number, bool profile = false, bool usePersistentSnapshot = true);

    NTable::TScanQueryPartIterator RunQuery01(TInstant startDate = TInstant::ParseIso8601("1998-12-01"),
                                                  TDuration startDateShift = TDuration::Days(3),
                                                  bool profile = false, bool usePersistentSnapshot = true);

    // todo: replace hardcoded query parameters with function's arguments
    NTable::TScanQueryPartIterator RunQuery02(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery03(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery04(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery05(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery06(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery07(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery08(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery09(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery10(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery11(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery12(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery13(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery14(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery15(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery16(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery17(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery18(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery19(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery20(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery21(bool profile = false, bool usePersistentSnapshot = true);
    NTable::TScanQueryPartIterator RunQuery22(bool profile = false, bool usePersistentSnapshot = true);

private:
    using TRowProvider = std::function<bool(TString&)>;
    static void UploadTable(const TString& table, ui32 expectedRows, TRowProvider&& rowProvider,
                            const NTable::TTableDescription& descr, NTable::TTableClient& client);

    static TVector<ui32> CalcTableSplitBoundaries(const TString& table, ui32 partitionSizeMb, const TString& filename);

    NTable::TScanQueryPartIterator RunQueryGeneric(ui32 number, bool profile, bool usePersistentSnapshot);

    const TDriver& Driver;
    const TString TablesPath;
};

} // namespace NYdb::NTpch
