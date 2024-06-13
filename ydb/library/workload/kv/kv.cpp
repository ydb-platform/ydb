#include "kv.h"
#include <util/generic/serialized_enum.h>
#include <util/random/random.h>
#include <util/datetime/base.h>

#include <ydb/core/util/lz4_data_generator.h>

#include <cmath>
#include <iomanip>
#include <string>
#include <thread>
#include <random>
#include <sstream>
#include <chrono>
#include <format>

namespace NYdbWorkload {

using TRow = TKvWorkloadGenerator::TRow;

// Note: there is no mechanism to update row values for now so all keys should be different
struct TRowsVerifyer {
    TRowsVerifyer(size_t capacity = 10000)
        : Capacity(capacity)
    {
        Rows.reserve(Capacity);
    }

    void TryInsert(TRow row) {
        std::unique_lock<std::mutex> lock(Mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            return;
        }

        if (Rows.size() < Capacity) {
            Rows.push_back(row);
            return;
        }

        Rows[RandomNumber<size_t>(Rows.size())] = row;
    }

    std::optional<TRow> GetRandom() {
        std::unique_lock<std::mutex> lock(Mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            return { };
        }

        if (!Rows.size()) {
            return { };
        }

        size_t index = RandomNumber<size_t>(Rows.size());
        return Rows[index];
    }

private:
    const size_t Capacity;
    std::mutex Mutex;
    TVector<TRow> Rows;
};

TRowsVerifyer RowsVerifyer;

void Fail() {
    // Note: sleep helps to detect more fails
    std::this_thread::sleep_for(std::chrono::seconds(3));
    Y_ABORT();
}

void AddResultSet(const NYdb::TResultSet& resultSet, TVector<TRow>& rows) {
    NYdb::TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        TRow row;

        for (size_t col = 0; col < parser.ColumnsCount(); col++) {
            auto& valueParser = parser.ColumnParser(col);
            bool optional = valueParser.GetKind() == NYdb::TTypeParser::ETypeKind::Optional; 
            if (optional) {
                valueParser.OpenOptional();
            }
            if (valueParser.GetPrimitiveType() == NYdb::EPrimitiveType::Uint64) {
                row.Ints.push_back(valueParser.GetUint64());
            } else {
                row.Strings.push_back(valueParser.GetString());
            }
            if (optional) {
                valueParser.CloseOptional();
            }
        }

        rows.push_back(std::move(row));
    }
}

void VerifyRows(const TRow& checkRow, const TVector<TRow>& readRows, TString message) {
    if (readRows.empty()) {
        Cerr << "Expected to have " << checkRow.ToString()
            << " but got empty "
            << message
            << Endl;

        Fail();
    }

    if (readRows.size() > 1) {
        Cerr << "Expected to have " << checkRow.ToString()
            << " but got " << readRows.size() << " rows "
            << message
            << Endl;

        for (auto r : readRows) {
            Cerr << r.ToString() << Endl;
        }

        Fail();
    }

    if (readRows[0] != checkRow) {
        Cerr << "Expected to have " << checkRow.ToString()
            << " but got " << readRows[0].ToString() << " "
            << message
            << Endl;

        Fail();
    } else {
        // Cerr << "OK " << checkRow.ToString() << " " << message << Endl;
    }
}


TKvWorkloadGenerator::TKvWorkloadGenerator(const TKvWorkloadParams* params)
    : TBase(params)
    , BigString(NKikimr::GenDataForLZ4(Params.StringLen))
{
    if (Params.MixedChangePartitionsSize) {
        MixedNextChangePartitionsSize = Now();
    } else {
        MixedNextChangePartitionsSize = TInstant::Max();
    }

    Y_ABORT_UNLESS(Params.IntColumnsCnt <= Params.ColumnsCnt);
    Y_ABORT_UNLESS(Params.KeyColumnsCnt <= Params.ColumnsCnt);
}

std::string TKvWorkloadGenerator::GetDDLQueries() const {
    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << Params.DbPath << "/" << Params.TableName << "`(";

    for (size_t i = 0; i < Params.ColumnsCnt; ++i) {
        if (i < Params.IntColumnsCnt) {
            ss << "c" << i << " Uint64, ";
        } else {
            ss << "c" << i << " String, ";
        }
    }

    ss << "PRIMARY KEY(";
    for (size_t i = 0; i < Params.KeyColumnsCnt; ++i) {
        ss << "c" << i;
        if (i + 1 < Params.KeyColumnsCnt) {
            ss << ", ";
        }
    }
    ss << ")) WITH (";

    if (Params.PartitionsByLoad) {
        ss << "AUTO_PARTITIONING_BY_LOAD = ENABLED, ";
    }
    ss << "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions << ", ";
    ss << "AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb << ", ";
    ss << "UNIFORM_PARTITIONS = " << Params.MinPartitions << ", ";
    ss << "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << Max(Params.MinPartitions, Params.MaxPartitions) << ")";

    return ss.str();
}

TQueryInfoList TKvWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::UpsertRandom:
            return Upsert(GenerateRandomRows());
        case EType::InsertRandom:
            return Insert(GenerateRandomRows());
        case EType::SelectRandom:
            return Select(GenerateRandomRows());
        case EType::ReadRowsRandom:
            return ReadRows(GenerateRandomRows());
        case EType::Mixed:
            return Mixed();
        default:
            return TQueryInfoList();
    }
}


TVector<IWorkloadQueryGenerator::TWorkloadType> TKvWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::UpsertRandom), "upsert", "Upsert random rows into table");
    result.emplace_back(static_cast<int>(EType::InsertRandom), "insert", "Insert random rows into table");
    result.emplace_back(static_cast<int>(EType::SelectRandom), "select", "Select rows matching primary key(s)");
    result.emplace_back(static_cast<int>(EType::ReadRowsRandom), "read-rows", "ReadRows rows matching primary key(s)");
    result.emplace_back(static_cast<int>(EType::Mixed), "mixed", "Writes and SELECT/ReadsRows rows randomly, verifies them");
    return result;
}

TQueryInfoList TKvWorkloadGenerator::WriteRows(TString operation, TVector<TRow>&& rows) {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
            TString cname = "$c" + std::to_string(row) + "_" + std::to_string(col);
            if (col < Params.IntColumnsCnt) {
                ss << "DECLARE " << cname << " AS Uint64;\n";
            } else {
                ss << "DECLARE " << cname << " AS String;\n";
            }
            if (col < Params.IntColumnsCnt) {
                paramsBuilder.AddParam(cname).Uint64(rows[row].Ints[col]).Build();
            } else {
                paramsBuilder.AddParam(cname).String(rows[row].Strings[col - Params.IntColumnsCnt]).Build();
            }
        }
    }

    ss << operation << " INTO `" << Params.TableName << "` (";

    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ", ";
        }
    }

    ss << ") VALUES ";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        ss << "(";

        for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
            ss << "$c" << row << "_" << col;
            if (col + 1 < Params.ColumnsCnt) {
                ss << ", ";
            }
        }

        ss << ")";

        if (row + 1 < Params.RowsCnt) {
            ss << ", ";
        }
    }

    auto params = paramsBuilder.Build();

    return TQueryInfoList(1, TQueryInfo(ss.str(), std::move(params)));
}

TQueryInfoList TKvWorkloadGenerator::Upsert(TVector<TRow>&& rows) {
    return WriteRows("UPSERT", std::move(rows));
}

TQueryInfoList TKvWorkloadGenerator::Insert(TVector<TRow>&& rows) {
    return WriteRows("INSERT", std::move(rows));
}

TQueryInfoList TKvWorkloadGenerator::Select(TVector<TRow>&& rows) {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            TString paramName = "$r" + std::to_string(row) + "_" + std::to_string(col);
            if (col < Params.IntColumnsCnt) {
                ss << "DECLARE " << paramName << " AS Uint64;\n";
                paramsBuilder.AddParam(paramName).Uint64(rows[row].Ints[col]).Build();
            } else {
                ss << "DECLARE " << paramName << " AS String;\n";
                paramsBuilder.AddParam(paramName).String(rows[row].Strings[col - Params.IntColumnsCnt]).Build();
            }
        }
    }

    ss << "SELECT ";
    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ",";
        }
        ss << " ";
    }

    ss << "FROM `" << Params.TableName << "` WHERE ";
    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            TString paramName = "$r" + std::to_string(row) + "_" + std::to_string(col);
            ss << "c" << col << " = " << paramName;
            if (col + 1 < Params.KeyColumnsCnt) {
                ss << " AND ";
            }
        }
        if (row + 1 < Params.RowsCnt) {
            ss << " OR ";
        }
    }

    auto params = paramsBuilder.Build();
    TQueryInfo info(ss.str(), std::move(params));
    info.UseStaleRO = Params.StaleRO;
    return TQueryInfoList(1, std::move(info));
}

TQueryInfoList TKvWorkloadGenerator::ReadRows(TVector<TRow>&& rows) {
    NYdb::TValueBuilder keys;
    keys.BeginList();
    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        keys.AddListItem().BeginStruct();
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            if (col < Params.IntColumnsCnt) {
                keys.AddMember("c" + std::to_string(col)).Uint64(rows[row].Ints[col]);
            } else {
                keys.AddMember("c" + std::to_string(col)).String(rows[row].Strings[col - Params.IntColumnsCnt]);
            }
        }
        keys.EndStruct();
    }
    keys.EndList();

    TQueryInfo info;
    info.TablePath = Params.DbPath + "/" + Params.TableName;
    info.UseReadRows = true;
    info.KeyToRead = keys.Build();
    return TQueryInfoList(1, std::move(info));
}

TQueryInfoList TKvWorkloadGenerator::Mixed() {
    static thread_local TRow lastRow;

    if (Params.MixedChangePartitionsSize && RandomNumber<ui32>(1000) == 0) {
        TInstant nextChange = MixedNextChangePartitionsSize;
        if (nextChange <= Now() && MixedNextChangePartitionsSize.compare_exchange_strong(nextChange, nextChange + TDuration::Seconds(RandomNumber<ui32>(60*20)))) {
            auto alterTable = NYdb::NTable::TAlterTableSettings()
                .AlterPartitioningSettings(NYdb::NTable::TPartitioningSettingsBuilder()
                    .SetPartitionSizeMb(RandomNumber<ui32>(1000) + 10).Build());

            TQueryInfo info;
            info.TablePath = Params.DbPath + "/" + Params.TableName;
            info.AlterTable = alterTable;
            return TQueryInfoList(1, std::move(info));
        }
    }

    if (RandomNumber<ui32>(2) == 0) { // write
        TVector<TRow> rows = GenerateRandomRows(true);
        lastRow = rows[0];

        auto upsertQuery = Upsert(std::move(rows));

        auto callback = [](auto queryResult) {
            if (queryResult.IsSuccess()) {
                // Note: helps to keep old values too
                if (RandomNumber<ui32>(1000) == 0) {
                    RowsVerifyer.TryInsert(lastRow);
                }
            }
        };
        upsertQuery.front().DataQueryResultCallback = callback;
        upsertQuery.front().GenericQueryResultCallback = callback;

        return upsertQuery;
    } else { // read
        auto checkRow = RowsVerifyer.GetRandom();
        if (checkRow) {
            lastRow = checkRow.value();
        } else {
            lastRow.Ints.clear();
            lastRow.Strings.clear();
        }

        TVector<TRow> rows = checkRow ? TVector<TRow>{checkRow.value()} : GenerateRandomRows(true);

        bool doReadRows = false;
        bool doSelect = false;
        if (!Params.MixedDoReadRows || !Params.MixedDoSelect) {
            if (Params.MixedDoReadRows) doReadRows = true;
            if (Params.MixedDoSelect) doSelect = true;
        } else {
            if (RandomNumber<ui32>(2) == 0)
                doReadRows = true;
            else
                doSelect = true;
        }
        Y_ABORT_UNLESS(doReadRows ^ doSelect);

        if (doSelect) {
            auto selectQuery = Select(std::move(rows));

            if (checkRow) {
                auto callback = [](auto queryResult) {
                    if (queryResult.IsSuccess()) {
                        TVector<TRow> readRows;
                        for (auto& resultSet : queryResult.GetResultSets()) {
                            AddResultSet(resultSet, readRows);
                        }
                        TString queryStatus = "Status: " + std::to_string(int(queryResult.GetStatus())) + " Method: SELECT" + " Issues: " + queryResult.GetIssues().ToOneLineString() + " Meta: ";
                        for (auto m : queryResult.GetResponseMetadata()) {
                            queryStatus +=  " " + m.first + "=" + m.second;
                        }
                        VerifyRows(lastRow, std::move(readRows), queryStatus);
                    }
                };
                selectQuery.front().DataQueryResultCallback = callback;
                selectQuery.front().GenericQueryResultCallback = callback;
            }

            return selectQuery;
        }
        if (doReadRows) {
            auto readRowsQuery = ReadRows(std::move(rows));

            if (checkRow) {
                readRowsQuery.front().ReadRowsResultCallback = [](NYdb::NTable::TReadRowsResult queryResult) {
                    if (queryResult.IsSuccess()) {
                        TVector<TRow> readRows;
                        AddResultSet(queryResult.GetResultSet(), readRows);
                        TString queryStatus = "Status: " + std::to_string(int(queryResult.GetStatus())) + " Method: ReadRows" + " Issues: " + queryResult.GetIssues().ToOneLineString() + " Meta: ";
                        for (auto m : queryResult.GetResponseMetadata()) {
                            queryStatus +=  " " + m.first + "=" + m.second;
                        }
                        VerifyRows(lastRow, std::move(readRows), queryStatus);
                    }
                };
            }

            return readRowsQuery;
        }
    }

    return TQueryInfoList();
}

TQueryInfoList TKvWorkloadGenerator::GetInitialData() {
    TQueryInfoList res;
    for (size_t i = 0; i < Params.InitRowCount; ++i) {
        auto queryInfos = Upsert(GenerateRandomRows());
        res.insert(res.end(), queryInfos.begin(), queryInfos.end());
    }

    return res;
}

TVector<std::string> TKvWorkloadGenerator::GetCleanPaths() const {
    return { Params.TableName };
}

TVector<TRow> TKvWorkloadGenerator::GenerateRandomRows(bool randomValues) {
    TVector<TRow> result(Params.RowsCnt);

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        result[row].Ints.resize(Params.IntColumnsCnt);
        result[row].Strings.resize(Params.ColumnsCnt - Params.IntColumnsCnt);

        for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
            if (col < Params.IntColumnsCnt) {
                ui64 val = col < Params.KeyColumnsCnt ? RandomNumber<ui64>(Params.MaxFirstKey) : RandomNumber<ui64>();
                result[row].Ints[col] = val;
            } else {
                TString val;
                if (col < Params.KeyColumnsCnt) {
                    val = TString(std::format("{:x}", RandomNumber<ui64>(Params.MaxFirstKey)));
                } else if (randomValues) {
                    val = TString(Params.StringLen, '_');
                    for (size_t i = 0; i < Params.StringLen; i++) {
                        val[i] = (char)('a' + RandomNumber<u_char>(26));
                    }
                } else {
                    val = BigString;
                }
                result[row].Strings[col - Params.IntColumnsCnt] = val;
            }
        }
    }

    return result;
}

void TKvWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("init-upserts", "count of upserts need to create while table initialization")
            .DefaultValue((ui64)KvWorkloadConstants::INIT_ROW_COUNT).StoreResult(&InitRowCount);
        opts.AddLongOption("min-partitions", "Minimum partitions for tables.")
            .DefaultValue((ui64)KvWorkloadConstants::MIN_PARTITIONS).StoreResult(&MinPartitions);
        opts.AddLongOption("max-partitions", "Maximum partitions for tables.")
            .DefaultValue((ui64)KvWorkloadConstants::MAX_PARTITIONS).StoreResult(&MaxPartitions);
        opts.AddLongOption("partition-size", "Maximum partition size in megabytes (AUTO_PARTITIONING_PARTITION_SIZE_MB).")
            .DefaultValue((ui64)KvWorkloadConstants::PARTITION_SIZE_MB).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("auto-partition", "Enable auto partitioning by load.")
            .DefaultValue((ui64)KvWorkloadConstants::PARTITIONS_BY_LOAD).StoreResult(&PartitionsByLoad);
        opts.AddLongOption("max-first-key", "Maximum value of a first primary key")
            .DefaultValue((ui64)KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
        opts.AddLongOption("len", "String len")
            .DefaultValue((ui64)KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
        opts.AddLongOption("cols", "Number of columns")
            .DefaultValue((ui64)KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue((ui64)KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue((ui64)KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
        opts.AddLongOption("rows", "Number of rows")
            .DefaultValue((ui64)KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
        break;
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption("max-first-key", "Maximum value of a first primary key")
            .DefaultValue((ui64)KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue((ui64)KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue((ui64)KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
        switch (static_cast<TKvWorkloadGenerator::EType>(workloadType)) {
        case TKvWorkloadGenerator::EType::UpsertRandom:
            opts.AddLongOption("len", "String len")
                .DefaultValue((ui64)KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
            opts.AddLongOption("cols", "Number of columns to upsert")
                .DefaultValue((ui64)KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
            opts.AddLongOption("rows", "Number of rows to upsert")
                .DefaultValue((ui64)NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
            break;
        case TKvWorkloadGenerator::EType::InsertRandom:
            opts.AddLongOption("len", "String len")
                .DefaultValue((ui64)KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
            opts.AddLongOption("cols", "Number of columns to insert")
                .DefaultValue((ui64)KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
            opts.AddLongOption("rows", "Number of rows to insert")
                .DefaultValue((ui64)NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
            break;
        case TKvWorkloadGenerator::EType::SelectRandom:
            opts.AddLongOption("cols", "Number of columns to select for a single query")
                .DefaultValue((ui64)KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
            opts.AddLongOption("rows", "Number of rows to select for a single query")
                .DefaultValue((ui64)NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
            opts.AddLongOption("stale-ro", "Read with Stale Read Only mode")
                .DefaultValue((ui64)KvWorkloadConstants::STALE_RO).StoreResult(&StaleRO);
            break;
        case TKvWorkloadGenerator::EType::ReadRowsRandom:
            opts.AddLongOption("cols", "Number of columns to select for a single query")
                .DefaultValue((ui64)KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
            opts.AddLongOption("rows", "Number of rows to select for a single query")
                .DefaultValue((ui64)NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
            break;
        case TKvWorkloadGenerator::EType::Mixed:
            opts.AddLongOption("len", "String len")
                .DefaultValue((ui64)KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
            opts.AddLongOption("cols", "Number of columns")
                .DefaultValue((ui64)KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
            opts.AddLongOption("change-partitions-size", "Apply random changes of AUTO_PARTITIONING_PARTITION_SIZE_MB setting")
                .DefaultValue((ui64)KvWorkloadConstants::MIXED_CHANGE_PARTITIONS_SIZE).StoreResult(&MixedChangePartitionsSize);
            opts.AddLongOption("do-select", "Do SELECT operations")
                .DefaultValue((ui64)KvWorkloadConstants::MIXED_DO_SELECT).StoreResult(&MixedDoSelect);
            opts.AddLongOption("do-read-rows", "Do ReadRows operations")
                .DefaultValue((ui64)KvWorkloadConstants::MIXED_DO_READ_ROWS).StoreResult(&MixedDoReadRows);
        }
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TKvWorkloadParams::CreateGenerator() const {
    return MakeHolder<TKvWorkloadGenerator>(this);
}

TString TKvWorkloadParams::GetWorkloadName() const {
    return "Key-Value";
}

}
