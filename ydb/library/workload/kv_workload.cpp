#include "kv_workload.h"
#include "format"
#include "util/random/random.h"

#include <util/datetime/base.h>

#include <ydb/core/util/lz4_data_generator.h>

#include <cmath>
#include <iomanip>
#include <string>
#include <thread>
#include <random>
#include <sstream>
#include <chrono>

template <>
void Out<NYdbWorkload::KvWorkloadConstants>(IOutputStream& out, NYdbWorkload::KvWorkloadConstants constant)
{
    out << static_cast<ui64>(constant);
}

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
    Y_FAIL();
}

void AddResultSet(const NYdb::TResultSet& resultSet, TVector<TRow>& rows) {
    NYdb::TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        TRow row;
        
        for (size_t col = 0; col < parser.ColumnsCount(); col++) {
            auto& valueParser = parser.ColumnParser(col);
            if (valueParser.GetPrimitiveType() == NYdb::EPrimitiveType::Uint64) {
                row.Ints.push_back(valueParser.GetUint64());
            } else {
                row.Strings.push_back(valueParser.GetString());
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
    : Params(*params)
    , BigString(NKikimr::GenDataForLZ4(Params.StringLen))
{
    if (Params.MixedChangePartitionsSize) {
        MixedNextChangePartitionsSize = Now();
    } else {
        MixedNextChangePartitionsSize = TInstant::Max();
    }

    Y_VERIFY(Params.IntColumnsCnt <= Params.ColumnsCnt);
    Y_VERIFY(Params.KeyColumnsCnt <= Params.ColumnsCnt);
}

TKvWorkloadParams* TKvWorkloadGenerator::GetParams() {
    return &Params;
}

std::string TKvWorkloadGenerator::GetDDLQueries() const {
    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << Params.DbPath << "/" << Params.TableName << "`(";

    for (size_t i = 0; i < Params.ColumnsCnt; ++i) {
        if (i < Params.IntColumnsCnt) {
            ss << "c" << i << " Uint64 NOT NULL, ";
        } else {
            ss << "c" << i << " String NOT NULL, ";
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

    ss << ") VALUES (";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
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

    return TQueryInfoList(1, TQueryInfo(ss.str(), std::move(params)));
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

        upsertQuery.front().DataQueryResultCallback = [](NYdb::NTable::TDataQueryResult queryResult) {
            if (queryResult.IsSuccess()) {
                // Note: helps to keep old values too
                if (RandomNumber<ui32>(1000) == 0) {
                    RowsVerifyer.TryInsert(lastRow);
                }
            }
        };

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
        Y_VERIFY(doReadRows ^ doSelect);

        if (doSelect) {
            auto selectQuery = Select(std::move(rows));

            if (checkRow) {
                selectQuery.front().DataQueryResultCallback = [](NYdb::NTable::TDataQueryResult queryResult) {
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

std::string TKvWorkloadGenerator::GetCleanDDLQueries() const {
    std::string query = "DROP TABLE `" + Params.TableName + "`;";

    return query;
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


}