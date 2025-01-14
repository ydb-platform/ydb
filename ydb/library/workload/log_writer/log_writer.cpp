#include "log_writer.h"
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

namespace NLogWriter {

using TRow = TLogWriterWorkloadGenerator::TRow;

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


TLogWriterWorkloadGenerator::TLogWriterWorkloadGenerator(const TLogWriterWorkloadParams* params)
    : TBase(params)
    , TotalColumnsCnt(1 + Params.IntColumnsCnt + Params.StrColumnsCnt)
{
    Y_ABORT_UNLESS(TotalColumnsCnt >= Params.KeyColumnsCnt);
}

std::string TLogWriterWorkloadGenerator::GetDDLQueries() const {
    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << Params.DbPath << "/" << Params.TableName << "`(";

    for (size_t i = 0; i < TotalColumnsCnt; ++i) {
        if (i == 0)
        {
            ss << "ts Timestamp";

        }
        else if (i < Params.IntColumnsCnt + 1) {
            ss << "c" << i << " Uint64";
        }
        else
        {
            ss << "c" << i << " String";
        }
        if (i < Params.KeyColumnsCnt && Params.GetStoreType() == TLogWriterWorkloadParams::EStoreType::Column) {
            ss << " NOT NULL";
        }
        ss << ", ";
    }

    ss << "PRIMARY KEY(";
    ss << "ts, ";
    for (size_t i = 1; i < Params.KeyColumnsCnt; ++i) {
        ss << "c" << i;
        if (i + 1 < Params.KeyColumnsCnt) {
            ss << ", ";
        }
    }
    ss << ")) WITH (";

    ss << "TTL = Interval(\"PT7H\") ON ts, ";

    switch (Params.GetStoreType()) {

        case TLogWriterWorkloadParams::EStoreType::Row:
            ss << "STORE = ROW, ";
            if (Params.PartitionsByLoad) {
                ss << "AUTO_PARTITIONING_BY_LOAD = ENABLED, ";
            }
            ss << "UNIFORM_PARTITIONS = " << Params.MinPartitions << ", ";
            ss << "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << Max(Params.MinPartitions, Params.MaxPartitions) << ", ";
            ss << "AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb << ", ";
            break;
        case TLogWriterWorkloadParams::EStoreType::Column:
            ss << "STORE = COLUMN, ";
            break;
        default:
            throw yexception() << "Unsupported store type: " << Params.GetStoreType();
    }
    ss << "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions << ")";
    return ss.str();
}

TQueryInfoList TLogWriterWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::Upsert:
            return Upsert(GenerateRandomRows());
        case EType::BulkUpsert:
            return BulkUpsert(GenerateRandomRows());
        default:
            return TQueryInfoList();
    }
}


TVector<IWorkloadQueryGenerator::TWorkloadType> TLogWriterWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::Upsert), "upsert", "Upsert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::BulkUpsert), "bulk_upsert", "Bulk upsert random rows into table near current ts");
    return result;
}

TQueryInfoList TLogWriterWorkloadGenerator::WriteRows(TString operation, TVector<TRow>&& rows) {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < TotalColumnsCnt; ++col) {
            TString cname = "$c" + std::to_string(row) + "_" + std::to_string(col);
            if (col == 0)
            {
                ss << "DECLARE " << cname << " AS Timestamp;\n";
                paramsBuilder.AddParam(cname).Timestamp(rows[row].Ts).Build();
            }
            else if (col < Params.IntColumnsCnt + 1) {
                ss << "DECLARE " << cname << " AS Uint64;\n";
                paramsBuilder.AddParam(cname).Uint64(rows[row].Ints[col - 1]).Build();
            } else {
                ss << "DECLARE " << cname << " AS String;\n";
                paramsBuilder.AddParam(cname).String(rows[row].Strings[col - Params.IntColumnsCnt - 1]).Build();
            }
        }
    }

    ss << operation << " INTO `" << Params.TableName << "` (";

    for (size_t col = 0; col < TotalColumnsCnt; ++col) {
        if (col != 0)
        {
            ss << "c" << col;
        }
        else
        {
            ss << "ts";
        }
        if (col + 1 < TotalColumnsCnt) {
            ss << ", ";
        }
    }

    ss << ") VALUES ";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        ss << "(";

        for (size_t col = 0; col < TotalColumnsCnt; ++col) {
            ss << "$c" << row << "_" << col;
            if (col + 1 < TotalColumnsCnt) {
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

TQueryInfoList TLogWriterWorkloadGenerator::Upsert(TVector<TRow>&& rows) {
    return WriteRows("UPSERT", std::move(rows));
}

TQueryInfoList TLogWriterWorkloadGenerator::BulkUpsert(TVector<TRow>&& rows) {
    NYdb::TValueBuilder valueBuilder;
    valueBuilder.BeginList();
    for (const TRow& row : rows) {
        auto &listItem = valueBuilder.AddListItem();
        listItem.BeginStruct();
        for (size_t col = 0; col < TotalColumnsCnt; ++col) {
            if (col == 0)
            {
                listItem.AddMember("ts").Timestamp(row.Ts);
            }
            else if (col < Params.IntColumnsCnt + 1) {
                listItem.AddMember(std::format("c{}", col)).Uint64(row.Ints[col-1]);
            } else {
                listItem.AddMember(std::format("c{}", col)).String(row.Strings[col - Params.IntColumnsCnt - 1]);
            }
        }
        listItem.EndStruct();
    }
    valueBuilder.EndList();
    TString table_path = Params.DbPath + "/" + Params.TableName;
    NYdb::TValue rowsValue = valueBuilder.Build();
    auto bulkUpsertOperation = [table_path, rowsValue](NYdb::NTable::TTableClient& tableClient) {
        auto r = rowsValue;
        auto status = tableClient.BulkUpsert(table_path, std::move(r));
        return status.GetValueSync();
    };
    TQueryInfo queryInfo;
    queryInfo.TableOperation = bulkUpsertOperation;
    return TQueryInfoList(1, std::move(queryInfo));
}


TQueryInfoList TLogWriterWorkloadGenerator::GetInitialData() {
    TQueryInfoList res;
    for (size_t i = 0; i < Params.InitRowCount; ++i) {
        auto queryInfos = Upsert(GenerateRandomRows());
        res.insert(res.end(), queryInfos.begin(), queryInfos.end());
    }

    return res;
}

TVector<std::string> TLogWriterWorkloadGenerator::GetCleanPaths() const {
    return { Params.TableName };
}

TVector<TRow> TLogWriterWorkloadGenerator::GenerateRandomRows() {
    TVector<TRow> result(Params.RowsCnt);

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        result[row].Ts = TInstant::Now();
        result[row].Ints.resize(Params.IntColumnsCnt);
        result[row].Strings.resize(Params.StrColumnsCnt);

        for (size_t col = 0; col < Params.IntColumnsCnt; ++col) {
            ui64 val = RandomNumber<ui64>();
            result[row].Ints[col] = val;
        }

        for (size_t col = 0; col < Params.StrColumnsCnt; ++col) {
            TString val;
            val = TString(Params.StringLen, '_');
            for (size_t i = 0; i < Params.StringLen; i++) {
                val[i] = (char)('a' + RandomNumber<u_char>(26));
            }
            result[row].Strings[col] = val;
        }
    }

    return result;
}

void TLogWriterWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    opts.AddLongOption('p', "path", "Path where benchmark tables are located")
        .Optional()
        .DefaultValue(TableName)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            while(arg.SkipPrefix("/"));
            while(arg.ChopSuffix("/"));
            TableName = arg;
        });
    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("init-upserts", "count of upserts need to create while table initialization")
            .DefaultValue((ui64)LogWriterWorkloadConstants::INIT_ROW_COUNT).StoreResult(&InitRowCount);
        opts.AddLongOption("min-partitions", "Minimum partitions for tables.")
            .DefaultValue((ui64)LogWriterWorkloadConstants::MIN_PARTITIONS).StoreResult(&MinPartitions);
        opts.AddLongOption("max-partitions", "Maximum partitions for tables.")
            .DefaultValue((ui64)LogWriterWorkloadConstants::MAX_PARTITIONS).StoreResult(&MaxPartitions);
        opts.AddLongOption("partition-size", "Maximum partition size in megabytes (AUTO_PARTITIONING_PARTITION_SIZE_MB).")
            .DefaultValue((ui64)LogWriterWorkloadConstants::PARTITION_SIZE_MB).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("auto-partition", "Enable auto partitioning by load.")
            .DefaultValue((ui64)LogWriterWorkloadConstants::PARTITIONS_BY_LOAD).StoreResult(&PartitionsByLoad);
        opts.AddLongOption("len", "String len")
            .DefaultValue((ui64)LogWriterWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue((ui64)LogWriterWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("str-cols", "Number of columns")
            .DefaultValue((ui64)LogWriterWorkloadConstants::STR_COLUMNS_CNT).StoreResult(&StrColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue((ui64)LogWriterWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
        opts.AddLongOption("rows", "Number of rows")
            .DefaultValue((ui64)LogWriterWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
        opts.AddLongOption("store", "Storage type."
                " Options: row, column\n"
                "  row - use row-based storage engine;\n"
                "  column - use column-based storage engine.")
            .DefaultValue(StoreType)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                const auto l = to_lower(TString(arg));
                if (!TryFromString(arg, StoreType)) {
                    throw yexception() << "Ivalid store type: " << arg;
                }
            });
        break;
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption("str-cols", "Number of int columns")
            .DefaultValue((ui64)LogWriterWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&StrColumnsCnt);
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue((ui64)LogWriterWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue((ui64)LogWriterWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
        switch (static_cast<TLogWriterWorkloadGenerator::EType>(workloadType)) {
        case TLogWriterWorkloadGenerator::EType::BulkUpsert:
        case TLogWriterWorkloadGenerator::EType::Upsert:
            opts.AddLongOption("len", "String len")
                .DefaultValue((ui64)LogWriterWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
            opts.AddLongOption("rows", "Number of rows to upsert")
                .DefaultValue((ui64)LogWriterWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
            break;
        }
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TLogWriterWorkloadParams::CreateGenerator() const {
    return MakeHolder<TLogWriterWorkloadGenerator>(this);
}

TString TLogWriterWorkloadParams::GetWorkloadName() const {
    return "Log Writer";
}

} // namespace NYdbWorkload

} // namespace NLogWriter
