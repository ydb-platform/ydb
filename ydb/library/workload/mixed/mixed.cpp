#include "mixed.h"
#include <util/generic/serialized_enum.h>
#include <util/random/normal.h>
#include <util/random/random.h>
#include <util/datetime/base.h>

namespace NYdbWorkload {

namespace NMixed {

using TRow = TLogGenerator::TRow;


TLogGenerator::TLogGenerator(const TMixedWorkloadParams* params)
    : TBase(params)
    , TotalColumnsCnt(1 + Params.IntColumnsCnt + Params.StrColumnsCnt)
{
    Y_ABORT_UNLESS(TotalColumnsCnt >= Params.KeyColumnsCnt);
}

std::string TLogGenerator::GetDDLQueries() const {
    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << Params.DbPath << "/" << Params.TableName << "`(";

    for (size_t i = 0; i < TotalColumnsCnt; ++i) {
        if (i == 0) {
            ss << "ts Timestamp";
        } else if (i < Params.IntColumnsCnt + 1) {
            ss << "c" << i << " Uint64";
        } else {
            ss << "c" << i << " String";
        }

        if (i < Params.KeyColumnsCnt && Params.GetStoreType() == TMixedWorkloadParams::EStoreType::Column) {
            ss << " NOT NULL";
        }
        ss << ", ";
    }

    ss << "PRIMARY KEY(";
    ss << "ts";
    for (size_t i = 1; i < Params.KeyColumnsCnt; ++i) {
        ss << ", c" << i;
    }
    ss << ")) WITH (";

    ss << "TTL = Interval(\"PT" << Params.TimestampTtlMinutes << "M\") ON ts, ";

    switch (Params.GetStoreType()) {
        case TMixedWorkloadParams::EStoreType::Row:
            ss << "STORE = ROW, ";
            break;
        case TMixedWorkloadParams::EStoreType::Column:
            ss << "STORE = COLUMN, ";
            break;
        default:
            throw yexception() << "Unsupported store type: " << Params.GetStoreType();
    }
    if (Params.PartitionsByLoad) {
        ss << "AUTO_PARTITIONING_BY_LOAD = ENABLED, ";
    }
    ss << "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << Max(Params.MinPartitions, Params.MaxPartitions) << ", ";
    ss << "AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb << ", ";
    ss << "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions << ")";
    return ss.str();
}

TQueryInfoList TLogGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::Insert:
            return Insert(GenerateRandomRows());
        case EType::Upsert:
            return Upsert(GenerateRandomRows());
        case EType::BulkUpsert:
            return BulkUpsert(GenerateRandomRows());
        case EType::Select:
            return Select(GenerateRandomRows());
        default:
            return TQueryInfoList();
    }
}


TVector<IWorkloadQueryGenerator::TWorkloadType> TLogGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::Insert), "insert", "Insert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::Upsert), "upsert", "Upsert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::BulkUpsert), "bulk_upsert", "Bulk upsert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::Select), "select", "Select random rows from table");
    return result;
}

TQueryInfoList TLogGenerator::WriteRows(TString operation, TVector<TRow>&& rows) {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < TotalColumnsCnt; ++col) {
            TString cname = "$c" + std::to_string(row) + "_" + std::to_string(col);
            if (col == 0) {
                ss << "DECLARE " << cname << " AS Timestamp;\n";
                paramsBuilder.AddParam(cname).Timestamp(rows[row].Ts).Build();
            } else if (col < Params.IntColumnsCnt + 1) {
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
        if (col != 0) {
            ss << "c" << col;
        } else {
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

TQueryInfoList TLogGenerator::Insert(TVector<TRow>&& rows) {
    return WriteRows("INSERT", std::move(rows));
}

TQueryInfoList TLogGenerator::Upsert(TVector<TRow>&& rows) {
    return WriteRows("UPSERT", std::move(rows));
}

TQueryInfoList TLogGenerator::Select(TVector<TRow>&& rows) {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            TString paramName = "$r" + std::to_string(row) + "_" + std::to_string(col);
            if (col == 0) {
                ss << "DECLARE " << paramName << " AS Timestamp;\n";
                paramsBuilder.AddParam(paramName).Timestamp(rows[row].Ts).Build();
            } else if (col < Params.IntColumnsCnt) {
                ss << "DECLARE " << paramName << " AS Uint64;\n";
                paramsBuilder.AddParam(paramName).Uint64(rows[row].Ints[col]).Build();
            } else {
                ss << "DECLARE " << paramName << " AS String;\n";
                paramsBuilder.AddParam(paramName).String(rows[row].Strings[col - Params.IntColumnsCnt]).Build();
            }
        }
    }

    ss << "SELECT ";
    for (size_t col = 1; col <= TotalColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < TotalColumnsCnt) {
            ss << ",";
        }
        ss << " ";
    }

    ss << "FROM `" << Params.TableName << "` WHERE ";
    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            TString paramName = "$r" + std::to_string(row) + "_" + std::to_string(col);
            if (col == 0) {
                ss << "ts = " << paramName;
            } else {
                ss << "c" << col << " = " << paramName;
            }
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
    return TQueryInfoList(1, std::move(info));
}

TQueryInfoList TLogGenerator::BulkUpsert(TVector<TRow>&& rows) {
    NYdb::TValueBuilder valueBuilder;
    valueBuilder.BeginList();
    for (const TRow& row : rows) {
        auto &listItem = valueBuilder.AddListItem();
        listItem.BeginStruct();
        for (size_t col = 0; col < TotalColumnsCnt; ++col) {
            if (col == 0) {
                listItem.AddMember("ts").Timestamp(row.Ts);
            } else if (col < Params.IntColumnsCnt + 1) {
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


TQueryInfoList TLogGenerator::GetInitialData() {
    TQueryInfoList res;
    return res;
}

TVector<std::string> TLogGenerator::GetCleanPaths() const {
    return { Params.TableName };
}

TVector<TRow> TLogGenerator::GenerateRandomRows() {
    TVector<TRow> result(Params.RowsCnt);

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        result[row].Ts = TInstant::Now();
        i64 millisecondsDiff = 60 * 1000 * NormalRandom(0., static_cast<double>(Params.TimestampStandardDeviationMinutes));
        if (millisecondsDiff >= 0) { // TDuration::MilliSeconds can't be negative for some reason...
            result[row].Ts = result[row].Ts + TDuration::MilliSeconds(millisecondsDiff);
        } else {
            result[row].Ts = result[row].Ts - TDuration::MilliSeconds(-millisecondsDiff);
        }

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

void TMixedWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
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
        opts.AddLongOption("min-partitions", "Minimum partitions for tables.")
            .DefaultValue((ui64)LogWorkloadConstants::MIN_PARTITIONS).StoreResult(&MinPartitions);
        opts.AddLongOption("max-partitions", "Maximum partitions for tables.")
            .DefaultValue((ui64)LogWorkloadConstants::MAX_PARTITIONS).StoreResult(&MaxPartitions);
        opts.AddLongOption("partition-size", "Maximum partition size in megabytes (AUTO_PARTITIONING_PARTITION_SIZE_MB).")
            .DefaultValue((ui64)LogWorkloadConstants::PARTITION_SIZE_MB).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("auto-partition", "Enable auto partitioning by load.")
            .DefaultValue((ui64)LogWorkloadConstants::PARTITIONS_BY_LOAD).StoreResult(&PartitionsByLoad);
        opts.AddLongOption("len", "String len")
            .DefaultValue((ui64)LogWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue((ui64)LogWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("str-cols", "Number of string columns")
            .DefaultValue((ui64)LogWorkloadConstants::STR_COLUMNS_CNT).StoreResult(&StrColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue((ui64)LogWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
        opts.AddLongOption("ttl", "TTL for timestamp column in minutes")
            .DefaultValue((ui64)LogWorkloadConstants::TIMESTAMP_TTL_MIN).StoreResult(&TimestampTtlMinutes);
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
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue((ui64)LogWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("str-cols", "Number of string columns")
            .DefaultValue((ui64)LogWorkloadConstants::STR_COLUMNS_CNT).StoreResult(&StrColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue((ui64)LogWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
        switch (static_cast<TLogGenerator::EType>(workloadType)) {
        case TLogGenerator::EType::Select:
        case TLogGenerator::EType::Insert:
        case TLogGenerator::EType::Upsert:
        case TLogGenerator::EType::BulkUpsert:
            opts.AddLongOption("len", "String len")
                .DefaultValue((ui64)LogWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
            opts.AddLongOption("rows", "Number of rows to upsert")
                .DefaultValue((ui64)LogWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
            opts.AddLongOption("timestamp_deviation", "Standard deviation. For each timestamp, a random variable with a specified standard deviation in minutes is added.")
                .DefaultValue((ui64)LogWorkloadConstants::TIMESTAMP_STANDARD_DEVIATION_MINUTES).StoreResult(&TimestampStandardDeviationMinutes);
            break;
        }
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TMixedWorkloadParams::CreateGenerator() const {
    return MakeHolder<TLogGenerator>(this);
}

TString TMixedWorkloadParams::GetWorkloadName() const {
    return "Log";
}

} // namespace NMixed

} // namespace NYdbWorkload
