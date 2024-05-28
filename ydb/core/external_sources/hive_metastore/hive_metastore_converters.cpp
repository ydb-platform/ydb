#include "hive_metastore_converters.h"
#include <util/generic/set.h>

namespace NKikimr::NExternalSource {

namespace {

Ydb::Type CreatePrimitiveType(::Ydb::Type_PrimitiveTypeId value) {
    Ydb::Type type;
    type.set_type_id(value);
    return type;
}

Ydb::Type GetType(const std::string& typeName) {
    if (typeName == "boolean") {
        return CreatePrimitiveType(Ydb::Type::BOOL);
    }

    if (typeName == "int") {
        return CreatePrimitiveType(Ydb::Type::INT32);
    }

    if (typeName == "long") {
        return CreatePrimitiveType(Ydb::Type::INT64);
    }

    if (typeName == "float") {
        return CreatePrimitiveType(Ydb::Type::FLOAT);
    }

    if (typeName == "double") {
        return CreatePrimitiveType(Ydb::Type::DOUBLE);
    }

    if (typeName == "date") {
        return CreatePrimitiveType(Ydb::Type::DATE);
    }

    if (typeName == "string") {
        return CreatePrimitiveType(Ydb::Type::UTF8);
    }

    if (typeName == "timestamp") {
        return CreatePrimitiveType(Ydb::Type::TIMESTAMP);
    }

    if (typeName == "binary") {
        return CreatePrimitiveType(Ydb::Type::STRING);
    }

    throw yexception() << "Unsupported type: " << typeName;
}

}

TString THiveMetastoreConverters::GetFormat(const Apache::Hadoop::Hive::Table& table) {
    if (!table.sd.__isset.inputFormat) {
        throw yexception() << "Input format wasn't specified for table " << table.tableName << " in database " << table.dbName;
    }

    if (!table.sd.__isset.outputFormat) {
        throw yexception() << "Output format wasn't specified for table " << table.tableName << " in database " << table.dbName;
    }

    if (table.sd.inputFormat == "org.apache.hadoop.hive.serde2.OpenCSVSerde") {
        if (table.sd.outputFormat != "org.apache.hadoop.hive.serde2.OpenCSVSerde") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "csv_with_names";
    }

    if (table.sd.inputFormat == "org.apache.hadoop.mapred.FileInputFormat") {
        if (table.sd.outputFormat != "org.apache.hadoop.mapred.FileOutputFormat") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "raw";
    }

    if (table.sd.inputFormat == "org.apache.hudi.hadoop.HoodieParquetInputFormat") {
        if (table.sd.outputFormat != "org.apache.hudi.hadoop.HoodieParquetOutputFormat") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "parquet";
    }

    if (table.sd.inputFormat == "org.apache.hive.hcatalog.data.JsonSerDe") {
        if (table.sd.outputFormat != "org.apache.hive.hcatalog.data.JsonSerDe") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "json_each_row";
    }

    if (table.sd.inputFormat == "org.apache.hadoop.hive.serde2.JsonSerDe") {
        if (table.sd.outputFormat != "org.apache.hadoop.hive.serde2.JsonSerDe") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "json_each_row";
    }

    if (table.sd.inputFormat == "org.openx.data.jsonserde.JsonSerDe") {
        if (table.sd.outputFormat != "org.openx.data.jsonserde.JsonSerDe") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "json_each_row";
    }

    if (table.sd.inputFormat == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") {
        if (table.sd.outputFormat != "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "parquet";
    }

    if (table.sd.inputFormat == "org.apache.hadoop.mapred.TextInputFormat") {
        if (table.sd.outputFormat != "org.apache.hadoop.mapred.TextOutputFormat") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "raw";
    }

    if (table.sd.inputFormat == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") {
        if (table.sd.outputFormat != "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") {
            throw yexception() << "Invalid combination of input and output formats: " << table.sd.inputFormat << " " << table.sd.outputFormat;
        }
        return "parquet";
    }

    throw yexception() << "Input format with name " << table.sd.inputFormat << " isn't supported";
}

TString THiveMetastoreConverters::GetCompression(const Apache::Hadoop::Hive::Table&) {
    return TString{};
}

TString THiveMetastoreConverters::GetLocation(const Apache::Hadoop::Hive::Table& table) {
    return TString{table.sd.location};
}

std::vector<Ydb::Column> THiveMetastoreConverters::GetColumns(const Apache::Hadoop::Hive::Table& table) {
    std::vector<Ydb::Column> columns;
    TSet<TString> columnNames;
    for (const auto& hiveColumn: table.sd.cols) {
        if (columnNames.contains(hiveColumn.name)) {
            continue;
        }
        columnNames.insert(TString{hiveColumn.name});
        Ydb::Column ydbColumn;
        ydbColumn.set_name(TString{hiveColumn.name});
        *ydbColumn.mutable_type() = GetType(hiveColumn.type);
        columns.push_back(ydbColumn);
    }
    for (const auto& hiveColumn: table.partitionKeys) {
        if (columnNames.contains(hiveColumn.name)) {
            continue;
        }
        columnNames.insert(TString{hiveColumn.name});
        Ydb::Column ydbColumn;
        ydbColumn.set_name(TString{hiveColumn.name});
        *ydbColumn.mutable_type() = GetType(hiveColumn.type);
        columns.push_back(ydbColumn);
    }
    return columns;
}

std::vector<TString> THiveMetastoreConverters::GetPartitionedColumns(const Apache::Hadoop::Hive::Table& table) {
    std::vector<TString> columns;
    TSet<TString> columnNames;
    for (const auto& hiveColumn: table.partitionKeys) {
        if (columnNames.contains(hiveColumn.name)) {
            continue;
        }
        columnNames.insert(TString{hiveColumn.name});
        columns.push_back(TString{hiveColumn.name});
    }
    return columns;
}

TString THiveMetastoreConverters::GetPartitionsFilter(const NYql::NConnector::NApi::TPredicate&) {
    return TString{}; // TODO: push down filter
}

THiveMetastoreConverters::TStatistics THiveMetastoreConverters::GetStatistics(const Apache::Hadoop::Hive::TableStatsResult& statistics) {
    if (statistics.tableStats.empty()) {
        return TStatistics{};
    }

    int64_t rowsCount = 0;
    for (const auto& columnStatistics : statistics.tableStats) {
        if (columnStatistics.statsData.__isset.binaryStats) {
            auto stats = columnStatistics.statsData.binaryStats;
            rowsCount = std::max(rowsCount, stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.booleanStats) {
            auto stats = columnStatistics.statsData.booleanStats;
            rowsCount = std::max(rowsCount, stats.numTrues + stats.numFalses + stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.dateStats) {
            auto stats = columnStatistics.statsData.dateStats;
            rowsCount = std::max(rowsCount, stats.numDVs + stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.decimalStats) {
            auto stats = columnStatistics.statsData.decimalStats;
            rowsCount = std::max(rowsCount, stats.numDVs + stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.doubleStats) {
            auto stats = columnStatistics.statsData.doubleStats;
            rowsCount = std::max(rowsCount, stats.numDVs + stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.longStats) {
            auto stats = columnStatistics.statsData.longStats;
            rowsCount = std::max(rowsCount, stats.numDVs + stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.stringStats) {
            auto stats = columnStatistics.statsData.stringStats;
            rowsCount = std::max(rowsCount, stats.numDVs + stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.timestampStats) {
            auto stats = columnStatistics.statsData.timestampStats;
            rowsCount = std::max(rowsCount, stats.numDVs + stats.numNulls);
            continue;
        }
    }

    int64_t sizeCount = 0;
    for (const auto& columnStatistics : statistics.tableStats) {
        if (columnStatistics.statsData.__isset.binaryStats) {
            auto stats = columnStatistics.statsData.binaryStats;
            sizeCount += (rowsCount - stats.numNulls) * stats.avgColLen;
            continue;
        }
        if (columnStatistics.statsData.__isset.booleanStats) {
            auto stats = columnStatistics.statsData.booleanStats;
            sizeCount += (rowsCount - stats.numNulls);
            continue;
        }
        if (columnStatistics.statsData.__isset.dateStats) {
            auto stats = columnStatistics.statsData.dateStats;
            sizeCount += (rowsCount - stats.numNulls) * 4;
            continue;
        }
        if (columnStatistics.statsData.__isset.decimalStats) {
            auto stats = columnStatistics.statsData.decimalStats;
            sizeCount += (rowsCount - stats.numNulls) * 8;
            continue;
        }
        if (columnStatistics.statsData.__isset.doubleStats) {
            auto stats = columnStatistics.statsData.doubleStats;
            sizeCount += (rowsCount - stats.numNulls) * 8;
            continue;
        }
        if (columnStatistics.statsData.__isset.longStats) {
            auto stats = columnStatistics.statsData.longStats;
            sizeCount += (rowsCount - stats.numNulls) * 8;
            continue;
        }
        if (columnStatistics.statsData.__isset.stringStats) {
            auto stats = columnStatistics.statsData.stringStats;
            sizeCount += (rowsCount - stats.numNulls) * stats.avgColLen;
            continue;
        }
        if (columnStatistics.statsData.__isset.timestampStats) {
            auto stats = columnStatistics.statsData.timestampStats;
            sizeCount += (rowsCount - stats.numNulls) * 8;
            continue;
        }
    }

    return TStatistics{rowsCount, sizeCount};
}

THiveMetastoreConverters::TStatistics THiveMetastoreConverters::GetStatistics(const std::vector<Apache::Hadoop::Hive::Partition>& partitions) {
    THiveMetastoreConverters::TStatistics statistics;
    for (const auto& partition: partitions) {
        auto it = partition.parameters.find("numRows");
        if (it != partition.parameters.end()) {
            statistics.Rows = statistics.Rows.GetOrElse(0) + std::stoi(it->second);
        }

        it = partition.parameters.find("totalSize");
        if (it != partition.parameters.end()) {
            statistics.Size = statistics.Size.GetOrElse(0) + std::stoi(it->second);
        }
    }
    return statistics;
}

}
