#include "tpch_runner.h"
#include "tpch_tables.h"

#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/split.h>


namespace NYdb::NTpch {

using namespace NTable;
using namespace NScheme;

namespace {

void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        ythrow yexception() << "Operation failed with status " << status.GetStatus() << ": "
                            << status.GetIssues().ToString();
    }
}

template <typename T>
void BuildRow(const TString& line, const TVector<TTableColumn>& columns, TValueBuilderBase<T>& row) {
    TVector<TStringBuf> data = StringSplitter(line).Split('|').SkipEmpty();
    Y_ENSURE(data.size() == columns.size());

    row.BeginStruct();

    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& column = columns[i];
        const auto value = data[i];

        auto& cell = row.AddMember(column.Name);

        bool optional = false;
        TTypeParser typeParser{column.Type};
        if (typeParser.GetKind() == TTypeParser::ETypeKind::Optional) {
            optional = true;
            typeParser.OpenOptional();
        }
        if (typeParser.GetKind() == TTypeParser::ETypeKind::Primitive) {
            switch (typeParser.GetPrimitive()) {
                case EPrimitiveType::Uint32: {
                    if (optional) {
                        cell.OptionalUint32(FromString(value));
                    } else {
                        cell.Uint32(FromString(value));
                    }
                    break;
                }
                case EPrimitiveType::Int32: {
                    if (optional) {
                        cell.OptionalInt32(FromString(value));
                    } else {
                        cell.Int32(FromString(value));
                    }
                    break;
                }
                case EPrimitiveType::Double: {
                    if (optional) {
                        cell.OptionalDouble(FromString(value));
                    } else {
                        cell.Double(FromString(value));
                    }
                    break;
                }
                case EPrimitiveType::Utf8: {
                    if (optional) {
                        cell.OptionalUtf8(ToString(value));
                    } else {
                        cell.Utf8(ToString(value));
                    }
                    break;
                }
                case EPrimitiveType::Date: {
                    if (optional) {
                        cell.OptionalDate(TInstant::ParseIso8601(value));
                    } else {
                        cell.Date(TInstant::ParseIso8601(value));
                    }
                    break;
                }
                default: {
                    Y_ABORT("primitive type %s not supported yet", ToString(typeParser.GetPrimitive()).c_str());
                }
            }
        } else if (typeParser.GetKind() == TTypeParser::ETypeKind::Decimal) {
            if (optional) {
                Y_ABORT("Optional<Decimal>");
            } else {
                cell.Decimal(TDecimalValue(ToString(value)));
            }
        } else {
            Y_ABORT("type kind `%s` not supported yet", ToString(typeParser.GetKind()).c_str());
        }
    }

    row.EndStruct();
}

} // namespace

TTpchRunner::TTpchRunner(const TDriver& driver, const TString& tablesPath)
    : Driver(driver)
    , TablesPath(tablesPath)
{
}

void TTpchRunner::CreateTables(const TMap<TString, ui32>& partitions, bool force) {
    TTableClient client{Driver};

    if (force) {
        DropTables(client, false);
    }

    Cout << "Create tables..." << Endl;
    for (const auto& table : TABLES) {
        const TString tbl = JoinFsPaths(TablesPath, table.first);
        TTableDescription description = table.second;
        ui32 partitionsCount = partitions.contains(ToString(table.first)) ? partitions.at(ToString(table.first)) : 0;

        ThrowOnError(client.RetryOperationSync([&tbl, &description, partitionsCount](TSession session) {
            auto partitionPolicy = TPartitioningPolicy();
            if (partitionsCount) {
                Cout << "  create table `" << tbl << "` with " << partitionsCount << " uniform partitions" << Endl;
                partitionPolicy.UniformPartitions(partitionsCount);
            } else {
                Cout << "  create table `" << tbl << "` with autosplit" << Endl;
                partitionPolicy.AutoPartitioning(EAutoPartitioningPolicy::AutoSplitMerge);
            }
            auto createTableSettings = TCreateTableSettings()
                .PartitioningPolicy(partitionPolicy);
            return session.CreateTable(tbl, std::move(description), createTableSettings).GetValueSync();
        }));
    }

    client.Stop().GetValueSync();
}

void TTpchRunner::DropTables(TTableClient& client, bool removeDir) {
    Cout << "Drop tables..." << Endl;
    for (const auto& table : TABLES) {
        const TString tbl = JoinFsPaths(TablesPath, table.first);
        auto status = client.RetryOperationSync([tbl](TSession session) {
            return session.DropTable(tbl).GetValueSync();
        });
        if (status.IsSuccess()) {
            Cout << "  drop table `" << table.first << "`" << Endl;
        } else if (status.GetStatus() != EStatus::SCHEME_ERROR) {
            Cerr << "  drop table `" << table.first << "` failure: [" << status.GetStatus() << "] "
                 << status.GetIssues().ToString() << Endl;
        }
    }
    if (removeDir) {
        TSchemeClient sc{Driver};
        auto status = sc.RemoveDirectory(TablesPath).GetValueSync();
        if (status.IsSuccess()) {
            Cout << "  and remove directory" << Endl;
        } else {
            Cerr << "  error while removing directory: " << status.GetIssues().ToString() << Endl;
        }
    }
}

void TTpchRunner::UploadBundledData(ui32 partitionsCount, bool dropTables) {
    Cout << "Uploading bundled data..." << Endl;

    TTableClient client{Driver};

    if (dropTables) {
        DropTables(client);
    }

    Cout << "Create tables..." << Endl;
    for (const auto& table : TABLES) {
        const TString tbl = JoinFsPaths(TablesPath, table.first);
        TTableDescription description = table.second;
        ThrowOnError(client.RetryOperationSync([&tbl, &description, &partitionsCount](TSession session) {
            Cout << "  create table `" << tbl << "`" << Endl;
            auto createTableSettings = TCreateTableSettings()
                .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(partitionsCount));
            return session.CreateTable(tbl, std::move(description), createTableSettings).GetValueSync();
        }));

        TVector<TString> content = StringSplitter(NResource::Find(ToString(table.first) + ".tbl")).Split('\n').SkipEmpty();
        size_t nextRow = 0;
        UploadTable(tbl, 1000, [&content, &nextRow](TString& row) {
            if (nextRow >= content.size()) {
                return false;
            }
            row = std::move(content[nextRow]);
            ++nextRow;
            return true;
        }, table.second, client);
    }

    Cout << "Bundled data has been uploaded" << Endl;
    client.Stop().GetValueSync();
}

void TTpchRunner::UploadData(const TString& dir, ui32 partitionSizeMb, size_t batchSize) {
    Cout << "Uploading data from " << dir << " ..." << Endl;

    TTableClient client{Driver};

    DropTables(client);

    {
        Cout << "Create YDB database `" << TablesPath << "`..." << Endl;
        TSchemeClient sc{Driver};
        auto result = sc.MakeDirectory(TablesPath).GetValueSync();
        if (!result.IsSuccess()) {
            Cerr << "Failed to create directory `" << TablesPath << "`: " << result.GetIssues().ToString() << Endl;
        }
    }

    for (const auto& table : TABLES) {
        const TString file = JoinFsPaths(dir, table.first) + ".tbl";
        const TString tbl = JoinFsPaths(TablesPath, table.first);
        TTableDescription description = table.second;

        ThrowOnError(client.RetryOperationSync([&](TSession session) {
            Cout << "Create table `" << tbl << "`" << Endl;

            TCreateTableSettings settings;

            auto boundaries = CalcTableSplitBoundaries(tbl, partitionSizeMb, file);
            if (!boundaries.empty()) {
                TExplicitPartitions partitions;
                for (ui32 boundary : boundaries) {
                    partitions.AppendSplitPoints(
                        TValueBuilder()
                            .BeginTuple()
                                .AddElement().OptionalUint32(boundary)
                            .EndTuple()
                            .Build());
                }
                settings.PartitioningPolicy(TPartitioningPolicy().ExplicitPartitions(partitions));
                Cout << "  with " << (boundaries.size() + 1) << " explicit partitions" << Endl;
            }

            return session.CreateTable(tbl, std::move(description), settings).GetValueSync();
        }));

        Cout << "Uploading data into the table `" << tbl << "`, batchSize: " << batchSize << Endl;
        TFileInput in{file};
        bool hasMore = true;
        ui32 totalRows = 0;
        while (hasMore) {
            size_t loadedRows = 0;
            UploadTable(tbl, batchSize, [&in, &loadedRows, &totalRows, &hasMore, batchSize](TString& row) {
                if (loadedRows >= batchSize) {
                    return false;
                }
                size_t read = in.ReadLine(row);
                if (read) {
                    ++loadedRows;
                    ++totalRows;
                } else {
                    hasMore = false;
                }
                return read > 0;
            }, table.second, client);
        }
        Cout << Endl << "  uploaded " << totalRows << " rows" << Endl;
    }

    client.Stop().GetValueSync();
}

TVector<ui32> TTpchRunner::CalcTableSplitBoundaries(const TString& table, ui32 partitionSizeMb, const TString& file) {
    Cout << "Calc table `" << table << "` split boundaries for " << partitionSizeMb << "MB partitions" << Endl;

    TFileInput in{file};
    TVector<ui32> ret;

    TString line;
    ui64 size = 0;
    while (in.ReadLine(line)) {
        size += line.size();
        auto sizeMb = 1.25 * (size >> 20ul);
        if (sizeMb >= partitionSizeMb) {
            auto key = FromString<ui32>(TStringBuf(line).NextTok('|'));
            ret.push_back(key);
            Cout << "  - split by key: " << key << ", size: " << sizeMb << " MB" << Endl;
            size = 0;
        }
    }

    return ret;
}

void TTpchRunner::UploadTable(const TString& table, ui32 expectedRows, TRowProvider&& rowProvider,
    const TTableDescription& descr, TTableClient& client)
{
    const auto& columns = descr.GetTableColumns();

    TVector<TString> bulk;
    bulk.reserve(expectedRows);

    TString line;
    ui32 rowsCount = 0;
    while (rowProvider(line)) {
        bulk.emplace_back(line);
        ++rowsCount;
    }

    if (rowsCount == 0) {
        return;
    }

    ThrowOnError(client.RetryOperationSync([&](TSession) { // NOLINT(performance-unnecessary-value-param)
        TValueBuilder valuesBuilder;
        valuesBuilder.BeginList();

        for (auto& line : bulk) {
            BuildRow(line, columns, valuesBuilder.AddListItem());
        }

        valuesBuilder.EndList();
        auto values = valuesBuilder.Build();

        Cout << ".";
        Flush(Cout);

        auto settings = TBulkUpsertSettings()
            .ClientTimeout(TDuration::Hours(5))
            .OperationTimeout(TDuration::Hours(5))
            .CancelAfter(TDuration::Hours(5));
        return client.BulkUpsert(table, std::move(values), settings).GetValueSync();
    }));
}

void TTpchRunner::PrepareDataForPostgresql(const TString& dir, const TString& outFileName) {
    static const TVector<TStringBuf> TABLES_ORDER = {
        "region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"
    };

    TFileOutput out{outFileName};
    out << "begin;" << Endl << Endl;

    for (const auto& table : TABLES_ORDER) {
        out << "-- table " << table << Endl;

        const auto& descr = TABLES.at(table);
        const auto& columns = descr.GetTableColumns();

        TStringBuilder queryHeader;
        queryHeader << "insert into " << table << " (";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i != 0) {
                queryHeader << ", ";
            }
            queryHeader << columns[i].Name;
        }
        queryHeader << ") values (";

        TStringBuilder queryTail;
        queryTail << ") on conflict (";
        for (size_t i = 0; i < descr.GetPrimaryKeyColumns().size(); ++i) {
            if (i != 0) {
                queryTail << ", ";
            }
            queryTail << descr.GetPrimaryKeyColumns()[i];
        }
        queryTail << ") do update set ";

        const TString file = JoinFsPaths(dir, table) + ".tbl";
        TFileInput in{file};
        TString line;
        while (in.ReadLine(line)) {
            TVector<TStringBuf> data = StringSplitter(line).Split('|').SkipEmpty();
            Y_ABORT_UNLESS(data.size() == columns.size());

            TStringBuilder header, tail;
            header << queryHeader;
            tail << queryTail;

            for (size_t i = 0; i < columns.size(); ++i) {
                const auto& column = columns[i];
                const auto value = data[i];

                if (i != 0) {
                    header << ", ";
                    tail << ", ";
                }

                TTypeParser typeParser{column.Type};
                if (typeParser.GetKind() == TTypeParser::ETypeKind::Optional) {
                    typeParser.OpenOptional();
                }
                if (typeParser.GetKind() == TTypeParser::ETypeKind::Primitive) {
                    switch (typeParser.GetPrimitive()) {
                        case EPrimitiveType::Uint32:
                        case EPrimitiveType::Int32:
                        case EPrimitiveType::Double: {
                            header << value;
                            tail << column.Name << "=" << value;
                            break;
                        }
                        case EPrimitiveType::Utf8:
                        case EPrimitiveType::Date: {
                            header << '\'' << value << '\'';
                            tail << column.Name << "='" << value << '\'';
                            break;
                        }
                        default: {
                            Y_ABORT("primitive type %s not suported yet", ToString(typeParser.GetPrimitive()).c_str());
                        }
                    }
                } else if (typeParser.GetKind() == TTypeParser::ETypeKind::Decimal) {
                    header << value;
                    tail << column.Name << "=" << value;
                } else {
                    Y_ABORT("type kind `%s` not supported yet", ToString(typeParser.GetKind()).c_str());
                }
            }

            out << header << tail << ';' << Endl;
        }
        out << Endl;
    }

    out << Endl << "commit;" << Endl;
}

NTable::TScanQueryPartIterator TTpchRunner::RunQuery(ui32 number, bool profile, bool usePersistentSnapshot) {
    if (number == 1)  { return RunQuery01(TInstant::ParseIso8601("1998-12-01"), TDuration::Days(3), profile, usePersistentSnapshot); }
    if (number == 2)  { return RunQuery02(profile, usePersistentSnapshot); }
    if (number == 3)  { return RunQuery03(profile, usePersistentSnapshot); }
    if (number == 4)  { return RunQuery04(profile, usePersistentSnapshot); }
    if (number == 5)  { return RunQuery05(profile, usePersistentSnapshot); }
    if (number == 6)  { return RunQuery06(profile, usePersistentSnapshot); }
    if (number == 7)  { return RunQuery07(profile, usePersistentSnapshot); }
    if (number == 8)  { return RunQuery08(profile, usePersistentSnapshot); }
    if (number == 9)  { return RunQuery09(profile, usePersistentSnapshot); }
    if (number == 10) { return RunQuery10(profile, usePersistentSnapshot); }
    if (number == 11) { return RunQuery11(profile, usePersistentSnapshot); }
    if (number == 12) { return RunQuery12(profile, usePersistentSnapshot); }
    if (number == 13) { return RunQuery13(profile, usePersistentSnapshot); }
    if (number == 14) { return RunQuery14(profile, usePersistentSnapshot); }
    if (number == 15) { return RunQuery15(profile, usePersistentSnapshot); }
    if (number == 16) { return RunQuery16(profile, usePersistentSnapshot); }
    if (number == 17) { return RunQuery17(profile, usePersistentSnapshot); }
    if (number == 18) { return RunQuery18(profile, usePersistentSnapshot); }
    if (number == 19) { return RunQuery19(profile, usePersistentSnapshot); }
    if (number == 20) { return RunQuery20(profile, usePersistentSnapshot); }
    if (number == 21) { return RunQuery21(profile, usePersistentSnapshot); }
    if (number == 22) { return RunQuery22(profile, usePersistentSnapshot); }

    Y_ABORT("Unknown query %02d", number);
}

NTable::TScanQueryPartIterator TTpchRunner::RunQuery01(TInstant startDate, TDuration startDateShift, bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #01: Pricing Summary Report Query" << Endl;

    TString query = NResource::Find("01.sql");
    SubstGlobal(query, "$DBROOT$", TablesPath);

    if (usePersistentSnapshot) {
        SubstGlobal(query, "$PRAGMAS$", "");
    } else {
        SubstGlobal(query, "$PRAGMAS$", "PRAGMA kikimr.DisablePersistentSnapshot = 'true';");
    }

//    Cout << query << Endl;
//    Cout << "$start_date: " << startDate << Endl;
//    Cout << "$start_date_shift: " << startDateShift << Endl;

    TTableClient client{Driver};

    auto params = client.GetParamsBuilder()
            .AddParam("$start_date").Date(startDate).Build()
            .AddParam("$start_date_shift").Interval(startDateShift.MicroSeconds()).Build()
            .Build();

    auto settings = TStreamExecScanQuerySettings();
    if (profile) {
        settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    }

    auto it = client.StreamExecuteScanQuery(query, params, settings).GetValueSync();

    if (it.IsSuccess()) {
        return it;
    }

    ythrow yexception() << "Query execution failed: " << it.GetStatus() << ", " << it.GetIssues().ToString();
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery02(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #02: Minimum Cost Supplier Query" << Endl;
    return RunQueryGeneric(2, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery03(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #03: Shipping Priority Query" << Endl;
    return RunQueryGeneric(3, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery04(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #04: Order Priority Checking Query" << Endl;
    return RunQueryGeneric(4, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery05(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #05: Local Supplier Volume Query" << Endl;
    return RunQueryGeneric(5, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery06(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #06: Forecasting Revenue Change Query" << Endl;
    return RunQueryGeneric(6, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery07(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #07: Volume Shipping Query" << Endl;
    return RunQueryGeneric(7, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery08(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #08: National Market Share Query" << Endl;
    return RunQueryGeneric(8, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery09(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #09: Product Type Profit Measure Query" << Endl;
    return RunQueryGeneric(9, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery10(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #10: Returned Item Reporting Query" << Endl;
    return RunQueryGeneric(10, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery11(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #11: Important Stock Identification Query" << Endl;
    return RunQueryGeneric(11, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery12(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #12: Shipping Modes and Order Priority Query" << Endl;
    return RunQueryGeneric(12, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery13(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #13: Customer Distribution Query" << Endl;
    return RunQueryGeneric(13, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery14(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #14: Promotion Effect Query" << Endl;
    return RunQueryGeneric(14, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery15(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #15: Top Supplier Query" << Endl;
    return RunQueryGeneric(15, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery16(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #16: Parts Supplier Relationship Query" << Endl;
    return RunQueryGeneric(16, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery17(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #17: Small Quantity Order Revenue Query" << Endl;
    return RunQueryGeneric(17, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery18(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #18: Large Volume Customer Query" << Endl;
    return RunQueryGeneric(18, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery19(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #19: Discounted Revenue Query" << Endl;
    return RunQueryGeneric(19, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery20(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #20: Potential Part Promotion Query" << Endl;
    return RunQueryGeneric(20, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery21(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #21: Suppliers Who Kept Orders Waiting Query" << Endl;
    return RunQueryGeneric(21, profile, usePersistentSnapshot);
}

NTable::TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQuery22(bool profile, bool usePersistentSnapshot) {
    Cout << "Run query #22: Global Sales Opportunity Query" << Endl;
    return RunQueryGeneric(22, profile, usePersistentSnapshot);
}

TScanQueryPartIterator NYdb::NTpch::TTpchRunner::RunQueryGeneric(ui32 number, bool profile, bool usePersistentSnapshot) {
    TString query = NResource::Find(Sprintf("%02d.sql", number));
    SubstGlobal(query, "$DBROOT$", TablesPath);

    if (usePersistentSnapshot) {
        SubstGlobal(query, "$PRAGMAS$", "");
    } else {
        SubstGlobal(query, "$PRAGMAS$", "PRAGMA kikimr.DisablePersistentSnapshot = 'true';");
    }

    // Cout << query << Endl;

    TTableClient client{Driver};

    TStreamExecScanQuerySettings settings;
    if (profile) {
        settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    }

    auto it = client.StreamExecuteScanQuery(query, settings).GetValueSync();

    if (it.IsSuccess()) {
        return it;
    }

    ythrow yexception() << "Query execution failed: " << it.GetStatus() << ", " << it.GetIssues().ToString();
}

} // namespace NYdb::NTpch
