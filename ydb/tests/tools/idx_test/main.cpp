#include <util/system/env.h>
#include <util/string/printf.h>
#include <util/stream/file.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/public/lib/idx_test/idx_test.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;
using namespace NIdxTest;
using namespace NLastGetopt;

class TYdbErrorException : public yexception {
    mutable TString StrBuf_;
public:
    TYdbErrorException(const NYdb::TStatus& status)
        : Status(status) {}

    NYdb::TStatus Status;
    const char* what() const noexcept override {
        TStringStream ss;
        ss << "Status: " << Status.GetStatus() << "\n";
        ss << "Issues: " << Status.GetIssues().ToString() << "\n";
        StrBuf_ = ss.Str();
        return StrBuf_.c_str();
    }
};

static void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    }
}

static TMap<TString, IWorkLoader::ELoadCommand> Cmds {
    {"upsert", IWorkLoader::LC_UPSERT},
    {"insert", IWorkLoader::LC_INSERT},
    {"update", IWorkLoader::LC_UPDATE},
    {"update_on", IWorkLoader::LC_UPDATE_ON},
    {"replace", IWorkLoader::LC_REPLACE},
    {"delete", IWorkLoader::LC_DELETE},
    {"delete_on", IWorkLoader::LC_DELETE_ON},
    {"select", IWorkLoader::LC_SELECT},
    {"upsert_if_uniq", IWorkLoader::LC_UPSERT_IF_UNIQ},
    {"add_index", IWorkLoader::LC_ALTER_ADD_INDEX},
    {"add_covering_index", IWorkLoader::LC_ALTER_ADD_INDEX_WITH_DATA_COLUMN},
};

static TString GetWorkLoadList() {
    TString res;
    auto i = Cmds.size();
    for (const auto& c : Cmds) {
        res += c.first;
        if (--i)
            res += " ";
    }
    return res;
}

static ui32 ParseCmd(int argc, char** argv) {
    ui32 res = 0;
    for (int i = 0; i < argc; i++) {
        auto it = Cmds.find(TString(argv[i]));
        if (it != Cmds.end()) {
            res |= it->second;
        }
    }
    return res;
}

static void ExecuteDDL(TTableClient client, const TString& sql) {
    Cerr << "Try to execute DDL query: " << sql << Endl;
    ThrowOnError(client.RetryOperationSync([sql](TSession session) {
        return session.ExecuteSchemeQuery(sql).GetValueSync();
    }));
}

static void CreatePath(TSchemeClient scheme, const TString& database, const TString prefix) {
    size_t prevPos = 0;
    size_t pos = 0;
    TString curPath = database + "/";
    curPath.reserve(database.size() + prefix.size() + 1);
    for (;;) {
        prevPos = pos;
        pos = prefix.find('/', pos+1);
        if (pos == prefix.npos)
            break;
        curPath += prefix.substr(prevPos, pos - prevPos);
        ThrowOnError(scheme.MakeDirectory(curPath, TMakeDirectorySettings()
            .ClientTimeout(TDuration::Seconds(10))).GetValueSync());
    }
}

static void ExecuteCreateUniformTable(TTableClient client, const TString& tablePath, ui32 shardsCount) {
    Cerr << "Try to create table " << tablePath << " shards count: " << shardsCount << Endl;

    auto builder = TTableBuilder()
        .AddNullableColumn("key", EPrimitiveType::Uint32)
        .AddNullableColumn("index1", EPrimitiveType::Uint32)
        .AddNullableColumn("value", EPrimitiveType::String)
        .SetPrimaryKeyColumn("key")
        .AddSecondaryIndex("index_name", "index1");

    auto desc = builder.Build();

    ThrowOnError(client.RetryOperationSync([tablePath, desc, shardsCount](TSession session) {
        auto d = desc;
        return session.CreateTable(tablePath,
            std::move(d),
            TCreateTableSettings()
                .PartitioningPolicy(
                    TPartitioningPolicy()
                        .UniformPartitions(shardsCount)
                )
            ).GetValueSync();
    }));
}
/*
static void UploadData(TDriver driver, const TString& tablePath, ui32 rows) {
    auto param = TUploaderParams{1};
    // Uploader will try to create table again, but table already exixts. It is not a error
    auto uploader = NIdxTest::CreateUploader(driver, tablePath, param);
    TTableClient client(driver);
    TMaybe<TTableDescription> desc;
    ThrowOnError(client.RetryOperationSync([&desc, tablePath](TSession session) {
        auto result = session.DescribeTable(tablePath).GetValueSync();
        if (result.IsSuccess()) {
            desc = result.GetTableDescription();
        }
        return result;
    }));
    uploader->Run(CreateDataProvider(rows, 1, desc.GetRef()));
}
*/
static void RunOperations(TDriver driver, const TString& tablePath, ui32 op, const IWorkLoader::TRunSettings& settings, const TString& json) {
    auto tracker = CreateStderrProgressTracker(500, "OPERATIONS LOADER");
    auto workLoader = CreateWorkLoader(driver, std::move(tracker));
    auto report = workLoader->Run(tablePath, op, settings);
    if (json) {
        TOFStream jStream{json};
        NJson::WriteJson(&jStream, &report, /*formatOutput*/ true);
        jStream.Finish();
    }
}

int main(int argc, char** argv) {

    TString endpoint;
    TString database;
    TString prefix;
    TString table = "TestTable1";
    TString json;
    bool createTable = false;
    ui32 rows = 1000;
    ui32 opsPerTx = 1;
    ui32 createUniformTable = 0;
    ui32 infly = 100;
    bool skipCheck = false;

    TOpts opts = TOpts::Default();

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('p', "prefix", "Base prefix for tables").Optional().RequiredArgument("PATH")
        .StoreResult(&prefix);
    opts.AddLongOption('t', "table", "Table name to use").Optional().RequiredArgument("NAME")
        .StoreResult(&table);
    opts.AddLongOption('i', "infly", "Number of requests infly").Optional().RequiredArgument("NUM")
        .StoreResult(&infly);
    opts.AddLongOption('j', "json", "Path to json stat result").Optional().RequiredArgument("PATH")
        .StoreResult(&json);

    opts.AddLongOption("create", "Create table before run").Optional()
        .StoreTrue(&createTable);
    opts.AddLongOption("skip-check", "Skip data and index table comparation").Optional()
        .StoreTrue(&skipCheck);
    opts.AddLongOption("create_uniform", "Create table with uniform partitioning using given number of shards")
        .Optional().RequiredArgument("SHARD_COUNT").StoreResult(&createUniformTable);
    opts.AddLongOption("rows", "Number of string to touch").Optional()
        .StoreResult(&rows);
    opts.AddLongOption("ops", "Number of operations per transaction").Optional()
        .StoreResult(&opsPerTx);

    opts.SetFreeArgsMin(0);
    opts.SetFreeArgTitle(0, "<WORKLOAD>", GetWorkLoadList());

    TOptsParseResult res(&opts, argc, argv);
    size_t freeArgsPos = res.GetFreeArgsPos();

    argv += freeArgsPos;

    ui32 op = ParseCmd(argc - freeArgsPos, argv);

    auto driver = TDriver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
            .SetAuthToken(GetEnv("YDB_TOKEN"))
            .SetGRpcKeepAliveTimeout(TDuration::Seconds(20))
            .SetGRpcKeepAlivePermitWithoutCalls(true));

    auto client = TTableClient(driver);
    if (prefix) {
        if (prefix[prefix.size() - 1] != '/') {
            prefix += "/";
        }
    }
    const auto tablePath = database + "/" + prefix + table;

    try {
        if (createTable && createUniformTable) {
            Cerr << "Only one option create or create_uniform allowed" << Endl;
            return 1;
        }
        if (createTable) {
            auto sql = Sprintf(NResource::Find("create_table1").c_str(), tablePath.c_str());
            ExecuteDDL(client, sql);
            Cerr << "Table created" << Endl;
        }
        if (createUniformTable) {
            CreatePath(TSchemeClient(driver), database, prefix);
            ExecuteCreateUniformTable(client, tablePath, createUniformTable);
            Cerr << "Table created" << Endl;
        }

        if (op) {
            RunOperations(driver, tablePath, op, IWorkLoader::TRunSettings{rows, infly, opsPerTx}, json);
        }

        if (!skipCheck) {
            auto tracker = CreateStderrProgressTracker(500, "CHECKER");
            auto checker = CreateChecker(driver, std::move(tracker));
            checker->Run(tablePath);
        }
    } catch (const std::exception& ex) {
        driver.Stop(true);
        Cerr << "Test failed: " << ex.what() << Endl;
        Cerr << "..." << Endl;
        auto ydbEx = dynamic_cast<const TYdbErrorException*>(&ex);
        if (ydbEx) {
            Cerr << "Ydb error: " << ydbEx->Status.GetStatus() << " " << ydbEx->Status.GetIssues().ToString() << Endl;
        }
        return 1;
    }
    driver.Stop(true);
    return 0;
}
