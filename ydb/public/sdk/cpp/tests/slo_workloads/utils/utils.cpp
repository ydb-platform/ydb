#include "utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <library/cpp/threading/future/async.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>
#include <util/random/random.h>

using namespace NLastGetopt;
using namespace NYdb;

const TDuration DefaultReactionTime = TDuration::Minutes(2);
const TDuration ReactionTimeDelay = TDuration::MilliSeconds(5);
const TDuration GlobalTimeout = TDuration::Minutes(2);
const ui64 PartitionsCount = 64;

Y_DECLARE_OUT_SPEC(, NYdb::TStatus, stream, value) {
    stream << "Status: " << value.GetStatus() << Endl;
    value.GetIssues().PrintTo(stream);
}

TDurationMeter::TDurationMeter(TDuration& value)
    : Value(value)
    , StartTime(TInstant::Now())
{
}

TDurationMeter::~TDurationMeter() {
    Value += TInstant::Now() - StartTime;
}

TRpsProvider::TRpsProvider(ui64 rps)
    : Rps(rps)
    , Period(Max(TDuration::MilliSeconds(10), TDuration::MicroSeconds(1000000 / Rps)))
    , ProcessedTime(TInstant::Now())
{
}

void TRpsProvider::Reset() {
    ProcessedTime = TInstant::Now() - Period - Period;
}

void TRpsProvider::Use() {
    if (Allowed) {
        --Allowed;
        return;
    }

    while (!TryUse()) {
        SleepUntil(TInstant::Now() + Period);
    }
}

bool TRpsProvider::TryUse() {
    TInstant now = TInstant::Now();
    // Number of objects to process since ProcessedTime
    Allowed = Rps * TDuration(now - ProcessedTime).MicroSeconds() / 1000000;
    if (Allowed) {
        ProcessedTime += TDuration::MicroSeconds(1000000 * Allowed / Rps);
        --Allowed;
        return true;
    } else {
        return false;
    }
}

ui64 TRpsProvider::GetRps() const {
    return Rps;
}

bool ParseToken(std::string& token, std::string& tokenFile) {
    if (!tokenFile.empty()) {
        if (!token.empty()) {
            Cerr << "Both token and token_file provided. Choose one." << Endl;
        } else {
            TFsPath path(tokenFile);
            if (path.Exists()) {
                token = Strip(TUnbufferedFileInput(path).ReadAll());
                return true;
            }
            Cerr << "Wrong path provided for token_file." << Endl;
        }
    } else if (!token.empty()) {
        return true;
    } else {
        token = GetEnv("YDB_TOKEN");
        return true;
    }
    return false;
}

void StartStatCollecting([[maybe_unused]] TDriver& driver, const std::string& statConfigFile) {
    if (statConfigFile.empty()) {
        return;
    }

    // TODO: Implement
}

std::string GetDatabase(const std::string& connectionString) {
    constexpr std::string_view databaseFlag = "/?database=";
    size_t pathIndex = connectionString.find(databaseFlag);
    if (pathIndex != std::string::npos) {
        return connectionString.substr(pathIndex + databaseFlag.size());
    }
    return {};
}

int DoMain(int argc, char** argv, TCreateCommand create, TRunCommand run, TCleanupCommand cleanup) {
    TOpts opts = TOpts::Default();

    std::string connectionString;
    std::string prefix;
    std::string token;
    std::string tokenFile;
    std::string iamSaKeyFile;
    std::string statConfigFile;

    opts.AddLongOption('c', "connection_string", "YDB connection string").Required().RequiredArgument("SCHEMA://HOST:PORT/?DATABASE=DATABASE")
        .StoreResult(&connectionString);
    opts.AddLongOption('p', "prefix", "Base prefix for tables").RequiredArgument("PATH")
        .StoreResult(&prefix);
    opts.AddLongOption('k', "token", "security token").RequiredArgument("TOKEN")
        .StoreResult(&token);
    opts.AddLongOption('f', "token_file", "security token file").RequiredArgument("PATH")
        .StoreResult(&tokenFile);
    opts.AddLongOption("iam_sa_key_file", "IAM service account key file").RequiredArgument("SECRET")
        .StoreResult(&iamSaKeyFile);
    opts.AddLongOption('s', "stat_config", "statistics config file").Optional().RequiredArgument("PATH")
        .StoreResult(&statConfigFile);
    opts.AddHelpOption('h');
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgTitle(0, "<COMMAND>", GetCmdList());
    opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;

    TOptsParseResult res(&opts, argc, argv);
    size_t freeArgsPos = res.GetFreeArgsPos();
    argc -= freeArgsPos;
    argv += freeArgsPos;
    ECommandType command = ParseCommand(*argv);
    if (command == ECommandType::Unknown) {
        Cerr << "Unknown command '" << *argv << "'" << Endl;
        return EXIT_FAILURE;
    }

    if (prefix.empty()) {
        prefix = GetDatabase(connectionString);
    }

    if (!ParseToken(token, tokenFile)) {
        return EXIT_FAILURE;
    }

    auto config = TDriverConfig(connectionString);

    if (!iamSaKeyFile.empty()) {
        Cout << "Enabling IAM authentication..." << Endl;
        TIamJwtFilename iamJwtFilename{ .JwtFilename = iamSaKeyFile };
        config.SetCredentialsProviderFactory(CreateIamJwtFileCredentialsProviderFactory(iamJwtFilename));
    } else if (!token.empty()) {
        Cout << "Enabling OAuth authentication..." << Endl;
        config.SetCredentialsProviderFactory(CreateOAuthCredentialsProviderFactory(token));
    } else {
        Cerr << "Warning: No authentication methods provided." << Endl;
    }

    TDriver driver(config);

    StartStatCollecting(driver, statConfigFile);

    TDatabaseOptions dbOptions{ driver, prefix };
    int result;
    try {
        switch (command) {
        case ECommandType::Create:
            Cout << "Launching create command..." << Endl;
            result = create(dbOptions, argc, argv);
            break;
        case ECommandType::Run:
            Cout << "Launching run command..." << Endl;
            result = run(dbOptions, argc, argv);
            break;
        case ECommandType::Cleanup:
            Cout << "Launching cleanup command..." << Endl;
            result = cleanup(dbOptions, argc);
            break;
        default:
            Cerr << "Unknown command" << Endl;
            return EXIT_FAILURE;
        }
    }
    catch (const TYdbErrorException& e) {
        Cerr << "Exception caught: " << e << Endl;
        return EXIT_FAILURE;
    }
    driver.Stop(true);
    return result;
}

std::string GetCmdList() {
    return "create, run, cleanup";
}

ECommandType ParseCommand(const char* cmd) {
    if (!strcmp(cmd, "create")) {
        return ECommandType::Create;
    }
    if (!strcmp(cmd, "run")) {
        return ECommandType::Run;
    }
    if (!strcmp(cmd, "cleanup")) {
        return ECommandType::Cleanup;
    }
    return ECommandType::Unknown;
}

std::string JoinPath(const std::string& prefix, const std::string& path) {
    if (prefix.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(prefix);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}

std::string GenerateRandomString(ui32 minLength, ui32 maxLength) {
    ui32 length = minLength + RandomNumber<ui32>() % (maxLength - minLength);
    static const char* symbols = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result.push_back(symbols[RandomNumber<ui8>(61)]);
    }
    return result;
}

using namespace NYdb;
using namespace NYdb::NTable;

TParams PackValuesToParamsAsList(const std::vector<TValue>& items, const std::string name) {
    TValueBuilder itemsAsList;
    itemsAsList.BeginList();
    for (const TValue& item : items) {
        itemsAsList.AddListItem(item);
    }
    itemsAsList.EndList();

    TParamsBuilder paramsBuilder;
    paramsBuilder.AddParam(name, itemsAsList.Build());
    return paramsBuilder.Build();
}

static double shardSize = (static_cast<double>(Max<ui32>()) + 1) / PartitionsCount;

ui32 GetSpecialId(ui32 id) {
    return static_cast<ui32>(id / shardSize) * shardSize + 1;
}

ui32 GetShardSpecialId(ui64 shardNo) {
    return shardNo * shardSize + 1;
}

ui32 GetHash(ui32 value) {
    ui32 result = NumericHash(value);
    if (result == GetSpecialId(result)) {
        ++result;
    }
    return result;
}

TTableStats GetTableStats(TDatabaseOptions& dbOptions, const std::string& tableName) {
    Cout << TInstant::Now().ToRfc822StringLocal()
        << " Getting table stats (maxId and count of rows) with ReadTable... " << Endl;
    TInstant start_time = TInstant::Now();
    NYdb::NTable::TTableClient client(
        dbOptions.Driver,
        NYdb::NTable::TClientSettings()
            .MinSessionCV(8)
            .AllowRequestMigration(true)
    );

    std::optional<TTablePartIterator> tableIterator;
    ThrowOnError(client.RetryOperationSync([&tableIterator, &dbOptions, &tableName](TSession session) {
        auto result = session.ReadTable(
            JoinPath(dbOptions.Prefix, tableName),
            TReadTableSettings().AppendColumns("object_id")
        ).GetValueSync();

        if (result.IsSuccess()) {
            tableIterator = result;
        }

        return result;
    }));
    Y_ENSURE(tableIterator);
    TSimpleThreadPool pool;
    std::vector<NThreading::TFuture<TTableStats>> futures;
    pool.Start(10);
    for (;;) {
        auto tablePart = tableIterator->ReadNext().GetValueSync();
        if (!tablePart.IsSuccess()) {
            if (tablePart.EOS()) {
                break;
            }

            ThrowOnError(tablePart);
        }
        futures.push_back(
            NThreading::Async(
                [extractedPart = tablePart.ExtractPart()]{
                    auto rsParser = TResultSetParser(extractedPart);
                    ui32 partMax = 0;
                    while (rsParser.TryNextRow()) {
                        auto& idParser = rsParser.ColumnParser("object_id");
                        idParser.OpenOptional();
                        ui32 id = idParser.GetUint32();
                        if (id > partMax) {
                            partMax = id;
                        }
                    }
                    return TTableStats{ rsParser.RowsCount(), partMax };
                },
                pool
            )
        );
    }
    TTableStats result;
    for (auto future : futures) {
        TTableStats partStats = future.GetValueSync();
        if (partStats.MaxId > result.MaxId) {
            result.MaxId = partStats.MaxId;
        }
        result.RowCount += partStats.RowCount;
    }
    Cout << TInstant::Now().ToRfc822StringLocal() << " Done. maxId=" << result.MaxId << ", row count=" << result.RowCount
        << ". Calculations took " << TInstant::Now() - start_time << Endl;
    return result;
}

void ParseOptionsCommon(TOpts& opts, TCommonOptions& options, bool followers) {
    opts.AddLongOption("threads", "Number of threads to use").RequiredArgument("NUM")
        .DefaultValue(options.MaxInputThreads).StoreResult(&options.MaxInputThreads);
    opts.AddLongOption("stop_on_error", "Stop thread if an error occured").NoArgument()
        .SetFlag(&options.StopOnError).DefaultValue(options.StopOnError);
    opts.AddLongOption("payload_min", "Minimum length of payload string").RequiredArgument("NUM")
        .DefaultValue(options.MinLength).StoreResult(&options.MinLength);
    opts.AddLongOption("payload_max", "Maximum length of payload string").RequiredArgument("NUM")
        .DefaultValue(options.MaxLength).StoreResult(&options.MaxLength);
    opts.AddLongOption("timeout", "Read requests execution timeout [ms]").RequiredArgument("NUM")
        .DefaultValue(options.A_ReactionTime).StoreResult(&options.A_ReactionTime);
    opts.AddLongOption("dont_push", "Do not push metrics").NoArgument()
        .SetFlag(&options.DontPushMetrics).DefaultValue(options.DontPushMetrics);
    opts.AddLongOption("retry", "Retry each request until Ok reply or global timeout").NoArgument()
        .SetFlag(&options.RetryMode).DefaultValue(options.RetryMode);
    opts.AddLongOption("save_result", "Save result to file").NoArgument()
        .SetFlag(&options.SaveResult).DefaultValue(options.SaveResult);
    opts.AddLongOption("result_file_name", "Set result json file name").RequiredArgument("String")
        .DefaultValue(options.ResultFileName).StoreResult(&options.ResultFileName);
    opts.AddLongOption("app_timeout", "Use application timeout (over SDK)").NoArgument()
        .SetFlag(&options.UseApplicationTimeout).DefaultValue(options.UseApplicationTimeout);
    opts.AddLongOption("prevention_request", "Send prevention request at 1/2 of timeout").NoArgument()
        .SetFlag(&options.SendPreventiveRequest).DefaultValue(options.SendPreventiveRequest);
    opts.AddLongOption("no_prepare", "Do not prepare requests").NoArgument()
        .SetFlag(&options.DoNotPrepare).DefaultValue(options.DoNotPrepare);
    if (followers) {
        opts.AddLongOption("followers", "Use followers").NoArgument()
            .SetFlag(&options.UseFollowers).DefaultValue(options.UseFollowers);
    }
}

bool CheckOptionsCommon(TCommonOptions& options) {
    if (options.MinLength > options.MaxLength) {
        Cerr << "--payload_min should be less than --payload_max" << Endl;
        return false;
    }
    if (!options.MaxInputThreads) {
        Cerr << "--threads should be more than 0" << Endl;
        return false;
    }
    if (!options.DontPushMetrics) {
        Cerr << "Add --solomon_token_file option to push metrics to Solomon. Or use --dont_push instead." << Endl;
        return false;
    }
    return true;
}

bool ParseOptionsCreate(int argc, char** argv, TCreateOptions& createOptions, bool followers) {
    TOpts opts = TOpts::Default();
    ParseOptionsCommon(opts, createOptions.CommonOptions, followers);
    opts.AddLongOption("count", "Total number of records to generate").RequiredArgument("NUM")
        .DefaultValue(createOptions.Count).StoreResult(&createOptions.Count);
    opts.AddLongOption("pack_size", "Number of new records in each create request").RequiredArgument("NUM")
        .DefaultValue(createOptions.PackSize).StoreResult(&createOptions.PackSize);

    TOptsParseResult res(&opts, argc, argv);

    if (!CheckOptionsCommon(createOptions.CommonOptions)) {
        return false;
    }
    if (!createOptions.Count) {
        Cerr << "--count should be more than 0" << Endl;
        return false;
    }
    if (!createOptions.PackSize) {
        Cerr << "--pack_size should be more than 0" << Endl;
        return false;
    }
    return true;
}

bool ParseOptionsRun(int argc, char** argv, TRunOptions& runOptions, bool followers) {
    TOpts opts = TOpts::Default();
    ParseOptionsCommon(opts, runOptions.CommonOptions, followers);
    opts.AddLongOption("time", "Time to run (Seconds)").RequiredArgument("Seconds")
        .DefaultValue(runOptions.CommonOptions.SecondsToRun).StoreResult(&runOptions.CommonOptions.SecondsToRun);
    opts.AddLongOption("read_rps", "Request generation rate for read requests (Thread A)").RequiredArgument("NUM")
        .DefaultValue(runOptions.Read_rps).StoreResult(&runOptions.Read_rps);
    opts.AddLongOption("write_rps", "Request generation rate for write requests (Thread B)").RequiredArgument("NUM")
        .DefaultValue(runOptions.Write_rps).StoreResult(&runOptions.Write_rps);
    opts.AddLongOption("no_read", "Do not run reading requests (thread A)").NoArgument()
        .SetFlag(&runOptions.DontRunA).DefaultValue(runOptions.DontRunA);
    opts.AddLongOption("no_write", "Do not run writing requests (thread B)").NoArgument()
        .SetFlag(&runOptions.DontRunB).DefaultValue(runOptions.DontRunB);
    opts.AddLongOption("no_c", "Do not run thread C").NoArgument()
        .SetFlag(&runOptions.DontRunC).DefaultValue(runOptions.DontRunC);
    opts.AddLongOption("infly", "Maximum number of running jobs").RequiredArgument("NUM")
        .DefaultValue(runOptions.CommonOptions.MaxInfly).StoreResult(&runOptions.CommonOptions.MaxInfly);
    TOptsParseResult res(&opts, argc, argv);

    if (!CheckOptionsCommon(runOptions.CommonOptions)) {
        return false;
    }
    if (!runOptions.CommonOptions.SecondsToRun) {
        Cerr << "Time to run should be more than 0" << Endl;
        return false;
    }
    return true;
}
