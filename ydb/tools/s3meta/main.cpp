#include <util/system/thread.h>
#include <util/stream/output.h>
#include <util/folder/pathsplit.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/random/entropy.h>
#include <util/generic/guid.h>
#include <util/datetime/cputimer.h>
#include <util/stream/null.h>
#include <util/system/env.h>
#include <util/string/printf.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/threading/local_executor/local_executor.h>

#include <deque>
#include <stack>
#include <chrono>
#include <tuple>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/api/grpc/draft/ydb_s3_internal_v1.grpc.pb.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>

using namespace NYdb;

struct S3Path {
    TString Name;
    std::unordered_map<TString, S3Path> Subpaths;
    std::unordered_set<TString> Files;

    S3Path(TString name) : Name(name) {}
};

static std::vector<TString> FolderNames = {"foo", "bar", "baz", "test"};

TString GenerateRandomName() {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static const int charsetSize = sizeof(charset) - 1;
    TString path;

    for (int j = 0; j < rand() % 10 + 1; ++j) {
        path += charset[rand() % charsetSize];
    }

    return path;
}


bool Verbose = false;
ui64 MaxInFlight = 4;
ui32 RowsToUpsert = 1000;
ui32 BatchSize = 10;
ui32 MaxDepth = 5;
bool Check = false;
bool RunRandomQueries = true;
S3Path RootFolder("Root");
std::set<TString> PathsToQueryFor;

std::mutex FolderMutex;

TString BuildPathStructure(S3Path* root, int depth) {
    std::lock_guard<std::mutex> lock(FolderMutex);
    Y_ABORT_UNLESS(depth < 10);
    S3Path *cur = root;
    TStringBuilder name;
    name << "/" << root->Name << "/";

    for (int i = 0; i < depth; ++i) {
        if (i < (depth - 1)) {
            TString folderName = FolderNames[rand() % FolderNames.size()];
            name << folderName << "/";

            if (Check) {
                // Only save folders if we are going to check them later.
                auto [it, inserted] = cur->Subpaths.try_emplace(folderName, folderName);    
                S3Path *path = &it->second;
                cur = path;
            }
        } else {
            TString fName = GenerateRandomName();
            name << fName;

            if (Check) {
                // Only save file names if we are going to check them later.
                cur->Files.emplace(fName);
            }
        }
    }

    if (RunRandomQueries) {
        auto e = (size_t) (RowsToUpsert * 0.1);
        if ((rand() % 2) && PathsToQueryFor.size() < e) {
            PathsToQueryFor.emplace(name);
        }
    }

    return name;
}

bool WriteBatch(NTable::TTableClient& tableClient, const TString& table) {
    Y_ABORT_UNLESS(RowsToUpsert > 0);
    std::atomic<ui64> upserted(0);

    NPar::LocalExecutor().RunAdditionalThreads(MaxInFlight);

    NPar::LocalExecutor().ExecRange([=, &upserted, &tableClient](int /*id*/) {
        while (upserted < RowsToUpsert) {
            TValueBuilder rows;
            rows.BeginList();
            for (ui32 i = 0; i < BatchSize; ++i) {
                TString path = BuildPathStructure(&RootFolder, 1 + rand() % MaxDepth);
                rows.AddListItem()
                        .BeginStruct()
                        .AddMember("bucket_id").Uint64(50)
                        .AddMember("path").Utf8(path)
                        .AddMember("blob_id").Utf8("foobar")
                        .EndStruct();
            }
            rows.EndList();

            auto status = tableClient.RetryOperationSync([&](NTable::TTableClient tableClient) -> TStatus {
                return tableClient.BulkUpsert(table, rows.Build()).GetValueSync();
            });
            if (status.IsSuccess()) {
                ++upserted;
            } else {
                Cerr << status << Endl;
            }
            if (Verbose && upserted % 1'000 == 0) {
                Cerr << "upserted = " << upserted.load() << " batches" << Endl;
            }
        }
    }, 0, MaxInFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

    return true;
}

bool CreateTable(NTable::TTableClient& client, const TString& table) {
    Cerr << "Create table " << table << "\n";

    NTable::TRetryOperationSettings settings;
    auto status = client.RetryOperationSync([&table](NTable::TSession session) {
            TString createTable = TStringBuilder() << R"(
                    CREATE TABLE `)" << table << R"(`
                    (
                        bucket_id uint64,
                        path Utf8,
                        blob_id Utf8 NOT NULL,
                        PRIMARY KEY (bucket_id, path)
                    );

                )";
            return session.ExecuteSchemeQuery(createTable).GetValueSync();
        }, settings);
    if (!status.IsSuccess()) {
        Cerr << "Create table failed with status: " << status << Endl;
        return false;
    }
    return true;
}

void ClearTable(NTable::TTableClient& client, const TString& table) {
    Cerr << "Clear table " << table << "\n";

    NTable::TRetryOperationSettings settings;
    auto status = client.RetryOperationSync([&table](NTable::TSession session) {
            TString createTable = TStringBuilder() << R"(DELETE FROM `)" << table << R"(`;)";

            return session.ExecuteDataQuery(createTable, NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        }, settings);
    if (!status.IsSuccess()) {
        Cerr << "Clear table failed with status: " << status << Endl;
    }
}

std::tuple<std::set<TString>, std::set<TString>> ListingRequest(std::unique_ptr<Ydb::S3Internal::V1::S3InternalService::Stub> &stub, TString table, TString path, TString after) {
    TString keyPrefix = R"(
        type {
            tuple_type {
                elements {
                    type_id: UINT64
                }
            }
        }
        value {
            items {
                uint64_value: )" + ToString(50) + R"(
            }
        }
    )";

    TAutoPtr<Ydb::S3Internal::S3ListingRequest> request = new Ydb::S3Internal::S3ListingRequest();
    request->Setpath_column_prefix(path);
    request->Setpath_column_delimiter("/");
    request->Settable_name(table);

    bool parseOk = ::google::protobuf::TextFormat::ParseFromString(keyPrefix, request->mutable_key_prefix());
    Y_ABORT_UNLESS(parseOk);

    if (after) {
        TString pbStartAfterSuffix = R"(
            type {
                tuple_type {
                    elements {
                        type_id: UTF8
                    }
                }
            }
            value {
                items {
                    text_value: ")" + after + R"("
                }
            }
        )";

        parseOk = ::google::protobuf::TextFormat::ParseFromString(pbStartAfterSuffix, request->mutable_start_after_key_suffix());
        Y_ABORT_UNLESS(parseOk);
    }

    grpc::ClientContext rcontext;
    Ydb::S3Internal::S3ListingResponse listingResponse;
    grpc::Status status = stub->S3Listing(&rcontext, *request, &listingResponse);

    if (!status.ok()) {
        Cerr << "Listing request failed: " << status.error_message();
        return {};
    }

    Ydb::S3Internal::S3ListingResult listingResult;
    listingResponse.operation().result().UnpackTo(&listingResult);

    std::set<TString> foldersSet;
    std::set<TString> resSet;

    if (listingResult.has_common_prefixes()) {
        auto &folders = listingResult.common_prefixes();
        for (auto row : folders.rows()) {
            for (auto item : row.items()) {
                if (item.has_text_value()) {
                    foldersSet.emplace(item.text_value());
                    resSet.emplace(item.text_value());
                }
            }
        }
    }
    
    if (listingResult.has_contents()) {
        auto &files = listingResult.contents();
        for (auto row : files.rows()) {
            for (auto item : row.items()) {
                if (item.has_text_value()) {
                    resSet.emplace(item.text_value());
                }
            }
        }
    }

    return std::make_tuple(foldersSet, resSet);
}

std::chrono::milliseconds RunQueries(NTable::TTableClient& tableClient, const TString& table) {
    std::chrono::milliseconds totalMs = std::chrono::milliseconds(0);
    for (auto &path : PathsToQueryFor) {
        TValueBuilder rows;
        rows.BeginList();

        auto start = std::chrono::high_resolution_clock::now();

        auto status = tableClient.RetryOperationSync([&](NTable::TTableClient tableClient) -> TStatus {
            rows.AddListItem()
                    .BeginStruct()
                    .AddMember("bucket_id").Uint64(50)
                    .AddMember("path").Utf8(path)
                    .EndStruct()
                    .EndList();
            return tableClient.ReadRows(table, rows.Build()).GetValueSync();
        });

        if (!status.IsSuccess()) {
            Cerr << status << Endl;
        }

        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        totalMs += duration;
    }
    return totalMs;
}

std::tuple<bool, ui64, std::chrono::milliseconds> TraversePaths(const S3Path& root, std::unique_ptr<Ydb::S3Internal::V1::S3InternalService::Stub> &stub, const TString& table, const int threadId) {
    std::stack<std::pair<const S3Path*, TString>> stack;

    if (Check) {
        stack.push({&root, "/"});
    } else {
        stack.push({nullptr, "/Root/"});
    }

    ui64 totalRowCount = 0;
    std::chrono::milliseconds totalMs = std::chrono::milliseconds(0);
    while (!stack.empty()) {
        const S3Path* currentPath = stack.top().first;
        TString currentFullPath;
        if (Check) {
            currentFullPath = stack.top().second + currentPath->Name + "/";
        } else {
            currentFullPath = stack.top().second;
        }
        stack.pop();

        auto start = std::chrono::high_resolution_clock::now();

        TStringStream log;

        if (Verbose) {
            log << "[" << threadId << "] Listing for " << currentFullPath << Endl;
        }

        std::set<TString> expected;

        if (Check) {
            for (const auto& subpath : currentPath->Subpaths) {
                expected.emplace(currentFullPath + subpath.first + "/");
            }
            for (const auto& file : currentPath->Files) {
                expected.emplace(currentFullPath + file);
            }
        }

        ui64 actualSize = 0;
        std::set<TString> actual;
        std::set<TString> actualFolders;
        bool hasMore = true;
        TString after;

        while (hasMore) {
            auto [intermediateFolders, intermediateAll] = ListingRequest(stub, table, currentFullPath, after);
            if (intermediateAll.empty()) {
                hasMore = false;
            } else {
                after = *intermediateAll.rbegin();
            }
            if (Check) {
                actual.insert(intermediateAll.begin(), intermediateAll.end());
            }
            actualFolders.insert(intermediateFolders.begin(), intermediateFolders.end());
            actualSize += intermediateAll.size();
        }

        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        totalRowCount += actualSize;
        totalMs += duration;

        if (Verbose) {
            log << "Got " << actualSize << " rows in " << duration.count() << "ms" << Endl;

            Cerr << log.Str() << Endl;
        }

        if (Check && (expected != actual)) {
            Cerr << "Actual listing doesn't match expected" << Endl;
            
            if (Verbose) {
                Cerr << "Expected:" << Endl;
                for (auto exp : expected) {
                    Cerr << exp << Endl;
                }

                Cerr << "Actual:" << Endl;
                for (auto act : actual) {
                    Cerr << act << Endl;
                }
            }

            return std::make_tuple(false, totalRowCount, totalMs);
        }

        if (Check) {
            for (const auto& subpath : currentPath->Subpaths) {
                stack.push({&subpath.second, currentFullPath});
            }
        } else {
            for (const auto& subpath : actualFolders) {
                stack.push({nullptr, subpath});
            }
        }
    }

    return std::make_tuple(true, totalRowCount, totalMs);
}

bool TraversePathsParallel(const S3Path& root, std::unique_ptr<Ydb::S3Internal::V1::S3InternalService::Stub> stub, NTable::TTableClient& tableClient, const TString& table) {
    std::atomic_bool res = true;
    std::atomic_uint64_t totalRowCount = 0;
    std::atomic<std::chrono::milliseconds::rep> totalMs = 0;
    std::atomic<std::chrono::milliseconds::rep> randomQueriesTotalMs = 0;

    NPar::LocalExecutor().ExecRange([&](int id) {
        if ((id % 2) || !(RunRandomQueries)) {
            auto [result, responseRowCount, responseTotalMs] = TraversePaths(root, stub, table, id);

            if (!result) {
                res = false;
            }
            totalRowCount += responseRowCount;
            totalMs += responseTotalMs.count();
        } else {
            auto ms = RunQueries(tableClient, table);
            randomQueriesTotalMs += ms.count();
        }
    }, 0, MaxInFlight * 2, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    
    Cerr << "Read " << totalRowCount.load() << " rows in " << totalMs.load() << "ms" << Endl;

    if (RunRandomQueries) {
        auto ms = randomQueriesTotalMs.load();
        Cerr << "[During listings] Ran " << MaxInFlight << "x " << PathsToQueryFor.size() << " queries in " << ms << "ms"
            << " (avg " << ms / (MaxInFlight * PathsToQueryFor.size()) << "ms)"
            << Endl;

        randomQueriesTotalMs = 0;

        NPar::LocalExecutor().ExecRange([&](int /*id*/) {
            auto ms = RunQueries(tableClient, table);
            randomQueriesTotalMs += ms.count();
        }, 0, MaxInFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
        
        ms = randomQueriesTotalMs.load();
        
        Cerr << "[Without listings] Ran " << MaxInFlight << "x " << PathsToQueryFor.size() << " queries in " << ms << "ms"
            << " (avg " << ms / (MaxInFlight * PathsToQueryFor.size()) << "ms)"
            << Endl;
    }

    return res;
}

bool Run(const TDriver& driver, const TString& table, std::unique_ptr<Ydb::S3Internal::V1::S3InternalService::Stub> stub) {
    NTable::TClientSettings settings;
    settings.SessionPoolSettings(NTable::TSessionPoolSettings().MaxActiveSessions(200));
    NTable::TTableClient client(driver, settings);
    Cerr << "WriteBatch" << Endl;
    if (!WriteBatch(client, table))
        return false;

    Cerr << "Execute listings" << Endl;

    return TraversePathsParallel(RootFolder, std::move(stub), client, table);
}

TString JoinPath(const TString& basePath, const TString& path) {
    if (basePath.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(basePath);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}


int main(int argc, char** argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();

    TString endpoint;
    TString database;
    TString table;
    bool createTable = false;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database name").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('p', "table", "Path for table").Optional().RequiredArgument("PATH")
        .StoreResult(&table);
    opts.AddLongOption('u', "upsert_batches", "count of batches to upsert").Optional().RequiredArgument("NUM")
        .StoreResult(&RowsToUpsert).DefaultValue(RowsToUpsert);
    opts.AddLongOption('t', "create_table", "create table for test").Optional().RequiredArgument("BOOL")
        .StoreResult(&createTable).DefaultValue(createTable);
    opts.AddLongOption('i', "in_flight", "use that much parallel upsertion clients (each will upsert as many batches, as configured)").Optional().RequiredArgument("NUM")
        .StoreResult(&MaxInFlight).DefaultValue(MaxInFlight);
    opts.AddLongOption('b', "batch_size", "size of the batch to upsert").Optional().RequiredArgument("NUM")
        .StoreResult(&BatchSize).DefaultValue(BatchSize);
    opts.AddLongOption('D', "depth", "max path depth").Optional().RequiredArgument("NUM")
        .StoreResult(&MaxDepth).DefaultValue(MaxDepth);
    opts.AddLongOption('c', "check", "check listing result").Optional().RequiredArgument("BOOL")
        .StoreResult(&Check).DefaultValue(Check);
    opts.AddLongOption('q', "query", "check listing result").Optional().RequiredArgument("BOOL")
        .StoreResult(&RunRandomQueries).DefaultValue(RunRandomQueries);
    opts.AddLongOption('v', "verbose", "print debug").Optional().RequiredArgument("BOOL")
        .StoreResult(&Verbose).DefaultValue(Verbose);

    TOptsParseResult res(&opts, argc, argv);
    Y_UNUSED(res);

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database);
    
    TDriver driver(driverConfig);

    table = JoinPath(database, table);

    if (createTable) {
        NTable::TTableClient client(driver);
        if (!CreateTable(client, table)) {
            return 1;
        }
    }

    {
        NTable::TTableClient client(driver);
        ClearTable(client, table);
    }

    std::shared_ptr<grpc::Channel> channel;
    channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    std::unique_ptr<Ydb::S3Internal::V1::S3InternalService::Stub> stub;
    stub = Ydb::S3Internal::V1::S3InternalService::NewStub(channel);

    if (!Run(driver, table, std::move(stub))) {
        NTable::TTableClient client(driver);
        ClearTable(client, table);
        driver.Stop(true);
        return 2;
    }

    NTable::TTableClient client(driver);
    ClearTable(client, table);
    driver.Stop(true);

    return 0;
}
