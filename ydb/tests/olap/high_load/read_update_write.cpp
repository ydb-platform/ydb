#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <util/system/env.h>

#include <thread>


TString GetDatabase() {
    TString database = GetTestParam("ydb-db", GetEnv("YDB_DATABASE"));
    return database.StartsWith("/") ? database : "/" + database;
}

TString GetEndpoint() {
    return GetTestParam("ydb-endpoint", GetEnv("YDB_ENDPOINT"));
}

uint64_t GetTableSizeMiB() {
    return stoll(GetTestParam("table_size_mib", "64"));
}

class TTimeHistogram {
public:
    TTimeHistogram(const std::string& name)
        : Name(name)
    {}

    void TimeIt(const std::function<void()>& func) {
        auto start = TInstant::Now();
        func();
        auto end = TInstant::Now();
        ResponseTimes.push_back(end - start);
    }

    friend IOutputStream& operator<<(IOutputStream& os, TTimeHistogram& histogram) {
        std::sort(histogram.ResponseTimes.begin(), histogram.ResponseTimes.end());
        if (histogram.ResponseTimes.empty()) {
            return os << histogram.Name << ": empty";
        }
        return os << histogram.Name
                << ": 10% " << histogram.Percentile(0.10)
                << " 30% " << histogram.Percentile(0.30)
                << " 50% " << histogram.Percentile(0.50)
                << " 90% " << histogram.Percentile(0.90)
                << " 99% " << histogram.Percentile(0.99);
    }

private:
    TString Percentile(double percentile) {
        return ResponseTimes[percentile * ResponseTimes.size()].ToString();
    }

private:
    std::string Name;
    std::vector<TDuration> ResponseTimes;
};

class TRandomString {
public:
    TRandomString(size_t size) {
        Value.reserve(size);
        for (size_t i = 0; i < size; i++) {
            Value.push_back(AvailableChars[i % 62]);
        }
    }

    const std::string& Next() {
        std::swap(Value[random() % Value.size()], Value[random() % Value.size()]);
        return Value;
    }

private:
    static constexpr char const * AvailableChars{"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"};
    std::string Value;
};

class TRangeAllocator {
public:
    TRangeAllocator() = default;

    std::pair<uint64_t, uint64_t> AllocateRange(uint64_t length) {
        auto left = RightBorder.fetch_add(length);
        return std::make_pair(left, left + length);
    }

    uint64_t GetRightBorder() const {
        return RightBorder;
    }

private:
    std::atomic_uint64_t RightBorder{0};
};

class TThreadGroup {
public:
    TThreadGroup() = default;

    void AddThread(std::thread&& thread) {
        Threads.emplace_back(std::move(thread));
    }

    void JoinAll() {
        for (auto& thread: Threads) {
            thread.join();
        }
    }

    void DetachAll() {
        for (auto& thread: Threads) {
            thread.detach();
        } 
    }

private:
    std::vector<std::thread> Threads;
};

size_t DivUp(size_t a, size_t b) {
    return a % b == 0 ? a / b : a / b + 1;
}

class TLoopUpsert {
public:
    TLoopUpsert(NYdb::NTable::TTableClient& client, TRangeAllocator& rangeAllocator, size_t sizeKiB)
        : Client(client)
        , RangeAllocator(rangeAllocator)
        , SizeKiB(sizeKiB)
    {}

    void operator()() {
        TRandomString randomValue(100 * 1024);
        TTimeHistogram hist("Write");
        size_t counterKeys = DivUp(SizeKiB, 100);

        const TString tablePath = GetDatabase() + "/scenario/TestReadUpdateWriteLoad/read_update_write_load/big_table";
    
        for (size_t j = 0; j < counterKeys; j += 200) {
            auto range = RangeAllocator.AllocateRange(std::min(counterKeys - j, 200ul));
            
            NYdb::TValueBuilder rowsBuilder;
            rowsBuilder.BeginList();
            for (size_t i = range.first; i < range.second; i++) {
                rowsBuilder.AddListItem()
                .BeginStruct()
                    .AddMember("key").Int64(i)
                    .AddMember("value").OptionalUtf8(randomValue.Next())
                .EndStruct();
            }
            rowsBuilder.EndList();

            hist.TimeIt([&] {
                auto result = Client.BulkUpsert(tablePath, rowsBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            });
        }

        Cerr << hist << Endl;
    }
private:
    NYdb::NTable::TTableClient& Client;
    TRangeAllocator& RangeAllocator;
    size_t SizeKiB;
};

class TProgressTracker {
public:
    TProgressTracker(TRangeAllocator& rangeAllocator, std::atomic_bool& finished)
        : RangeAllocator(rangeAllocator)
        , Finished(finished)
    {}

    void operator()() {
        auto previousBorder = RangeAllocator.GetRightBorder();
        while (!Finished) {
            auto currentBorder = RangeAllocator.GetRightBorder() * 100 / 1024;
            Cerr << "Was written: " << currentBorder << " MiB, Speed: " << (currentBorder - previousBorder) / 10 << " MiB/s" << Endl;
            previousBorder = currentBorder;
            Sleep(TDuration::Seconds(10));
        }
    }

private:
    TRangeAllocator& RangeAllocator;
    std::atomic_bool& Finished;
};

class TLoopRandomRead {
public:
    TLoopRandomRead(NYdb::NQuery::TQueryClient& client, TRangeAllocator& rangeAllocator, std::atomic_bool& finished)
        : Client(client)
        , RangeAllocator(rangeAllocator)
        , Finished(finished)
    {}

    void operator()() {
        using namespace fmt::literals;
        TTimeHistogram hist("Read");
        while (!Finished) {
            int readTo = random() % RangeAllocator.GetRightBorder();
            int readFrom = std::max(readTo - 1024, 0);
            hist.TimeIt([&]() {
                auto result = Client.ExecuteQuery(fmt::format("SELECT * FROM `scenario/TestReadUpdateWriteLoad/read_update_write_load/big_table` WHERE {read_from} <= key and key <= {read_to};", "read_from"_a = readFrom, "read_to"_a = readTo), NYdb::NQuery::TTxControl::NoTx(),  NYdb::NQuery::TExecuteQuerySettings{}).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            });
        }
        Cerr << hist << Endl;
    }

private:
    NYdb::NQuery::TQueryClient& Client;
    TRangeAllocator& RangeAllocator;
    std::atomic_bool& Finished;
};

class TLoopRandomUpdate {
public:
    TLoopRandomUpdate(NYdb::NTable::TTableClient& client, TRangeAllocator& rangeAllocator, std::atomic_bool& finished)
        : Client(client)
        , RangeAllocator(rangeAllocator)
        , Finished(finished)
    {}

    void operator()() {
        TRandomString randomValue(100 * 1024);
        TTimeHistogram hist("Update");

        const TString tablePath = GetDatabase() + "/scenario/TestReadUpdateWriteLoad/read_update_write_load/big_table";
        while (!Finished) {
            int writeTo = random() % RangeAllocator.GetRightBorder();
            int writeFrom = std::max(writeTo - 30, 0);

            NYdb::TValueBuilder rowsBuilder;
            rowsBuilder.BeginList();
            for (int i = writeFrom; i < writeTo; i++) {
                rowsBuilder.AddListItem()
                .BeginStruct()
                    .AddMember("key").Int64(i)
                    .AddMember("value").OptionalUtf8(randomValue.Next())
                .EndStruct();
            }
            rowsBuilder.EndList();

            hist.TimeIt([&] {
                auto result = Client.BulkUpsert(tablePath, rowsBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            });
        }

        Cerr << hist << Endl;
    }

private:
    NYdb::NTable::TTableClient& Client;
    TRangeAllocator& RangeAllocator;
    std::atomic_bool& Finished;
};

Y_UNIT_TEST_SUITE(ReadUpdateWrite) {
    Y_UNIT_TEST(Load) {
        auto connection = NYdb::TDriver(NYdb::TDriverConfig().SetEndpoint(GetEndpoint()).SetDatabase(GetDatabase()));
        const size_t TableSizeMiB = GetTableSizeMiB();
        UNIT_ASSERT_GE(TableSizeMiB, 64);
        const size_t ParallelWrite = 64;

        NYdb::NTable::TTableClient tableClient(connection);
        NYdb::NQuery::TQueryClient queryClient(connection);
        auto sessionResult = tableClient.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), NYdb::EStatus::SUCCESS, sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        session.ExecuteSchemeQuery(R"(
            DROP TABLE `scenario/TestReadUpdateWriteLoad/read_update_write_load/big_table`
        )").GetValueSync();

        auto createTableResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `scenario/TestReadUpdateWriteLoad/read_update_write_load/big_table` (
                key Int64 NOT NULL,
                value Utf8,
                PRIMARY KEY (key)
            ) WITH (
                STORE=COLUMN
            )
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createTableResult.GetStatus(), NYdb::EStatus::SUCCESS, createTableResult.GetIssues().ToString());

        TRangeAllocator rangeAllocator;
        TThreadGroup progressTacker;
        std::atomic_bool progressFinished;
        progressTacker.AddThread(std::thread(TProgressTracker{rangeAllocator, progressFinished}));
        

        Cerr << "Step 1. only write" << Endl;
        {
            TThreadGroup upsertGroup;
            for (size_t i = 0; i < ParallelWrite; i++) {
                upsertGroup.AddThread(std::thread(TLoopUpsert{tableClient, rangeAllocator, TableSizeMiB * 1024 / 10 / ParallelWrite}));
            }
            upsertGroup.JoinAll();
        }

        Cerr << "Step 2. read write" << Endl;
        {
            TThreadGroup upsertGroup;
            for (size_t i = 0; i < ParallelWrite; i++) {
                upsertGroup.AddThread(std::thread(TLoopUpsert{tableClient, rangeAllocator, TableSizeMiB * 1024 / 10 / ParallelWrite}));
            }

            std::atomic_bool finished;

            TThreadGroup readGroup;
            readGroup.AddThread(std::thread(TLoopRandomRead{queryClient, rangeAllocator, finished}));

            upsertGroup.JoinAll();
            finished = true;
            readGroup.JoinAll();
        }

        Cerr << "Step 3. write modify" << Endl;

        {
            TThreadGroup upsertGroup;
            for (size_t i = 0; i < ParallelWrite; i++) {
                upsertGroup.AddThread(std::thread(TLoopUpsert{tableClient, rangeAllocator, TableSizeMiB * 1024 / 10 / ParallelWrite}));
            }

            std::atomic_bool finished;

            TThreadGroup updateGroup;
            updateGroup.AddThread(std::thread(TLoopRandomUpdate{tableClient, rangeAllocator, finished}));

            upsertGroup.JoinAll();
            finished = true;
            updateGroup.JoinAll();
        }

        Cerr << "Step 4. read modify write" << Endl;
        {
            TThreadGroup upsertGroup;
            for (size_t i = 0; i < ParallelWrite; i++) {
                upsertGroup.AddThread(std::thread(TLoopUpsert{tableClient, rangeAllocator, TableSizeMiB * 1024 * 7 / 10 / ParallelWrite}));
            }

            std::atomic_bool finished;

            TThreadGroup updateGroup;
            updateGroup.AddThread(std::thread(TLoopRandomUpdate{tableClient, rangeAllocator, finished}));

            TThreadGroup readGroup;
            readGroup.AddThread(std::thread(TLoopRandomRead{queryClient, rangeAllocator, finished}));

            upsertGroup.JoinAll();
            finished = true;
            updateGroup.JoinAll();
            readGroup.JoinAll();
        }

        progressFinished = true;
        progressTacker.JoinAll();
    }
}