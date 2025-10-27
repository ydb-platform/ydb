#include "key_value.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;

TWriteJob::TWriteJob(const TCommonOptions& opts, std::uint32_t maxId)
    : TThreadJob(opts, "write")
    , Executor(opts, Stats, TExecutor::ModeNonBlocking)
    , Generator(opts, maxId)
{}

void TWriteJob::ShowProgress(TStringBuilder& report) {
    report << Endl << "=====- WriteJob report (Thread B) -=====" << Endl;
    Executor.Report(report);
    report << "Generated " << ValuesGenerated.load() << " new elements." << Endl
        << "Generator compute time: " << Generator.GetComputeTime() << Endl;
    Stats.PrintStatistics(report);
    report << "==================================================" << Endl;
}

void TWriteJob::DoJob() {
    while (!ShouldStop.load() && TInstant::Now() < Deadline) {
        TKeyValueRecordData recordData = Generator.Get();
        auto value = BuildValueFromRecord(recordData);

        ValuesGenerated.fetch_add(1);
        
        auto upload = [value{ std::move(value) }, this](TSession session)->TAsyncStatus {
            static const TString query = Sprintf(R"(
--!syntax_v1
PRAGMA TablePathPrefix("%s");

DECLARE $items AS
    List<Struct<
        `object_id_key`: Uint32,
        `object_id`: Uint32,
        `timestamp`: Uint64,
        `payload`: Utf8>>;

UPSERT INTO `%s` SELECT * FROM AS_TABLE($items);

)", Prefix.c_str(), TableName.c_str());

            auto promise = NThreading::NewPromise<TStatus>();
            auto params = PackValuesToParamsAsList({value});

            auto resultFuture = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                std::move(params),
                TExecDataQuerySettings()
                    .KeepInQueryCache(true)
                    .OperationTimeout(MaxDelay + ReactionTimeDelay)
                    .ClientTimeout(MaxDelay + ReactionTimeDelay)
            );

            resultFuture.Subscribe([promise](TAsyncDataQueryResult queryFuture) mutable {
                Y_ABORT_UNLESS(queryFuture.HasValue());
                TDataQueryResult queryResult = queryFuture.GetValue();
                promise.SetValue(std::move(queryResult));
            });

            return promise.GetFuture();
        };

        RpsProvider.Use();

        if (!Executor.Execute(upload)) {
            break;
        }
    }
}

void TWriteJob::OnFinish() {
    Executor.Finish();
    Executor.Wait();
}

// Implementation of TReadJob
TReadJob::TReadJob(const TCommonOptions& opts, std::uint32_t maxId)
    : TThreadJob(opts, "read")
    , Executor(std::make_unique<TExecutor>(opts, Stats))
    , ObjectIdRange(static_cast<std::uint32_t>(maxId * 1.25)) // 20% of requests with no result
{}

void TReadJob::ShowProgress(TStringBuilder& report) {
    report << Endl << "======- ReadJob report (Thread A) -======" << Endl;
    Executor->Report(report);
    Stats.PrintStatistics(report);
    report << "========================================" << Endl;
}

void TReadJob::DoJob() {
    while (!ShouldStop.load() && TInstant::Now() < Deadline) {
        std::uint32_t idToSelect = RandomNumber<std::uint32_t>() % ObjectIdRange;

        auto read = [idToSelect, this](TSession session) -> TAsyncStatus {
            static const TString query = Sprintf(R"(
--!syntax_v1
PRAGMA TablePathPrefix("%s");

DECLARE $object_id_key AS Uint32;
DECLARE $object_id AS Uint32;

SELECT * FROM `%s` WHERE `object_id_key` = $object_id_key AND `object_id` = $object_id;

)", Prefix.c_str(), TableName.c_str());

            auto promise = NThreading::NewPromise<TStatus>();
            auto params = TParamsBuilder()
                .AddParam("$object_id_key")
                    .Uint32(GetHash(idToSelect))
                    .Build()
                .AddParam("$object_id")
                    .Uint32(idToSelect)
                    .Build()
                .Build();

            auto resultFuture = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                std::move(params),
                TExecDataQuerySettings()
                    .KeepInQueryCache(true)
                    .OperationTimeout(MaxDelay + ReactionTimeDelay)
                    .ClientTimeout(MaxDelay + ReactionTimeDelay)
            );

            resultFuture.Subscribe([promise](TAsyncDataQueryResult queryFuture) mutable {
                Y_ABORT_UNLESS(queryFuture.HasValue());
                TDataQueryResult queryResult = queryFuture.GetValue();
                promise.SetValue(std::move(queryResult));
            });

            return promise.GetFuture();
        };

        RpsProvider.Use();

        if (!Executor->Execute(read)) {
            break;
        }
    }
}

void TReadJob::OnFinish() {
    Executor->Finish();
    std::uint32_t infly = Executor->Wait(WaitTimeout);
    if (infly) {
        Cerr << "Warning: thread A finished while having " << infly << " infly requests." << Endl;
    }
}
