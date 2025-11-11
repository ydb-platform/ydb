#include "key_value.h"

#include <util/string/printf.h>

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;


TGenerateInitialContentJob::TGenerateInitialContentJob(const TCreateOptions& createOpts, std::uint32_t maxId)
    : TThreadJob(createOpts.CommonOptions)
    , Executor(createOpts.CommonOptions, Stats, TExecutor::ModeBlocking)
    , PackGenerator(
        createOpts.CommonOptions
        , createOpts.PackSize
        , [](const TKeyValueRecordData& recordData) { return BuildValueFromRecord(recordData); }
        , createOpts.Count
        , maxId
    )
    , Total(createOpts.Count)
{}

void TGenerateInitialContentJob::ShowProgress(TStringBuilder& report) {
    report << Endl << "======- GenerateInitialContentJob report -======" << Endl;
    Executor.Report(report);
    TDuration timePassed = TInstant::Now() - Stats.GetStartTime();
    std::uint64_t rps = (Total - PackGenerator.GetRemain()) * 1000000 / timePassed.MicroSeconds();
    report << "Generated " << Total - PackGenerator.GetRemain() << " new elements." << Endl
        << "With pack_size=" << PackGenerator.GetPackSize() << " its " << rps << " rows/sec" << Endl
        << "Generator compute time: " << PackGenerator.GetComputeTime() << Endl;
    Stats.PrintStatistics(report);
    report << "========================================" << Endl;
}

void TGenerateInitialContentJob::DoJob() {
    std::vector<TValue> pack;
    while (!ShouldStop.load() && PackGenerator.GetNextPack(pack)) {
        auto upload = [pack{ std::move(pack) }, this](TSession session)->TAsyncStatus {
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
            auto params = PackValuesToParamsAsList(pack);

            auto resultFuture = session.ExecuteDataQuery(
                query
                , TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
                , std::move(params)
                , TExecDataQuerySettings()
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

        if (!Executor.Execute(upload)) {
            break;
        }
    }
}

void TGenerateInitialContentJob::OnFinish() {
    Executor.Finish();
    Executor.Wait();
    Stats.Flush();
}
