#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/erase_request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/flush_request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/read_request_executor.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/restore_request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_request_test_fixture.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <benchmark/benchmark.h>

using namespace NKikimr;
using namespace NThreading;
using namespace NYdb::NBS;
using namespace NYdb::NBS::NBlockStore;
using namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect;

namespace {

const TBlockRange64 BenchRange = TBlockRange64::WithLength(10, 1000);

TRequestHeaders MakeHeaders(const TBlockRange64& range, ui32 blockSize)
{
    auto volumeConfig = std::make_shared<TVolumeConfig>(TVolumeConfig{
        .DiskId = "disk-1",
        .BlockSize = blockSize,
        .BlockCount = 65536,
        .BlocksPerStripe = 1024,
        .VChunkSize = DefaultVChunkSize});

    return TRequestHeaders{
        .VolumeConfig = std::move(volumeConfig),
        .RequestId = 1,
        .Range = range};
}

std::shared_ptr<TWriteRequestBundle> MakeWriteBundle(
    TWriteRequestTestFixture& f)
{
    auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
        MakeHeaders(f.Range, f.BlockSize));
    originalRequest->Sglist = f.MakeSgList();

    auto bundle = std::make_shared<TWriteRequestBundle>(
        f.Runtime->GetActorSystem(0),
        f.WriteClient,
        std::move(originalRequest),
        NWilson::TTraceId(),
        MakeIntrusive<TCallContext>(),
        f.Range);
    bundle->SetLsn(f.UserLsn);
    return bundle;
}

std::shared_ptr<TReadBlocksLocalRequest> MakeReadRequest(
    TWriteRequestTestFixture& f)
{
    auto request = std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
        .VolumeConfig = f.PartitionDirectService->GetVolumeConfig(),
        .RequestId = 1,
        .Range = f.Range});
    request->Sglist = f.MakeSgList();
    return request;
}

void InitFixture(TWriteRequestTestFixture& f)
{
    f.Range = BenchRange;
    f.Init();
    f.Runtime->SetLogPriority(
        NKikimrServices::NBS_PARTITION,
        NActors::NLog::PRI_ERROR);

    // Zero every delay/timeout so hedging/timeout Schedule paths are skipped
    // (executors early-return on zero) and flush cooldown calls DoRun() inline.
    f.HedgeDelay = TDuration::Zero();
    f.Timeout = TDuration::Zero();
    f.PBufferReplyTimeout = TDuration::Zero();
    f.DirectBlockGroup->Oracle.ReadHedgingDelay = TDuration::Zero();
    f.DirectBlockGroup->Oracle.ReadRequestTimeout = TDuration::Zero();
    f.DirectBlockGroup->Oracle.WriteHedgingDelay = TDuration::Zero();
    f.DirectBlockGroup->Oracle.WriteRequestTimeout = TDuration::Zero();
    f.DirectBlockGroup->Oracle.PBufferReplyTimeout = TDuration::Zero();
    f.DirectBlockGroup->Oracle.FlushRequestCooldown = TDuration::Zero();
    f.DirectBlockGroup->Oracle.FlushRequestTimeout = TDuration::Zero();
    f.DirectBlockGroup->Oracle.EraseRequestTimeout = TDuration::Zero();
    f.DirectBlockGroup->Oracle.WriteMode = EWriteMode::IndirectWrite;

    // Run() must complete synchronously so drained executors can be destroyed.
    // With the zeros above, Schedule is not used on the happy path. Still run
    // the callback immediately if something does schedule (e.g. a non-zero
    // flush cooldown), so previous->Run() cannot leave an unreplied executor.
    f.DirectBlockGroup->ScheduleHandler =
        [](TDuration delay, TCallback callback)
    {
        Y_UNUSED(delay);
        callback();
    };
    f.DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
        f.GetManyPBuffersHandlerWithImmediateOkResponse();
    // Contiguous reads go to DDisk; dirty-map splits go to PBuffer.
    auto immediateReadOk = [](auto&&...)
    {
        return MakeFuture(TDBGReadBlocksResponse{.Error = MakeError(S_OK)});
    };
    f.DirectBlockGroup->ReadBlocksFromDDiskHandler = immediateReadOk;
    f.DirectBlockGroup->ReadBlocksFromPBufferHandler = immediateReadOk;
    f.DirectBlockGroup->BatchEraseFromPBufferHandler =
        [](THostIndex, const TEraseSegments&, const NWilson::TTraceId&)
    {
        return MakeFuture(TDBGEraseResponse{.Error = MakeError(S_OK)});
    };
    f.DirectBlockGroup->SyncWithPBufferHandler =
        [](ui32,
           THostIndex,
           THostIndex,
           const TVector<TPBufferSegment>&,
           const NWilson::TTraceId&)
    {
        return MakeFuture(TDBGFlushResponse{.Errors = {MakeError(S_OK)}});
    };
    f.DirectBlockGroup->ListPBuffersHandler = [](THostIndex)
    {
        return MakeFuture(TListPBufferResponse{.Error = MakeError(S_OK)});
    };

    // Split the range so CreateReadRequestExecutor picks the multi-location
    // path when MakeReadHint is called for BM_ReadMultiple*.
    f.DirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(20, 10));
    f.DirtyMap->WriteFinished(
        100,
        TBlockRange64::WithLength(20, 10),
        f.VChunkConfig.GetDesiredPBuffers(),
        f.VChunkConfig.GetDesiredPBuffers());
}

}   // namespace

// Setup (retire previous request, build inputs) is excluded via PauseTiming.

static void BM_WriteRequestExecutorCreation(benchmark::State& state)
{
    TWriteRequestTestFixture fixture;
    InitFixture(fixture);

    TWriteRequestExecutorPtr previous;
    for (auto _: state) {
        state.PauseTiming();
        if (previous) {
            previous->Run();
            previous.reset();
        }
        auto bundle = MakeWriteBundle(fixture);
        state.ResumeTiming();

        auto executor = CreateWriteRequestExecutor(
            fixture.Runtime->GetActorSystem(0),
            fixture.LogTitle,
            fixture.VChunkConfig,
            fixture.DirectBlockGroup,
            std::move(bundle));
        auto* executorPtr = &executor;
        benchmark::DoNotOptimize(executorPtr);
        previous = std::move(executor);
    }
    if (previous) {
        previous->Run();
    }
}

static void BM_ReadSingleLocationRequestExecutorCreation(
    benchmark::State& state)
{
    TWriteRequestTestFixture fixture;
    InitFixture(fixture);

    // Contiguous hint: a fresh dirty map without the split registered above.
    auto cleanDirtyMap = std::make_shared<TBlocksDirtyMap>(
        fixture.VChunkConfig,
        fixture.BlockSize,
        fixture.VChunkBlockCount);

    IReadRequestExecutorPtr previous;
    for (auto _: state) {
        state.PauseTiming();
        if (previous) {
            previous->Run();
            previous.reset();
        }
        auto readHint = cleanDirtyMap->MakeReadHint(fixture.Range);
        auto request = MakeReadRequest(fixture);
        state.ResumeTiming();

        auto executor = CreateReadRequestExecutor(
            fixture.Runtime->GetActorSystem(0),
            fixture.LogTitle,
            fixture.VChunkConfig,
            fixture.DirectBlockGroup,
            std::move(readHint),
            MakeIntrusive<TCallContext>(),
            std::move(request),
            NWilson::TTraceId());
        auto* executorPtr = &executor;
        benchmark::DoNotOptimize(executorPtr);
        previous = std::move(executor);
    }
    if (previous) {
        previous->Run();
    }
}

static void BM_ReadMultipleLocationRequestExecutorCreation(
    benchmark::State& state)
{
    TWriteRequestTestFixture fixture;
    InitFixture(fixture);

    IReadRequestExecutorPtr previous;
    for (auto _: state) {
        state.PauseTiming();
        if (previous) {
            previous->Run();
            previous.reset();
        }
        auto readHint = fixture.DirtyMap->MakeReadHint(fixture.Range);
        auto request = MakeReadRequest(fixture);
        state.ResumeTiming();

        auto executor = CreateReadRequestExecutor(
            fixture.Runtime->GetActorSystem(0),
            fixture.LogTitle,
            fixture.VChunkConfig,
            fixture.DirectBlockGroup,
            std::move(readHint),
            MakeIntrusive<TCallContext>(),
            std::move(request),
            NWilson::TTraceId());
        auto* executorPtr = &executor;
        benchmark::DoNotOptimize(executorPtr);
        previous = std::move(executor);
    }
    if (previous) {
        previous->Run();
    }
}

static void BM_EraseRequestExecutorCreation(benchmark::State& state)
{
    TWriteRequestTestFixture fixture;
    InitFixture(fixture);

    std::shared_ptr<TEraseRequestExecutor> previous;
    for (auto _: state) {
        state.PauseTiming();
        if (previous) {
            previous->Run();
            previous.reset();
        }
        TEraseHint hint;
        hint.Segments.push_back(TEraseSegment{.Generation = 1, .Lsn = 42});
        state.ResumeTiming();

        auto executor = std::make_shared<TEraseRequestExecutor>(
            fixture.Runtime->GetActorSystem(0),
            fixture.LogTitle,
            fixture.VChunkConfig,
            fixture.DirectBlockGroup,
            THostIndex{1},
            std::move(hint),
            NWilson::TSpan());
        auto* executorPtr = &executor;
        benchmark::DoNotOptimize(executorPtr);
        previous = std::move(executor);
    }
    if (previous) {
        previous->Run();
    }
}

static void BM_FlushRequestExecutorCreation(benchmark::State& state)
{
    TWriteRequestTestFixture fixture;
    InitFixture(fixture);

    std::shared_ptr<TFlushRequestExecutor> previous;
    for (auto _: state) {
        state.PauseTiming();
        if (previous) {
            previous->Run();
            previous.reset();
        }
        TFlushHint hint;
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 42,
            .Range = TBlockRange64::WithLength(10, 3)});
        state.ResumeTiming();

        auto executor = std::make_shared<TFlushRequestExecutor>(
            fixture.Runtime->GetActorSystem(0),
            fixture.LogTitle,
            fixture.VChunkConfig,
            fixture.DirectBlockGroup,
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 1},
            std::move(hint),
            NWilson::TSpan());
        auto* executorPtr = &executor;
        benchmark::DoNotOptimize(executorPtr);
        previous = std::move(executor);
    }
    if (previous) {
        previous->Run();
    }
}

static void BM_RestoreRequestExecutorCreation(benchmark::State& state)
{
    TWriteRequestTestFixture fixture;
    InitFixture(fixture);

    std::shared_ptr<TRestoreRequestExecutor> previous;
    for (auto _: state) {
        state.PauseTiming();
        if (previous) {
            previous->Run();
            previous.reset();
        }
        state.ResumeTiming();

        auto executor = std::make_shared<TRestoreRequestExecutor>(
            fixture.Runtime->GetActorSystem(0),
            fixture.DirectBlockGroup);
        auto* executorPtr = &executor;
        benchmark::DoNotOptimize(executorPtr);
        previous = std::move(executor);
    }
    if (previous) {
        previous->Run();
    }
}

BENCHMARK(BM_WriteRequestExecutorCreation)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_ReadSingleLocationRequestExecutorCreation)
    ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_ReadMultipleLocationRequestExecutorCreation)
    ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_EraseRequestExecutorCreation)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_FlushRequestExecutorCreation)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RestoreRequestExecutorCreation)->Unit(benchmark::kNanosecond);
