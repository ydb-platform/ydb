#include "base_test_fixture.h"
#include "read_request_executor.h"
#include "read_request_multiple_location.h"
#include "read_request_single_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_checksums.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Compares checksum vectors element by element.
void AssertChecksums(
    const TBlockChecksums& expected,
    const TBlockChecksums& actual)
{
    UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(expected[i], actual[i]);
    }
}

// Builds a read for `range`, points its sglist at `buffer`, and starts it.
// `buffer` must outlive the returned executor: the request sglist points
// into it.
IReadRequestExecutorPtr StartRead(
    TBlockRange16 range,
    TReadHint readHint,
    TBaseFixture& fixture,
    TString* buffer)
{
    fixture.ExpectedRange = range;
    fixture.RangeData.assign(
        static_cast<size_t>(range.Size()) * fixture.BlockSize,
        'x');
    buffer->assign(fixture.RangeData.size(), '\0');

    auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
    auto request = std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
        .VolumeConfig = fixture.PartitionDirectService->GetVolumeConfig(),
        .RequestId = 1,
        .Range = ConvertRangeSafe<TBlockRange64>(range)});
    TSgList sglist;
    sglist.push_back(TBlockDataRef{buffer->data(), buffer->size()});
    request->Sglist = TGuardedSgList(std::move(sglist));

    auto executor = CreateReadRequestExecutor(
        fixture.Runtime->GetActorSystem(0),
        fixture.LogTitle,
        fixture.VChunkConfig,
        fixture.DirectBlockGroup,
        std::move(readHint),
        std::move(callContext),
        std::move(request),
        NWilson::TTraceId());
    executor->Run();
    return executor;
}

// A read whose first half is still on DDisk and whose second half is in a
// PBuffer record.
TReadHint MakeDDiskThenPBufferHint(TBlockRange16 range, TBaseFixture& fixture)
{
    UNIT_ASSERT(range.Size() >= 2);
    UNIT_ASSERT(range.Size() % 2 == 0);
    const TBlockRange16 pbufferRange = TBlockRange16::WithLength(
        static_cast<ui16>(range.Start + range.Size() / 2),
        static_cast<ui16>(range.Size() / 2));
    fixture.DirtyMap->RegisterInflightWrite(MakeKey(100), pbufferRange);
    fixture.DirtyMap->WriteFinished(
        MakeKey(100),
        pbufferRange,
        fixture.VChunkConfig.GetDesiredPBuffers(),
        fixture.VChunkConfig.GetDesiredPBuffers());
    return fixture.DirtyMap->MakeReadHint(range);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadRequestTest)
{
    Y_UNIT_TEST_F(ShouldCreateSingleLocationExecutor, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 1000);
        ExpectedRange = range;

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});

        auto readHint = DirtyMap->MakeReadHint(range);
        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());

        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadSingleLocationRequestExecutor>(
                readRequest) != nullptr);
        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadMultipleLocationRequestExecutor>(
                readRequest) == nullptr);

        auto future = readRequest->GetFuture();
        readRequest->Run();
        SetReadResult({.Error = MakeError(S_OK)}, false);
    }

    Y_UNIT_TEST_F(ShouldCreateMultipleLocationExecutor, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 1000);
        ExpectedRange = range;

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});

        DirtyMap->RegisterInflightWrite(
            MakeKey(100),
            TBlockRange16::WithLength(20, 10));
        DirtyMap->WriteFinished(
            MakeKey(100),
            TBlockRange16::WithLength(20, 10),
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());
        auto readHint = DirtyMap->MakeReadHint(range);
        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());

        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadMultipleLocationRequestExecutor>(
                readRequest) != nullptr);
        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadSingleLocationRequestExecutor>(
                readRequest) == nullptr);

        auto future = readRequest->GetFuture();
        readRequest->Run();
        SetReadResult({.Error = MakeError(S_OK)}, false);
    }

    Y_UNIT_TEST_F(ShouldRead, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 10);
        ExpectedRange = range;

        auto readHint = DirtyMap->MakeReadHint(range);
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        SetReadResult({.Error = MakeError(S_OK)}, false);
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReadMultipleLocations, TBaseFixture)
    {
        Init();

        DirtyMap->RegisterInflightWrite(
            MakeKey(100),
            TBlockRange16::WithLength(20, 10));
        DirtyMap->WriteFinished(
            MakeKey(100),
            TBlockRange16::WithLength(20, 10),
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());

        DirtyMap->RegisterInflightWrite(
            MakeKey(200),
            TBlockRange16::WithLength(40, 10));
        DirtyMap->WriteFinished(
            MakeKey(200),
            TBlockRange16::WithLength(40, 10),
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());

        const TBlockRange16 range = TBlockRange16::WithLength(10, 100);
        ExpectedRange = range;
        RangeData = GenerateRandomString(ExpectedRange.Size() * BlockSize);

        auto readHint = DirtyMap->MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(5, readHint.RangeHints.size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});
        TSgList sglist;
        TString readBuffer(RangeData.size(), '\0');
        sglist.push_back(TBlockDataRef{readBuffer.data(), readBuffer.size()});
        originalRequest->Sglist = TGuardedSgList(std::move(sglist));

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        SetReadResult({.Error = MakeError(S_OK)}, false);
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(RangeData, readBuffer);
    }

    Y_UNIT_TEST_F(ShouldHedgeReading, TBaseFixture)
    {
        Init();

        DirectBlockGroup->Oracle.ReadHedgingDelay = TDuration::Seconds(1);
        DirectBlockGroup->Oracle.ReadRequestTimeout = TDuration::Seconds(10);

        const TBlockRange16 range = TBlockRange16::WithLength(10, 10);
        ExpectedRange = range;

        auto readHint = DirtyMap->MakeReadHint(range);
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, ReadPromises.size());

        UNIT_ASSERT_VALUES_EQUAL(2, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(10),
            ScheduledTasks[0].Delay);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            ScheduledTasks[1].Delay);

        // Run hedging task
        ExpectedHost = 1;
        ScheduledTasks[1].Callback();
        UNIT_ASSERT_VALUES_EQUAL(2, ReadPromises.size());

        // Response with error for first request. This will trigger another
        // request to the third host.
        ExpectedHost = 2;
        ReadPromises[0].SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // Response with error for third request. This will not trigger another
        // request.
        ReadPromises[2].SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // Response with success for second request.
        ReadPromises[1].SetValue({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldHedgeAllReadAttempts, TBaseFixture)
    {
        Init();

        DirectBlockGroup->Oracle.ReadHedgingDelay = TDuration::Seconds(1);
        DirectBlockGroup->Oracle.ReadRequestTimeout = TDuration::Seconds(10);

        const TBlockRange16 range = TBlockRange16::WithLength(10, 10);
        ExpectedRange = range;

        auto readHint = DirtyMap->MakeReadHint(range);
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, ReadPromises.size());

        UNIT_ASSERT_VALUES_EQUAL(2, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(10),
            ScheduledTasks[0].Delay);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            ScheduledTasks[1].Delay);

        // Run hedging task #1. Will launch a new read request from H1.
        ExpectedHost = 1;
        ScheduledTasks[1].Callback();
        UNIT_ASSERT_VALUES_EQUAL(3, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, ReadPromises.size());

        // Run hedging task #2. Will launch a new read request from H2.
        ExpectedHost = 2;
        ScheduledTasks[2].Callback();
        UNIT_ASSERT_VALUES_EQUAL(4, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());

        // Run hedging task #3. Will not launch a new read request.
        ExpectedHost = 3;
        ScheduledTasks[3].Callback();
        UNIT_ASSERT_VALUES_EQUAL(4, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());

        // Response with error for first request.
        ReadPromises[0].SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // Response with error for second request.
        ReadPromises[1].SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // Response with success for third request.
        ReadPromises[2].SetValue({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldForwardDDiskReadChecksums, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 2);
        auto readHint = DirtyMap->MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(0, readHint.RangeHints[0].PBufferKey.Lsn);

        TString buffer;
        auto executor = StartRead(range, std::move(readHint), *this, &buffer);
        auto future = executor->GetFuture();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, ReadPromises.size());

        const TBlockChecksums checksums{11, 12};
        SetReadResult(
            {.Error = MakeError(S_OK), .Checksums = checksums},
            false);

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        AssertChecksums(checksums, response.Checksums);
    }

    Y_UNIT_TEST_F(ShouldForwardPBufferReadChecksums, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 2);
        DirtyMap->RegisterInflightWrite(MakeKey(100), range);
        DirtyMap->WriteFinished(
            MakeKey(100),
            range,
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());
        auto readHint = DirtyMap->MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT(readHint.RangeHints[0].PBufferKey.Lsn != 0);

        TString buffer;
        auto executor = StartRead(range, std::move(readHint), *this, &buffer);
        auto future = executor->GetFuture();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, ReadPromises.size());

        const TBlockChecksums checksums{21, 22};
        SetReadResult(
            {.Error = MakeError(S_OK), .Checksums = checksums},
            false);

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        AssertChecksums(checksums, response.Checksums);
    }

    Y_UNIT_TEST_F(ShouldJoinChecksumsInRangeOrder, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 4);
        auto readHint = MakeDDiskThenPBufferHint(range, *this);
        UNIT_ASSERT_VALUES_EQUAL(2, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(0, readHint.RangeHints[0].PBufferKey.Lsn);
        UNIT_ASSERT(readHint.RangeHints[1].PBufferKey.Lsn != 0);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readHint.RangeHints[0].RequestRelativeRange.Start);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            readHint.RangeHints[1].RequestRelativeRange.Start);

        TString buffer;
        auto executor = StartRead(range, std::move(readHint), *this, &buffer);
        auto future = executor->GetFuture();
        UNIT_ASSERT_VALUES_EQUAL(2, ReadPromises.size());

        // Complete the later piece first. The joined vector follows range
        // order, not completion order.
        const TBlockChecksums pbufferChecksums{201, 202};
        const TBlockChecksums ddiskChecksums{101, 102};
        ReadPromises[1].SetValue(
            {.Error = MakeError(S_OK), .Checksums = pbufferChecksums});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        ReadPromises[0].SetValue(
            {.Error = MakeError(S_OK), .Checksums = ddiskChecksums});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        AssertChecksums(
            TBlockChecksums{101, 102, 201, 202},
            response.Checksums);
    }

    Y_UNIT_TEST_F(ShouldReturnEmptyChecksumsWhenDisabled, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 4);
        auto readHint = MakeDDiskThenPBufferHint(range, *this);
        UNIT_ASSERT_VALUES_EQUAL(2, readHint.RangeHints.size());

        TString buffer;
        auto executor = StartRead(range, std::move(readHint), *this, &buffer);
        auto future = executor->GetFuture();
        UNIT_ASSERT_VALUES_EQUAL(2, ReadPromises.size());

        // Checksums disabled: every piece succeeds with an empty vector.
        ReadPromises[0].SetValue({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        ReadPromises[1].SetValue({.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT(response.Checksums.empty());
    }

    Y_UNIT_TEST_F(ShouldForwardChecksumsFromRetriedHost, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 2);
        auto readHint = DirtyMap->MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        const TVector<THostIndex> hosts =
            readHint.RangeHints[0].HostMask.Hosts();
        UNIT_ASSERT_GE(hosts.size(), 2u);

        TString buffer;
        ExpectedHost = hosts[0];
        auto executor = StartRead(range, std::move(readHint), *this, &buffer);
        auto future = executor->GetFuture();
        UNIT_ASSERT_VALUES_EQUAL(1, ReadPromises.size());

        // The retry is issued from inside SetValue, so the next host must
        // already be the one the mock expects.
        ExpectedHost = hosts[1];
        ReadPromises[0].SetValue({.Error = MakeError(E_IO)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, ReadPromises.size());

        const TBlockChecksums checksums{31, 32};
        ReadPromises[1].SetValue(
            {.Error = MakeError(S_OK), .Checksums = checksums});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        AssertChecksums(checksums, response.Checksums);
    }

    Y_UNIT_TEST_F(ShouldReturnEmptyChecksumsOnError, TBaseFixture)
    {
        Init();

        const TBlockRange16 range = TBlockRange16::WithLength(10, 2);
        auto readHint = DirtyMap->MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        const TVector<THostIndex> hosts =
            readHint.RangeHints[0].HostMask.Hosts();
        UNIT_ASSERT(!hosts.empty());

        TString buffer;
        ExpectedHost = hosts[0];
        auto executor = StartRead(range, std::move(readHint), *this, &buffer);
        auto future = executor->GetFuture();

        // Each failed host still carries checksums. The reply must drop them.
        const TBlockChecksums stray{7, 8};
        for (size_t i = 0; i < hosts.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i + 1, ReadPromises.size());
            UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
            if (i + 1 < hosts.size()) {
                ExpectedHost = hosts[i + 1];
            }
            ReadPromises[i].SetValue(
                {.Error = MakeError(E_FAIL), .Checksums = stray});
        }

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT(HasError(response.Error));
        UNIT_ASSERT(response.Checksums.empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
