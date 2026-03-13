#include "direct_block_group.h"
#include "direct_block_group_in_mem.h"
#include "dirty_map.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

#include <ydb/core/mind/bscontroller/types.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <functional>

using namespace NYdb::NBS;
using namespace NYdb::NBS::NBlockStore;
using namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Тестовая DBG с подменяемыми обработчиками (по аналогии с TTestService).
// Если обработчик не задан — ответ по умолчанию.
////////////////////////////////////////////////////////////////////////////////

class TTestDirectBlockGroup : public IDirectBlockGroup
{
public:
    using TEstablishConnectionsHandler = std::function<NThreading::TFuture<void>(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex)>;

    using TRestoreFromPersistentBuffersHandler =
        std::function<TVector<TRestoreMeta>(
            TExecutorPtr executor,
            NWilson::TTraceId traceId,
            ui32 vChunkIndex)>;

    using TWriteBlocksLocalHandler =
        std::function<NThreading::TFuture<TDBGWriteBlocksResponse>(
            ui32 vChunkIndex,
            TCallContextPtr callContext,
            std::shared_ptr<TWriteBlocksLocalRequest> request,
            NWilson::TTraceId traceId)>;

    using TReadBlocksLocalFromPersistentBufferHandler =
        std::function<NThreading::TFuture<TDBGReadBlocksResponse>(
            ui32 vChunkIndex,
            ui8 persistentBufferIndex,
            TCallContextPtr callContext,
            std::shared_ptr<TReadBlocksLocalRequest> request,
            NWilson::TTraceId traceId,
            ui64 lsn)>;

    using TReadBlocksLocalFromDDiskHandler =
        std::function<NThreading::TFuture<TDBGReadBlocksResponse>(
            ui32 vChunkIndex,
            TCallContextPtr callContext,
            std::shared_ptr<TReadBlocksLocalRequest> request,
            NWilson::TTraceId traceId)>;

    using TSyncWithPersistentBufferHandler =
        std::function<NThreading::TFuture<TDBGSyncBlocksResponse>(
            ui32 vChunkIndex,
            ui8 persistBufferIndex,
            const TVector<TSyncRequest>& syncRequests,
            NWilson::TTraceId traceId)>;

    TEstablishConnectionsHandler EstablishConnectionsHandler;
    TRestoreFromPersistentBuffersHandler RestoreFromPersistentBuffersHandler;
    TWriteBlocksLocalHandler WriteBlocksLocalHandler;
    TReadBlocksLocalFromPersistentBufferHandler
        ReadBlocksLocalFromPersistentBufferHandler;
    TReadBlocksLocalFromDDiskHandler ReadBlocksLocalFromDDiskHandler;
    TSyncWithPersistentBufferHandler SyncWithPersistentBufferHandler;

    TTestDirectBlockGroup() = default;

    NThreading::TFuture<void> EstablishConnections(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) override
    {
        if (EstablishConnectionsHandler) {
            return EstablishConnectionsHandler(
                std::move(executor),
                std::move(traceId),
                vChunkIndex);
        }
        return NThreading::MakeFuture();
    }

    TVector<TRestoreMeta> RestoreFromPersistentBuffers(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) override
    {
        if (RestoreFromPersistentBuffersHandler) {
            return RestoreFromPersistentBuffersHandler(
                std::move(executor),
                std::move(traceId),
                vChunkIndex);
        }
        return {};
    }

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksLocal(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override
    {
        if (WriteBlocksLocalHandler) {
            return WriteBlocksLocalHandler(
                vChunkIndex,
                std::move(callContext),
                std::move(request),
                std::move(traceId));
        }
        return NThreading::MakeFuture(DefaultWriteResponse(1));
    }

    NThreading::TFuture<TDBGReadBlocksResponse>
    ReadBlocksLocalFromPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 lsn) override
    {
        if (ReadBlocksLocalFromPersistentBufferHandler) {
            return ReadBlocksLocalFromPersistentBufferHandler(
                vChunkIndex,
                persistentBufferIndex,
                std::move(callContext),
                std::move(request),
                std::move(traceId),
                lsn);
        }
        return NThreading::MakeFuture(DefaultReadResponse());
    }

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksLocalFromDDisk(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override
    {
        if (ReadBlocksLocalFromDDiskHandler) {
            return ReadBlocksLocalFromDDiskHandler(
                vChunkIndex,
                std::move(callContext),
                std::move(request),
                std::move(traceId));
        }
        return NThreading::MakeFuture(DefaultReadResponse());
    }

    NThreading::TFuture<TDBGSyncBlocksResponse> SyncWithPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistBufferIndex,
        const TVector<TSyncRequest>& syncRequests,
        NWilson::TTraceId traceId) override
    {
        if (SyncWithPersistentBufferHandler) {
            return SyncWithPersistentBufferHandler(
                vChunkIndex,
                persistBufferIndex,
                syncRequests,
                std::move(traceId));
        }
        return NThreading::MakeFuture(DefaultSyncResponse());
    }

private:
    static TDBGWriteBlocksResponse DefaultWriteResponse(ui64 lsn = 1)
    {
        TDBGWriteBlocksResponse r;
        r.Error = MakeError(S_OK);
        r.Meta.reserve(DirtyMapPersistentBuffersCount);
        for (size_t i = 0; i < DirtyMapPersistentBuffersCount; i++) {
            r.Meta.emplace_back(static_cast<ui8>(i), lsn);
        }
        return r;
    }

    static TDBGReadBlocksResponse DefaultReadResponse()
    {
        return TDBGReadBlocksResponse{.Error = MakeError(S_OK)};
    }

    static TDBGSyncBlocksResponse DefaultSyncResponse()
    {
        return TDBGSyncBlocksResponse{.Error = MakeError(S_OK)};
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4096;
constexpr ui64 BlocksCount = 32768;
constexpr ui32 VChunkIndex = 0;

NWilson::TTraceId TestTraceId()
{
    return NWilson::TTraceId::NewTraceId(15, 4095);
}

TRequestHeaders TestHeaders()
{
    auto volumeConfig = std::make_shared<TVolumeConfig>(
        "test-disk",
        BlockSize,
        1000,
        10);
    return TRequestHeaders{
        .VolumeConfig = volumeConfig,
        .RequestId = 1,
        .ClientId = "test",
        .Timestamp = TInstant::Zero()};
}

IDirectBlockGroupPtr CreateInMemoryDbg()
{
    TVector<NKikimr::NBsController::TDDiskId> ddiskIds(5);
    TVector<NKikimr::NBsController::TDDiskId> persistentBufferIds(5);
    return std::make_shared<TInMemoryDirectBlockGroup>(
        1,   // tabletId
        1,   // generation
        std::move(ddiskIds),
        std::move(persistentBufferIds),
        BlockSize,
        BlocksCount);
}

std::shared_ptr<TWriteBlocksLocalRequest> MakeWriteRequest(
    ui64 startBlock,
    ui64 blockCount,
    const TString& data)
{
    Y_ABORT_UNLESS(data.size() == blockCount * BlockSize);
    TBlockDataRef ref(data.data(), data.size());
    TGuardedSgList sglist({ref});
    auto request = std::make_shared<TWriteBlocksLocalRequest>(
        TestHeaders(),
        TBlockRange64::WithLength(startBlock, blockCount));
    request->Sglist = std::move(sglist);
    return request;
}

std::shared_ptr<TReadBlocksLocalRequest> MakeReadRequest(
    ui64 startBlock,
    ui64 blockCount)
{
    TString buffer(blockCount * BlockSize, 0);
    TBlockDataRef ref(buffer.data(), buffer.size());
    TGuardedSgList sglist({ref});
    auto request = std::make_shared<TReadBlocksLocalRequest>(
        TestHeaders(),
        TBlockRange64::WithLength(startBlock, blockCount));
    request->Sglist = std::move(sglist);
    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVChunkTest)
{
    Y_UNIT_TEST(DirtyMapUpdatedOnWriteReadBackCorrect)
    {
        auto dbg = CreateInMemoryDbg();
        auto vchunk = std::make_shared<TVChunk>(VChunkIndex, dbg, 1);
        vchunk->Start();
        Sleep(TDuration::MilliSeconds(50));

        TString data(BlockSize, 'X');
        auto writeReq = MakeWriteRequest(0, 1, data);
        auto writeFuture = vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeReq),
            TestTraceId());
        writeFuture.Wait();

        UNIT_ASSERT_C(
            !HasError(writeFuture.Get().Error),
            writeFuture.Get().Error.GetMessage());

        auto readReq = MakeReadRequest(0, 1);
        auto readFuture = vchunk->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            readReq,
            TestTraceId());
        readFuture.Wait();

        UNIT_ASSERT_C(
            !HasError(readFuture.Get().Error),
            readFuture.Get().Error.GetMessage());
        if (auto guard = readReq->Sglist.Acquire()) {
            TString buf(BlockSize, 0);
            SgListCopy(guard.Get(), TBlockDataRef(buf.data(), buf.size()));
            UNIT_ASSERT_VALUES_EQUAL(data, buf);
        }
    }

    Y_UNIT_TEST(ReadUnwrittenBlockReturnsZeros)
    {
        auto dbg = CreateInMemoryDbg();
        auto vchunk = std::make_shared<TVChunk>(VChunkIndex, dbg, 10);
        vchunk->Start();
        Sleep(TDuration::MilliSeconds(50));

        auto readReq = MakeReadRequest(0, 1);
        auto readFuture = vchunk->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            readReq,
            TestTraceId());
        readFuture.Wait();

        UNIT_ASSERT_C(
            !HasError(readFuture.Get().Error),
            readFuture.Get().Error.GetMessage());
        if (auto guard = readReq->Sglist.Acquire()) {
            TString buf(BlockSize, 0);
            SgListCopy(guard.Get(), TBlockDataRef(buf.data(), buf.size()));
            for (size_t i = 0; i < BlockSize; i++) {
                UNIT_ASSERT_VALUES_EQUAL(0, (unsigned char)buf[i]);
            }
        }
    }

    Y_UNIT_TEST(ReadWrittenBlockReturnsData)
    {
        auto dbg = CreateInMemoryDbg();
        auto vchunk = std::make_shared<TVChunk>(VChunkIndex, dbg, 100);
        vchunk->Start();
        Sleep(TDuration::MilliSeconds(50));

        TString data(BlockSize, 'A');
        auto writeReq = MakeWriteRequest(0, 1, data);
        auto writeFuture = vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeReq),
            TestTraceId());
        writeFuture.Wait();
        UNIT_ASSERT_C(
            !HasError(writeFuture.Get().Error),
            writeFuture.Get().Error.GetMessage());

        auto readReq = MakeReadRequest(0, 1);
        auto readFuture = vchunk->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            readReq,
            TestTraceId());
        readFuture.Wait();
        UNIT_ASSERT_C(
            !HasError(readFuture.Get().Error),
            readFuture.Get().Error.GetMessage());
        if (auto guard = readReq->Sglist.Acquire()) {
            TString buf(BlockSize, 0);
            SgListCopy(guard.Get(), TBlockDataRef(buf.data(), buf.size()));
            UNIT_ASSERT_VALUES_EQUAL(data, buf);
        }
    }

    Y_UNIT_TEST(SyncQueueBatchWriteThreeBlocksReadBack)
    {
        auto dbg = CreateInMemoryDbg();
        auto vchunk = std::make_shared<TVChunk>(VChunkIndex, dbg, 3);
        vchunk->Start();
        Sleep(TDuration::MilliSeconds(50));

        TString d0(BlockSize, '0');
        TString d1(BlockSize, '1');
        TString d2(BlockSize, '2');

        auto w0 = vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeWriteRequest(0, 1, d0),
            TestTraceId());
        auto w1 = vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeWriteRequest(1, 1, d1),
            TestTraceId());
        auto w2 = vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeWriteRequest(2, 1, d2),
            TestTraceId());

        w0.Wait();
        w1.Wait();
        w2.Wait();
        UNIT_ASSERT_C(!HasError(w0.Get().Error), w0.Get().Error.GetMessage());
        UNIT_ASSERT_C(!HasError(w1.Get().Error), w1.Get().Error.GetMessage());
        UNIT_ASSERT_C(!HasError(w2.Get().Error), w2.Get().Error.GetMessage());

        Sleep(TDuration::MilliSeconds(100));

        auto r0 = MakeReadRequest(0, 1);
        auto r1 = MakeReadRequest(1, 1);
        auto r2 = MakeReadRequest(2, 1);
        auto f0 = vchunk->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            r0,
            TestTraceId());
        auto f1 = vchunk->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            r1,
            TestTraceId());
        auto f2 = vchunk->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            r2,
            TestTraceId());

        f0.Wait();
        f1.Wait();
        f2.Wait();
        UNIT_ASSERT_C(!HasError(f0.Get().Error), f0.Get().Error.GetMessage());
        UNIT_ASSERT_C(!HasError(f1.Get().Error), f1.Get().Error.GetMessage());
        UNIT_ASSERT_C(!HasError(f2.Get().Error), f2.Get().Error.GetMessage());

        auto readBack = [](const TGuardedSgList& sglist) {
            TString buf(BlockSize, 0);
            if (auto guard = sglist.Acquire()) {
                SgListCopy(guard.Get(), TBlockDataRef(buf.data(), buf.size()));
            }
            return buf;
        };
        UNIT_ASSERT_VALUES_EQUAL(d0, readBack(r0->Sglist));
        UNIT_ASSERT_VALUES_EQUAL(d1, readBack(r1->Sglist));
        UNIT_ASSERT_VALUES_EQUAL(d2, readBack(r2->Sglist));
    }
}
