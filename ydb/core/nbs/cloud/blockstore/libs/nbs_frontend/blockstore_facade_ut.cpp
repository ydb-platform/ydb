#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/frontend_test.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

#include <limits>
#include <thread>

namespace NYdb::NBS::NBlockStore {

namespace {

using namespace NTests;
namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;

// Real frontend/handlers backed by controllable in-memory native storage.
class TIoTestEnv final
{
public:
    // Registers a partition and mounts one client, with native stripe geometry.
    explicit TIoTestEnv(
        ui32 blockSize = DefaultBlockSize,
        ui64 blocksCount = TestBlocksCount,
        ui64 stripeBytes = TestStripeBytes)
    {
        Config = MakeTestVolumeConfig(blockSize, blocksCount);
        Storage->WriteBlocksLocalHandler =
            [this](TCallContextPtr context, auto request)
        {
            Headers.push_back(request->Headers);
            Contexts.push_back(std::move(context));
            const auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& range = request->Headers.Range;
            const ui32 blockSize = Config.GetBlockSize();
            TString data(range.Size() * blockSize, '\0');
            UNIT_ASSERT_VALUES_EQUAL(
                SgListCopy(
                    guard.Get(),
                    TBlockDataRef(data.data(), data.size())),
                data.size());
            for (ui64 i = 0; i != range.Size(); ++i) {
                Blocks[range.Start + i] = data.substr(i * blockSize, blockSize);
            }
            return NThreading::MakeFuture<TWriteBlocksLocalResponse>();
        };
        Storage->ReadBlocksLocalHandler =
            [this](TCallContextPtr context, auto request)
        {
            Headers.push_back(request->Headers);
            Contexts.push_back(std::move(context));
            const auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& range = request->Headers.Range;
            const ui32 blockSize = Config.GetBlockSize();
            TString data(range.Size() * blockSize, '\0');
            for (ui64 i = 0; i != range.Size(); ++i) {
                if (const auto it = Blocks.find(range.Start + i);
                    it != Blocks.end())
                {
                    data.replace(i * blockSize, blockSize, it->second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(
                SgListCopy(
                    TBlockDataRef(data.data(), data.size()),
                    guard.Get()),
                data.size());
            return NThreading::MakeFuture<TReadBlocksLocalResponse>();
        };
        UNIT_ASSERT(!HasError(FrontendEnv.RegisterVolume(
            Config,
            Storage,
            MakeTestIoConfig(Config, stripeBytes))));
        FrontendEnv.Facade->Start();
        auto mount = std::make_shared<NCompatProto::TMountVolumeRequest>();
        mount->SetDiskId(TestDiskId);
        mount->MutableHeaders()->SetClientId(TestClientId);
        const auto response =
            BlockStore->MountVolume(Context, mount).GetValueSync();
        UNIT_ASSERT(!HasError(response));
        SessionId = response.GetSessionId();
    }

    // Forms a valid read; individual tests change just the field under test.
    std::shared_ptr<NCompatProto::TReadBlocksRequest> ReadRequest(
        ui64 start = 0,
        ui32 count = 1) const
    {
        auto request = std::make_shared<NCompatProto::TReadBlocksRequest>();
        request->SetDiskId(TestDiskId);
        request->SetSessionId(SessionId);
        request->SetStartIndex(start);
        request->SetBlocksCount(count);
        request->MutableHeaders()->SetClientId(TestClientId);
        request->MutableHeaders()->SetRequestId(Context->RequestId);
        return request;
    }

    // Uses a multi-block protobuf buffer, not one buffer per logical block.
    std::shared_ptr<NCompatProto::TWriteBlocksRequest> WriteRequest(
        ui64 start = 0,
        ui32 count = 1) const
    {
        auto request = std::make_shared<NCompatProto::TWriteBlocksRequest>();
        request->SetDiskId(TestDiskId);
        request->SetSessionId(SessionId);
        request->SetStartIndex(start);
        request->MutableHeaders()->SetClientId(TestClientId);
        request->MutableHeaders()->SetRequestId(Context->RequestId);
        request->MutableBlocks()->AddBuffers(
            TString(ui64(count) * Config.GetBlockSize(), 'x'));
        return request;
    }

    // Revokes admission for subsequent I/O, without draining admitted requests.
    void Unmount()
    {
        auto request = std::make_shared<NCompatProto::TUnmountVolumeRequest>();
        request->SetDiskId(TestDiskId);
        request->SetSessionId(SessionId);
        request->MutableHeaders()->SetClientId(TestClientId);
        UNIT_ASSERT(!HasError(
            BlockStore->UnmountVolume(Context, request).GetValueSync()));
    }

    TFrontendTestEnv FrontendEnv;
    NNbs1CompatApi::NBlockStore::IBlockStorePtr BlockStore = FrontendEnv.Facade;
    std::shared_ptr<TTestStorage> Storage = std::make_shared<TTestStorage>();
    TCallContextPtr Context = MakeIntrusive<TCallContext>(ui64{42});
    NKikimrBlockStore::TVolumeConfig Config;
    TString SessionId;
    THashMap<ui64, TString> Blocks;
    TVector<TRequestHeaders> Headers;
    TVector<TCallContextPtr> Contexts;
};

// Keeps both parts of a read pending so tests can control buffer lifetime.
NThreading::TFuture<NCompatProto::TReadBlocksResponse> StartPendingSplitRead(
    TIoTestEnv& env,
    TVector<NThreading::TPromise<TReadBlocksLocalResponse>>* completions,
    TVector<TGuardedSgList>* sglists)
{
    env.Storage->ReadBlocksLocalHandler =
        [completions, sglists](TCallContextPtr context, auto request)
    {
        Y_UNUSED(context);
        sglists->push_back(request->Sglist);
        completions->push_back(
            NThreading::NewPromise<TReadBlocksLocalResponse>());
        return completions->back().GetFuture();
    };
    const auto response = env.BlockStore->ReadBlocks(
        env.Context,
        env.ReadRequest(TestStripeBytes / env.Config.GetBlockSize() - 1, 2));
    UNIT_ASSERT_VALUES_EQUAL(completions->size(), 2);
    UNIT_ASSERT(!response.HasValue());
    for (const auto& sglist: *sglists) {
        const auto guard = sglist.Acquire();
        UNIT_ASSERT(guard);
        const TString data(DefaultBlockSize, 'r');
        UNIT_ASSERT_VALUES_EQUAL(
            SgListCopy(TBlockDataRef(data.data(), data.size()), guard.Get()),
            DefaultBlockSize);
    }
    return response;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNbsBlockStoreFacadeTest)
{
    Y_UNIT_TEST(ShouldRejectEveryMethodOutsideAcceptingState)
    {
        auto blockStore = CreateNbsBlockStoreFacade(TLog{});

        // A newly created facade must keep every method behind the closed
        // admission gate until Start() is called.
#define TEST_METHOD(name, ...)                                                 \
    {                                                                          \
        auto response =                                                        \
            blockStore                                                         \
                ->name(                                                        \
                    MakeIntrusive<TCallContext>(),                             \
                    std::make_shared<NYdb::NBS::NNbs1CompatApi::NBlockStore::  \
                                         NProto::T##name##Request>())          \
                .GetValueSync();                                               \
        UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), E_REJECTED);   \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            response.GetError().GetMessage(),                                  \
            "NBS2 frontend is not accepting requests");                        \
    }

        NBS1_COMPAT_SERVICE(TEST_METHOD)

        // Enter the accepting state so that the first Stop() performs an
        // observable open-to-closed transition.
        blockStore->Start();
        blockStore->Stop();

        // The first Stop() must close the admission gate for every method.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

        blockStore->Stop();

        // A repeated Stop() must preserve the same closed state.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

#undef TEST_METHOD
    }

    Y_UNIT_TEST(ShouldServePingAndRejectDiskRequestsWithoutRegistration)
    {
        auto blockStore = CreateNbsBlockStoreFacade(TLog{});

        const auto checkStarted = [&]
        {
            UNIT_ASSERT(!HasError(
                blockStore
                    ->Ping(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NCompatProto::TPingRequest>())
                    .GetValueSync()));
            UNIT_ASSERT_VALUES_EQUAL(
                blockStore
                    ->MountVolume(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NCompatProto::TMountVolumeRequest>())
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_NOT_FOUND);
            UNIT_ASSERT_VALUES_EQUAL(
                blockStore
                    ->UnmountVolume(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NCompatProto::TUnmountVolumeRequest>())
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_NOT_FOUND);
            UNIT_ASSERT_VALUES_EQUAL(
                blockStore
                    ->ReadBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NCompatProto::TReadBlocksRequest>())
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_NOT_FOUND);
            UNIT_ASSERT_VALUES_EQUAL(
                blockStore
                    ->WriteBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NCompatProto::TWriteBlocksRequest>())
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_NOT_FOUND);
        };

        blockStore->Start();

        // The first Start() must open the admission gate and expose the
        // implemented-method behavior.
        checkStarted();

        blockStore->Start();

        // A repeated Start() must preserve the same open state.
        checkStarted();

        // Classic requests own protobuf data; no separate allocation API.
        UNIT_ASSERT(!blockStore->AllocateBuffer(DefaultBlockSize));
    }

    Y_UNIT_TEST(ShouldRejectBothIoMethodsAfterUnmount)
    {
        TIoTestEnv env;
        env.Unmount();
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->ReadBlocks(env.Context, env.ReadRequest())
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->WriteBlocks(env.Context, env.WriteRequest())
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(env.Headers.empty());
        UNIT_ASSERT(env.Blocks.empty());
    }

    Y_UNIT_TEST(ShouldReadWriteNativeBlockSizes)
    {
        for (ui32 blockSize = DefaultBlockSize; blockSize <= MaxBlockSize;
             blockSize *= 2)
        {
            TIoTestEnv env(blockSize);
            auto write = env.WriteRequest(1, 2);
            write->SetBlockSize(blockSize);
            UNIT_ASSERT(
                !HasError(env.BlockStore->WriteBlocks(env.Context, write)
                              .GetValueSync()));
            const auto response =
                env.BlockStore->ReadBlocks(env.Context, env.ReadRequest(1, 2))
                    .GetValueSync();
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_VALUES_EQUAL(response.GetBlocks().BuffersSize(), 2);
            for (const auto& buffer: response.GetBlocks().GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(buffer, TString(blockSize, 'x'));
            }
            UNIT_ASSERT_VALUES_EQUAL(env.Headers.size(), 2);
            for (const auto& headers: env.Headers) {
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->BlockSize,
                    blockSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->BlocksPerStripe,
                    TestStripeBytes / blockSize);
            }
        }
    }

    Y_UNIT_TEST(ShouldAdaptIoHeaders)
    {
        TIoTestEnv env;
        auto write = env.WriteRequest(1, 2);
        write->MutableHeaders()->SetTimestamp(1);
        UNIT_ASSERT(!HasError(
            env.BlockStore->WriteBlocks(env.Context, write).GetValueSync()));
        auto read = env.ReadRequest(1, 2);
        read->MutableHeaders()->SetOptimizeNetworkTransfer(
            NCompatProto::SKIP_VOID_BLOCKS);
        const auto response =
            env.BlockStore->ReadBlocks(env.Context, read).GetValueSync();
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT(!response.HasChecksum());
        UNIT_ASSERT(!response.GetAllZeroes());
        UNIT_ASSERT_VALUES_EQUAL(env.Headers.size(), 2);
        for (const auto& headers: env.Headers) {
            UNIT_ASSERT_VALUES_EQUAL(headers.ClientId, TestClientId);
            UNIT_ASSERT_VALUES_EQUAL(headers.RequestId, env.Context->RequestId);
            UNIT_ASSERT(headers.Timestamp > TInstant::MicroSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(headers.Range.Start, 1);
            UNIT_ASSERT_VALUES_EQUAL(headers.Range.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(headers.VolumeConfig->DiskId, TestDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                headers.VolumeConfig->BlockCount,
                TestBlocksCount);
            UNIT_ASSERT_VALUES_EQUAL(
                headers.VolumeConfig->VChunkSize,
                TestVChunkSize);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Contexts.size(), 2);
        for (const auto& context: env.Contexts) {
            UNIT_ASSERT(context == env.Context);
        }
    }

    Y_UNIT_TEST(ShouldHandleFragmentedWriteAndDiskBoundary)
    {
        TIoTestEnv env;
        const ui64 start = env.Config.GetPartitions(0).GetBlockCount() - 1;
        auto write = env.WriteRequest(start);
        write->MutableBlocks()->ClearBuffers();
        write->MutableBlocks()->AddBuffers(
            TString(env.Config.GetBlockSize() / 2, 'a'));
        write->MutableBlocks()->AddBuffers(
            TString(env.Config.GetBlockSize() / 2, 'b'));
        UNIT_ASSERT(!HasError(
            env.BlockStore->WriteBlocks(env.Context, write).GetValueSync()));
        const auto response =
            env.BlockStore->ReadBlocks(env.Context, env.ReadRequest(start))
                .GetValueSync();
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetBlocks().GetBuffers(0),
            TString(env.Config.GetBlockSize() / 2, 'a') +
                TString(env.Config.GetBlockSize() / 2, 'b'));
    }

    Y_UNIT_TEST(ShouldSplitAtStripeAndMaximumSubrequestSize)
    {
        // A large stripe isolates the handler's 4 MiB splitting from stripe
        // splitting.
        for (const ui64 stripeBytes: {ui64(TestStripeBytes), ui64(64_MB)}) {
            TIoTestEnv env(DefaultBlockSize, TestBlocksCount, stripeBytes);
            constexpr ui32 count = 32_MB / DefaultBlockSize;
            const ui64 start = TestStripeBytes / env.Config.GetBlockSize() - 1;
            UNIT_ASSERT(!HasError(
                env.BlockStore
                    ->WriteBlocks(env.Context, env.WriteRequest(start, count))
                    .GetValueSync()));
            const auto response =
                env.BlockStore
                    ->ReadBlocks(env.Context, env.ReadRequest(start, count))
                    .GetValueSync();
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_VALUES_EQUAL(response.GetBlocks().BuffersSize(), count);
            for (const auto& data: response.GetBlocks().GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(data, TString(DefaultBlockSize, 'x'));
            }
            ui64 totalBlocks = 0;
            for (const auto& headers: env.Headers) {
                UNIT_ASSERT(
                    headers.Range.Size() <=
                    MaxSubRequestSize / env.Config.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.Range.Start / (stripeBytes / DefaultBlockSize),
                    headers.Range.End / (stripeBytes / DefaultBlockSize));
                totalBlocks += headers.Range.Size();
            }
            UNIT_ASSERT(env.Headers.size() > 2);
            UNIT_ASSERT_VALUES_EQUAL(totalBlocks, 2 * count);
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidIoBeforeBackend)
    {
        TIoTestEnv env;
        const auto checkRead =
            [&](TStringBuf name,
                const std::shared_ptr<NCompatProto::TReadBlocksRequest>&
                    request,
                ui32 expected)
        {
            const auto response =
                env.BlockStore->ReadBlocks(env.Context, request).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                response.GetError().GetCode(),
                expected,
                name);
            UNIT_ASSERT_C(!response.HasBlocks(), name);
            UNIT_ASSERT_C(env.Headers.empty(), name);
            UNIT_ASSERT_C(env.Blocks.empty(), name);
        };
        const auto checkWrite =
            [&](TStringBuf name,
                const std::shared_ptr<NCompatProto::TWriteBlocksRequest>&
                    request,
                ui32 expected)
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                env.BlockStore->WriteBlocks(env.Context, request)
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                expected,
                name);
            UNIT_ASSERT_C(env.Headers.empty(), name);
            UNIT_ASSERT_C(env.Blocks.empty(), name);
        };

        auto read = env.ReadRequest();
        read->SetBlockSize(env.Config.GetBlockSize() * 2);
        checkRead("block size mismatch", read, E_ARGUMENT);
        checkRead("empty range", env.ReadRequest(0, 0), E_ARGUMENT);
        checkRead(
            "payload exceeds 32 MiB",
            env.ReadRequest(0, 32_MB / DefaultBlockSize + 1),
            E_ARGUMENT);
        checkRead(
            "range starts outside disk",
            env.ReadRequest(TestBlocksCount),
            E_ARGUMENT);
        checkRead(
            "range ends outside disk",
            env.ReadRequest(TestBlocksCount - 1, 2),
            E_ARGUMENT);
        checkRead(
            "maximum start index",
            env.ReadRequest(std::numeric_limits<ui64>::max()),
            E_ARGUMENT);

        read = env.ReadRequest();
        read->SetFlags(1);
        checkRead("unsupported flags", read, E_NOT_IMPLEMENTED);
        read = env.ReadRequest();
        read->SetCheckpointId("checkpoint");
        checkRead("unsupported checkpoint", read, E_NOT_IMPLEMENTED);
        read = env.ReadRequest();
        read->MutableHeaders()->SetReplicaIndex(1);
        checkRead("unsupported replica index", read, E_NOT_IMPLEMENTED);
        read = env.ReadRequest();
        read->MutableHeaders()->SetReplicaCount(1);
        checkRead("unsupported replica count", read, E_NOT_IMPLEMENTED);

        read = env.ReadRequest();
        read->ClearSessionId();
        checkRead("missing session", read, E_BS_INVALID_SESSION);
        read = env.ReadRequest();
        read->MutableHeaders()->SetClientId("other");
        checkRead("wrong client", read, E_BS_INVALID_SESSION);
        read = env.ReadRequest();
        read->SetDiskId("other");
        checkRead("unknown disk", read, E_NOT_FOUND);

        auto write = env.WriteRequest();
        write->SetBlockSize(env.Config.GetBlockSize() * 2);
        checkWrite("block size mismatch", write, E_ARGUMENT);
        write = env.WriteRequest();
        write->ClearBlocks();
        checkWrite("missing payload", write, E_ARGUMENT);
        write = env.WriteRequest();
        write->MutableBlocks()->AddBuffers("");
        checkWrite("empty buffer", write, E_ARGUMENT);
        write = env.WriteRequest();
        write->MutableBlocks()->MutableBuffers(0)->resize(
            env.Config.GetBlockSize() / 2);
        checkWrite("incomplete block", write, E_ARGUMENT);
        write = env.WriteRequest();
        write->MutableBlocks()->MutableBuffers(0)->resize(32_MB + 1);
        checkWrite("payload exceeds 32 MiB", write, E_ARGUMENT);
        checkWrite(
            "range starts outside disk",
            env.WriteRequest(TestBlocksCount),
            E_ARGUMENT);
        write = env.WriteRequest(TestBlocksCount - 1);
        write->MutableBlocks()->AddBuffers(TString(DefaultBlockSize, 'x'));
        checkWrite("range ends outside disk", write, E_ARGUMENT);
        checkWrite(
            "maximum start index",
            env.WriteRequest(std::numeric_limits<ui64>::max()),
            E_ARGUMENT);

        write = env.WriteRequest();
        write->SetFlags(1);
        checkWrite("unsupported flags", write, E_NOT_IMPLEMENTED);
        write = env.WriteRequest();
        write->AddChecksums();
        checkWrite("unsupported checksums", write, E_NOT_IMPLEMENTED);
        write = env.WriteRequest();
        write->MutableHeaders()->SetReplicaIndex(1);
        checkWrite("unsupported replica index", write, E_NOT_IMPLEMENTED);
        write = env.WriteRequest();
        write->MutableHeaders()->SetReplicaCount(1);
        checkWrite("unsupported replica count", write, E_NOT_IMPLEMENTED);

        write = env.WriteRequest();
        write->ClearSessionId();
        checkWrite("missing session", write, E_BS_INVALID_SESSION);
        write = env.WriteRequest();
        write->MutableHeaders()->SetClientId("other");
        checkWrite("wrong client", write, E_BS_INVALID_SESSION);
        write = env.WriteRequest();
        write->SetDiskId("other");
        checkWrite("unknown disk", write, E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldRejectByteOverflow)
    {
        TIoTestEnv env(DefaultBlockSize, std::numeric_limits<ui64>::max());
        const auto checkOverflow = [&](TStringBuf name, ui64 start)
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                env.BlockStore->ReadBlocks(env.Context, env.ReadRequest(start))
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_ARGUMENT,
                name);
            UNIT_ASSERT_VALUES_EQUAL_C(
                env.BlockStore
                    ->WriteBlocks(env.Context, env.WriteRequest(start))
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_ARGUMENT,
                name);
            UNIT_ASSERT_C(env.Headers.empty(), name);
        };
        constexpr ui64 maxByteBlocks =
            std::numeric_limits<ui64>::max() / DefaultBlockSize;
        checkOverflow("byte range end overflows", maxByteBlocks);
        checkOverflow("byte range start overflows", maxByteBlocks + 1);
    }

    Y_UNIT_TEST(ShouldPreserveIoValidationPriority)
    {
        TIoTestEnv env;
        auto read = env.ReadRequest(0, 0);
        read->SetFlags(1);
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->ReadBlocks(env.Context, read)
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_NOT_IMPLEMENTED);
        read->ClearSessionId();
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->ReadBlocks(env.Context, read)
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        read->SetDiskId("other");
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->ReadBlocks(env.Context, read)
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_NOT_FOUND);
        env.FrontendEnv.Facade->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->ReadBlocks(env.Context, read)
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_REJECTED);
        UNIT_ASSERT(env.Headers.empty());
    }

    Y_UNIT_TEST(ShouldPreserveBackendErrorsAndCloseBuffers)
    {
        TIoTestEnv env;
        auto error = MakeError(E_IO, "backend failure");
        error.SetFlags(17);
        TGuardedSgList readSglist;
        TGuardedSgList writeSglist;
        env.Storage->ReadBlocksLocalHandler =
            [&error, &readSglist](TCallContextPtr context, auto request)
        {
            Y_UNUSED(context);
            readSglist = request->Sglist;
            return NThreading::MakeFuture(TReadBlocksLocalResponse{error});
        };
        env.Storage->WriteBlocksLocalHandler =
            [&error, &writeSglist](TCallContextPtr context, auto request)
        {
            Y_UNUSED(context);
            writeSglist = request->Sglist;
            return NThreading::MakeFuture(TWriteBlocksLocalResponse{error});
        };
        const auto read =
            env.BlockStore->ReadBlocks(env.Context, env.ReadRequest())
                .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            read.GetError().SerializeAsString(),
            error.SerializeAsString());
        UNIT_ASSERT(!read.HasBlocks());
        UNIT_ASSERT(!readSglist.Acquire());
        const auto write =
            env.BlockStore->WriteBlocks(env.Context, env.WriteRequest())
                .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            write.GetError().SerializeAsString(),
            error.SerializeAsString());
        UNIT_ASSERT(!writeSglist.Acquire());
    }

    Y_UNIT_TEST(ShouldKeepWritePayloadUntilGuardsCloseAfterUnmount)
    {
        TIoTestEnv env;
        auto completion = NThreading::NewPromise<TWriteBlocksLocalResponse>();
        TGuardedSgList sglist;
        env.Storage->WriteBlocksLocalHandler =
            [&completion, &sglist](TCallContextPtr context, auto request)
        {
            Y_UNUSED(context);
            sglist = request->Sglist;
            return completion.GetFuture();
        };
        auto request = env.WriteRequest();
        std::weak_ptr<NCompatProto::TWriteBlocksRequest> owner = request;
        const auto response =
            env.BlockStore->WriteBlocks(env.Context, std::move(request));
        UNIT_ASSERT(!response.HasValue());
        UNIT_ASSERT(!owner.expired());
        env.Unmount();
        UNIT_ASSERT_VALUES_EQUAL(
            env.BlockStore->ReadBlocks(env.Context, env.ReadRequest())
                .GetValueSync()
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);

        std::thread finish;
        {
            const auto guard = sglist.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT(
                guard.Get().front().Data() ==
                owner.lock()->GetBlocks().GetBuffers(0).data());
            auto started = NThreading::NewPromise<void>();
            finish = std::thread(
                [completion, started]() mutable
                {
                    started.SetValue();
                    completion.SetValue({});
                });
            started.GetFuture().GetValueSync();
            UNIT_ASSERT(!response.HasValue());
            UNIT_ASSERT(!owner.expired());
        }
        finish.join();
        UNIT_ASSERT(!HasError(response.GetValueSync()));
        UNIT_ASSERT(!sglist.Acquire());
    }

    Y_UNIT_TEST(ShouldCompleteSplitReadAfterFrontendStop)
    {
        TVector<NThreading::TPromise<TReadBlocksLocalResponse>> completions;
        TVector<TGuardedSgList> sglists;
        TIoTestEnv env;
        const auto response =
            StartPendingSplitRead(env, &completions, &sglists);
        env.FrontendEnv.Facade->Stop();

        completions[0].SetValue({});
        UNIT_ASSERT(!response.HasValue());
        completions[1].SetValue({});
        const auto& result = response.GetValueSync();
        UNIT_ASSERT(!HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(result.GetBlocks().BuffersSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetBlocks().GetBuffers(0),
            TString(DefaultBlockSize, 'r'));
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetBlocks().GetBuffers(1),
            TString(DefaultBlockSize, 'r'));
        for (const auto& sglist: sglists) {
            UNIT_ASSERT(!sglist.Acquire());
        }
    }

    Y_UNIT_TEST(ShouldClosePendingSplitReadBuffersOnError)
    {
        TVector<NThreading::TPromise<TReadBlocksLocalResponse>> completions;
        TVector<TGuardedSgList> sglists;
        TIoTestEnv env;
        const auto response =
            StartPendingSplitRead(env, &completions, &sglists);
        env.FrontendEnv.Facade->Stop();

        completions[0].SetValue({MakeError(E_IO, "first stripe failed")});
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetValueSync().GetError().GetCode(),
            E_IO);
        UNIT_ASSERT(!response.GetValueSync().HasBlocks());
        // The other backend future is pending, but its frontend memory is
        // closed.
        UNIT_ASSERT(!sglists[1].Acquire());
        completions[1].SetValue({});
        for (const auto& sglist: sglists) {
            UNIT_ASSERT(!sglist.Acquire());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

}   // namespace NYdb::NBS::NBlockStore
