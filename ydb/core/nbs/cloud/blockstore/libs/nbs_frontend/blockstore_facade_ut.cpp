#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/frontend_runtime.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/frontend_state.h>
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
        UNIT_ASSERT(!HasError(Runtime.RegisterVolume(
            Config,
            Storage,
            MakeTestIoConfig(Config, stripeBytes))));
        Runtime.Start();
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

    TNbsFrontendRuntime Runtime{TLog{}};
    NNbs1CompatApi::NBlockStore::IBlockStorePtr BlockStore =
        Runtime.GetBlockStore();
    std::shared_ptr<TTestStorage> Storage = std::make_shared<TTestStorage>();
    TCallContextPtr Context = MakeIntrusive<TCallContext>(ui64{42});
    NKikimrBlockStore::TVolumeConfig Config;
    TString SessionId;
    THashMap<ui64, TString> Blocks;
    TVector<TRequestHeaders> Headers;
    TVector<TCallContextPtr> Contexts;
};

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNbsFrontendBlockStoreTest)
{
    Y_UNIT_TEST(ShouldRejectEveryMethodOutsideAcceptingState)
    {
        auto blockStore = CreateNbsFrontendBlockStore(
            std::make_shared<TFrontendState>(),
            TLog{});

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
        auto blockStore = CreateNbsFrontendBlockStore(
            std::make_shared<TFrontendState>(),
            TLog{});

#define TEST_METHOD(name, ...)                                                 \
    {                                                                          \
        auto response =                                                        \
            blockStore                                                         \
                ->name(                                                        \
                    MakeIntrusive<TCallContext>(),                             \
                    std::make_shared<NYdb::NBS::NNbs1CompatApi::NBlockStore::  \
                                         NProto::T##name##Request>())          \
                .GetValueSync();                                               \
        if (TStringBuf(#name) == "Ping") {                                     \
            UNIT_ASSERT(!HasError(response));                                  \
        } else {                                                               \
            UNIT_ASSERT_VALUES_EQUAL(                                          \
                response.GetError().GetCode(),                                 \
                E_NOT_FOUND);                                                  \
        }                                                                      \
    }

        blockStore->Start();

        // The first Start() must open the admission gate and expose the
        // implemented-method behavior.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

        blockStore->Start();

        // A repeated Start() must preserve the same open state.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

#undef TEST_METHOD

        // Classic requests own protobuf data; no separate allocation API.
        UNIT_ASSERT(!blockStore->AllocateBuffer(DefaultBlockSize));
    }

    Y_UNIT_TEST(ShouldSharePublishedMetadataWithRuntime)
    {
        TNbsFrontendRuntime runtime(TLog{});
        auto blockStore = runtime.GetBlockStore();
        auto request = std::make_shared<
            NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest>();
        request->SetDiskId(TestDiskId);
        request->MutableHeaders()->SetClientId(TestClientId);
        auto mount = [&]
        {
            return blockStore
                ->MountVolume(MakeIntrusive<TCallContext>(), request)
                .GetValueSync();
        };

        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_REJECTED);
        runtime.Start();
        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_NOT_FOUND);
        const auto config = MakeTestVolumeConfig();
        const auto registration = RegisterTestVolume(runtime, config);
        UNIT_ASSERT(!HasError(registration));

        const auto response = mount();
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_VALUES_EQUAL(response.GetVolume().GetDiskId(), TestDiskId);
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetVolume().GetBlocksCount(),
            TestBlocksCount);
        UNIT_ASSERT(!response.GetSessionId().empty());
        UNIT_ASSERT_VALUES_EQUAL(response.GetInactiveClientsTimeout(), 0);

        runtime.Stop();
        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_REJECTED);
        runtime.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            mount().GetVolume().GetBlocksCount(),
            TestBlocksCount);
        runtime.UnregisterVolume(registration.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldValidateBothIoMethodsAndRevokeSessionThroughFacade)
    {
        TNbsFrontendRuntime runtime(TLog{});
        auto blockStore = runtime.GetBlockStore();
        const auto config = MakeTestVolumeConfig();
        UNIT_ASSERT(!HasError(RegisterTestVolume(runtime, config)));
        runtime.Start();
        auto mount = std::make_shared<
            NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest>();
        mount->SetDiskId(TestDiskId);
        mount->MutableHeaders()->SetClientId(TestClientId);
        const auto mounted =
            blockStore->MountVolume(MakeIntrusive<TCallContext>(), mount)
                .GetValueSync();
        UNIT_ASSERT(!HasError(mounted));

        auto checkIo = [&](const TString& clientId,
                           const TString& sessionId,
                           ui32 expected)
        {
#define CHECK_IO(name)                                                         \
    {                                                                          \
        auto request = std::make_shared<                                       \
            NNbs1CompatApi::NBlockStore::NProto::T##name##Request>();          \
        request->SetDiskId(TestDiskId);                                        \
        request->MutableHeaders()->SetClientId(clientId);                      \
        request->SetSessionId(sessionId);                                      \
        const auto response =                                                  \
            blockStore->name(MakeIntrusive<TCallContext>(), request)           \
                .GetValueSync();                                               \
        UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), expected);     \
    }
            CHECK_IO(ReadBlocks)
            CHECK_IO(WriteBlocks)
#undef CHECK_IO
        };
        checkIo(TestClientId, "", E_BS_INVALID_SESSION);
        checkIo("client2", mounted.GetSessionId(), E_BS_INVALID_SESSION);
        // An otherwise valid session reaches payload validation for empty I/O.
        checkIo(TestClientId, mounted.GetSessionId(), E_ARGUMENT);
        auto unmount = std::make_shared<
            NNbs1CompatApi::NBlockStore::NProto::TUnmountVolumeRequest>();
        unmount->SetDiskId(TestDiskId);
        unmount->MutableHeaders()->SetClientId(TestClientId);
        unmount->SetSessionId(mounted.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(
            blockStore->UnmountVolume(MakeIntrusive<TCallContext>(), unmount)
                .GetValueSync()
                .GetError()
                .GetCode(),
            S_OK);
        checkIo(TestClientId, mounted.GetSessionId(), E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            blockStore->UnmountVolume(MakeIntrusive<TCallContext>(), unmount)
                .GetValueSync()
                .GetError()
                .GetCode(),
            S_ALREADY);
        runtime.Stop();
        checkIo(TestClientId, mounted.GetSessionId(), E_REJECTED);
    }

    Y_UNIT_TEST(ShouldReadWriteNativeBlockSizesAndAdaptHeaders)
    {
        for (ui32 blockSize = DefaultBlockSize; blockSize <= MaxBlockSize;
             blockSize *= 2)
        {
            TIoTestEnv env(blockSize);
            auto write = env.WriteRequest(1, 2);
            write->SetBlockSize(blockSize);
            write->MutableHeaders()->SetTimestamp(1);
            UNIT_ASSERT(
                !HasError(env.BlockStore->WriteBlocks(env.Context, write)
                              .GetValueSync()));
            auto read = env.ReadRequest(1, 2);
            read->MutableHeaders()->SetOptimizeNetworkTransfer(
                NCompatProto::SKIP_VOID_BLOCKS);
            const auto response =
                env.BlockStore->ReadBlocks(env.Context, read).GetValueSync();
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_VALUES_EQUAL(response.GetBlocks().BuffersSize(), 2);
            for (const auto& buffer: response.GetBlocks().GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(buffer, TString(blockSize, 'x'));
            }
            UNIT_ASSERT(!response.HasChecksum());
            UNIT_ASSERT(!response.GetAllZeroes());
            UNIT_ASSERT_VALUES_EQUAL(env.Headers.size(), 2);
            for (size_t i = 0; i != env.Headers.size(); ++i) {
                const auto& headers = env.Headers[i];
                UNIT_ASSERT_VALUES_EQUAL(headers.ClientId, TestClientId);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.RequestId,
                    env.Context->RequestId);
                UNIT_ASSERT(headers.Timestamp > TInstant::MicroSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(headers.Range.Start, 1);
                UNIT_ASSERT_VALUES_EQUAL(headers.Range.Size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->DiskId,
                    TestDiskId);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->BlockSize,
                    blockSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->BlockCount,
                    TestBlocksCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->BlocksPerStripe,
                    TestStripeBytes / blockSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    headers.VolumeConfig->VChunkSize,
                    TestVChunkSize);
                UNIT_ASSERT(env.Contexts[i] == env.Context);
            }
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
        for (ui32 i = 0; i != 12; ++i) {
            auto read = env.ReadRequest();
            switch (i) {
                case 0:
                    read->SetBlockSize(env.Config.GetBlockSize() * 2);
                    break;
                case 1:
                    read->SetBlocksCount(0);
                    break;
                case 2:
                    read->SetBlocksCount(32_MB / DefaultBlockSize + 1);
                    break;
                case 3:
                    read->SetStartIndex(TestBlocksCount);
                    break;
                case 4:
                    read->SetStartIndex(TestBlocksCount - 1);
                    read->SetBlocksCount(2);
                    break;
                case 5:
                    read->SetStartIndex(std::numeric_limits<ui64>::max());
                    break;
                case 6:
                    read->SetFlags(1);
                    break;
                case 7:
                    read->SetCheckpointId("checkpoint");
                    break;
                case 8:
                    read->MutableHeaders()->SetReplicaIndex(1);
                    break;
                case 9:
                    read->MutableHeaders()->SetReplicaCount(1);
                    break;
                case 10:
                    read->ClearSessionId();
                    break;
                case 11:
                    read->SetDiskId("other");
                    break;
            }
            const ui32 expected = i < 6     ? E_ARGUMENT
                                  : i < 10  ? E_NOT_IMPLEMENTED
                                  : i == 10 ? E_BS_INVALID_SESSION
                                            : E_NOT_FOUND;
            const auto response =
                env.BlockStore->ReadBlocks(env.Context, read).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                response.GetError().GetCode(),
                expected,
                i);
            UNIT_ASSERT(!response.HasBlocks());
        }
        for (ui32 i = 0; i != 15; ++i) {
            auto write = env.WriteRequest();
            switch (i) {
                case 0:
                    write->SetBlockSize(env.Config.GetBlockSize() * 2);
                    break;
                case 1:
                    write->ClearBlocks();
                    break;
                case 2:
                    write->MutableBlocks()->AddBuffers("");
                    break;
                case 3:
                    write->MutableBlocks()->MutableBuffers(0)->resize(
                        env.Config.GetBlockSize() / 2);
                    break;
                case 4:
                    write->MutableBlocks()->MutableBuffers(0)->resize(
                        32_MB + 1);
                    break;
                case 5:
                    write->SetStartIndex(TestBlocksCount);
                    break;
                case 6:
                    write->SetStartIndex(TestBlocksCount - 1);
                    write->MutableBlocks()->AddBuffers(
                        TString(DefaultBlockSize, 'x'));
                    break;
                case 7:
                    write->SetStartIndex(std::numeric_limits<ui64>::max());
                    break;
                case 8:
                    write->SetFlags(1);
                    break;
                case 9:
                    write->AddChecksums();
                    break;
                case 10:
                    write->MutableHeaders()->SetReplicaIndex(1);
                    break;
                case 11:
                    write->MutableHeaders()->SetReplicaCount(1);
                    break;
                case 12:
                    write->ClearSessionId();
                    break;
                case 13:
                    write->MutableHeaders()->SetClientId("other");
                    break;
                case 14:
                    write->SetDiskId("other");
                    break;
            }
            const ui32 expected = i < 8    ? E_ARGUMENT
                                  : i < 12 ? E_NOT_IMPLEMENTED
                                  : i < 14 ? E_BS_INVALID_SESSION
                                           : E_NOT_FOUND;
            UNIT_ASSERT_VALUES_EQUAL_C(
                env.BlockStore->WriteBlocks(env.Context, write)
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                expected,
                i);
        }
        UNIT_ASSERT(env.Headers.empty());
        UNIT_ASSERT(env.Blocks.empty());
    }

    Y_UNIT_TEST(ShouldRejectByteOverflowAndPreserveValidationPriority)
    {
        TIoTestEnv env(DefaultBlockSize, std::numeric_limits<ui64>::max());
        for (ui64 start:
             {std::numeric_limits<ui64>::max() / DefaultBlockSize,
              std::numeric_limits<ui64>::max() / DefaultBlockSize + 1})
        {
            UNIT_ASSERT_VALUES_EQUAL(
                env.BlockStore->ReadBlocks(env.Context, env.ReadRequest(start))
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_ARGUMENT);
            UNIT_ASSERT_VALUES_EQUAL(
                env.BlockStore
                    ->WriteBlocks(env.Context, env.WriteRequest(start))
                    .GetValueSync()
                    .GetError()
                    .GetCode(),
                E_ARGUMENT);
        }
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
        env.Runtime.Stop();
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

    Y_UNIT_TEST(ShouldKeepReadBuffersAndClosePendingSplitRequestsOnError)
    {
        for (const bool fail: {false, true}) {
            TIoTestEnv env;
            TVector<NThreading::TPromise<TReadBlocksLocalResponse>> completions;
            TVector<TGuardedSgList> sglists;
            env.Storage->ReadBlocksLocalHandler =
                [&completions, &sglists](TCallContextPtr context, auto request)
            {
                Y_UNUSED(context);
                sglists.push_back(request->Sglist);
                completions.push_back(
                    NThreading::NewPromise<TReadBlocksLocalResponse>());
                return completions.back().GetFuture();
            };
            const auto response = env.BlockStore->ReadBlocks(
                env.Context,
                env.ReadRequest(
                    TestStripeBytes / env.Config.GetBlockSize() - 1,
                    2));
            UNIT_ASSERT_VALUES_EQUAL(completions.size(), 2);
            UNIT_ASSERT(!response.HasValue());
            for (const auto& sglist: sglists) {
                const auto guard = sglist.Acquire();
                UNIT_ASSERT(guard);
                const TString data(DefaultBlockSize, 'r');
                UNIT_ASSERT_VALUES_EQUAL(
                    SgListCopy(
                        TBlockDataRef(data.data(), data.size()),
                        guard.Get()),
                    DefaultBlockSize);
            }
            env.Runtime.Stop();
            if (fail) {
                completions[0].SetValue(
                    {MakeError(E_IO, "first stripe failed")});
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetValueSync().GetError().GetCode(),
                    E_IO);
                UNIT_ASSERT(!response.GetValueSync().HasBlocks());
                // The other backend future is pending, but its frontend memory
                // is closed.
                UNIT_ASSERT(!sglists[1].Acquire());
                completions[1].SetValue({});
            } else {
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
            }
            for (const auto& sglist: sglists) {
                UNIT_ASSERT(!sglist.Acquire());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

}   // namespace NYdb::NBS::NBlockStore
