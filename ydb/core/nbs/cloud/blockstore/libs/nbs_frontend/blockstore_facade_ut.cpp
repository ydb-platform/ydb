#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/frontend_runtime.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/frontend_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

namespace {

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

        // The frontend skeleton does not provide data-path buffer allocation.
        UNIT_ASSERT(!blockStore->AllocateBuffer(4096));
    }

    Y_UNIT_TEST(ShouldSharePublishedMetadataWithRuntime)
    {
        TNbsFrontendRuntime runtime(TLog{});
        auto blockStore = runtime.GetBlockStore();
        auto request = std::make_shared<
            NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest>();
        request->SetDiskId("disk1");
        request->MutableHeaders()->SetClientId("client1");
        auto mount = [&]
        {
            return blockStore
                ->MountVolume(MakeIntrusive<TCallContext>(), request)
                .GetValueSync();
        };

        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_REJECTED);
        runtime.Start();
        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_NOT_FOUND);
        NKikimrBlockStore::TVolumeConfig config;
        config.SetDiskId("disk1");
        config.SetBlockSize(4096);
        config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
        config.AddPartitions()->SetBlockCount(33554432);
        const auto registration = runtime.RegisterVolume(config);
        UNIT_ASSERT(!HasError(registration));

        const auto response = mount();
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_VALUES_EQUAL(response.GetVolume().GetDiskId(), "disk1");
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetVolume().GetBlocksCount(),
            33554432);
        UNIT_ASSERT(!response.GetSessionId().empty());
        UNIT_ASSERT_VALUES_EQUAL(response.GetInactiveClientsTimeout(), 0);

        runtime.Stop();
        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_REJECTED);
        runtime.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            mount().GetVolume().GetBlocksCount(),
            33554432);
        runtime.UnregisterVolume(registration.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(mount().GetError().GetCode(), E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldValidateBothIoMethodsAndRevokeSessionThroughFacade)
    {
        TNbsFrontendRuntime runtime(TLog{});
        auto blockStore = runtime.GetBlockStore();
        NKikimrBlockStore::TVolumeConfig config;
        config.SetDiskId("disk1");
        config.SetBlockSize(4096);
        config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
        config.AddPartitions()->SetBlockCount(33554432);
        UNIT_ASSERT(!HasError(runtime.RegisterVolume(config)));
        runtime.Start();
        auto mount = std::make_shared<
            NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest>();
        mount->SetDiskId("disk1");
        mount->MutableHeaders()->SetClientId("client1");
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
        request->SetDiskId("disk1");                                           \
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
        checkIo("client1", "", E_BS_INVALID_SESSION);
        checkIo("client2", mounted.GetSessionId(), E_BS_INVALID_SESSION);
        checkIo("client1", mounted.GetSessionId(), E_NOT_IMPLEMENTED);
        auto unmount = std::make_shared<
            NNbs1CompatApi::NBlockStore::NProto::TUnmountVolumeRequest>();
        unmount->SetDiskId("disk1");
        unmount->MutableHeaders()->SetClientId("client1");
        unmount->SetSessionId(mounted.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(
            blockStore->UnmountVolume(MakeIntrusive<TCallContext>(), unmount)
                .GetValueSync()
                .GetError()
                .GetCode(),
            S_OK);
        checkIo("client1", mounted.GetSessionId(), E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            blockStore->UnmountVolume(MakeIntrusive<TCallContext>(), unmount)
                .GetValueSync()
                .GetError()
                .GetCode(),
            S_ALREADY);
        runtime.Stop();
        checkIo("client1", mounted.GetSessionId(), E_REJECTED);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

}   // namespace NYdb::NBS::NBlockStore
