#include "frontend_test.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/events.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session_control.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NTests {

namespace {

using namespace NStorage::NPartitionDirect;

struct TEvStopSession final
    : NActors::TEventLocal<
          TEvStopSession,
          EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 1>
{
    NThreading::TPromise<void> Done = NThreading::NewPromise<void>();
};

// Keeps session mutations on the owner actor, including teardown.
class TTestPartitionSessionActor final
    : public NActors::TActorBootstrapped<TTestPartitionSessionActor>
{
public:
    explicit TTestPartitionSessionActor(TPartitionSessionPtr state)
        : State(std::move(state))
    {}

    ~TTestPartitionSessionActor() override
    {
        State->Stop();
    }

    void Bootstrap()
    {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionSession::TEvMount, HandleMount);
            hFunc(TEvPartitionSession::TEvUnmount, HandleUnmount);
            hFunc(TEvStopSession, HandleStop);
        }
    }

private:
    void HandleMount(TEvPartitionSession::TEvMount::TPtr& ev)
    {
        auto* request = ev->Get();
        request->Result.SetValue(State->Mount(request->ClientId));
    }

    void HandleUnmount(TEvPartitionSession::TEvUnmount::TPtr& ev)
    {
        auto* request = ev->Get();
        request->Result.SetValue(
            State->Unmount(request->ClientId, request->SessionId));
    }

    void HandleStop(TEvStopSession::TPtr& ev)
    {
        State->Stop();
        ev->Get()->Done.SetValue();
        PassAway();
    }

    const TPartitionSessionPtr State;
};

}   // namespace

const TString TestDiskId = "disk1";
const TString TestClientId = "client1";

// Retains the identity needed to simulate teardown of any incarnation.
struct TFrontendTestEnv::TRegistration
{
    TString DiskId;
    TString Token;
    NActors::TActorId ActorId;
};

TFrontendTestEnv::TFrontendTestEnv()
    : Actors(std::make_unique<NActors::TTestActorRuntimeBase>(1, true))
{
    Actors->Initialize();
}

TFrontendTestEnv::~TFrontendTestEnv()
{
    Facade->Stop();
    for (const auto& registration: Registrations) {
        Facade->UnregisterVolume(registration.DiskId, registration.Token);
    }
    Actors.reset();
}

TResultOrError<TString> TFrontendTestEnv::RegisterVolume(
    const NKikimrBlockStore::TVolumeConfig& metadata,
    IStoragePtr storage,
    TVolumeConfigPtr geometry)
{
    auto created = TPartitionSession::Create(
        metadata,
        std::move(storage),
        std::move(geometry));
    if (HasError(created)) {
        return created.GetError();
    }
    auto state = created.ExtractResult();
    const auto actorId =
        Actors->Register(new TTestPartitionSessionActor(state));
    auto registration = Facade->RegisterVolume(
        state,
        CreatePartitionSessionControl(Actors->GetActorSystem(0), actorId));
    Registrations.push_back(
        {metadata.GetDiskId(), state->GetRegistrationId(), actorId});
    return registration;
}

void TFrontendTestEnv::UnregisterVolume(
    const TString& diskId,
    const TString& registrationId)
{
    Facade->UnregisterVolume(diskId, registrationId);
    for (auto it = Registrations.begin(); it != Registrations.end(); ++it) {
        if (it->DiskId == diskId && it->Token == registrationId) {
            auto event = std::make_unique<TEvStopSession>();
            auto done = event->Done.GetFuture();
            Actors->GetActorSystem(0)->Send(it->ActorId, event.release());
            Y_ABORT_UNLESS(done.Wait(TDuration::Seconds(10)));
            done.GetValueSync();
            Registrations.erase(it);
            break;
        }
    }
}

NKikimrBlockStore::TVolumeConfig MakeTestVolumeConfig(
    ui32 blockSize,
    ui64 blocksCount)
{
    NKikimrBlockStore::TVolumeConfig config;
    config.SetDiskId(TestDiskId);
    config.SetBlockSize(blockSize);
    config.AddPartitions()->SetBlockCount(blocksCount);
    config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
    config.SetVersion(42);
    config.SetProjectId("project");
    config.SetFolderId("folder");
    config.SetCloudId("cloud");
    return config;
}

NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest MakeTestMountRequest()
{
    NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest request;
    request.SetDiskId(TestDiskId);
    request.MutableHeaders()->SetClientId(TestClientId);
    return request;
}

TVolumeConfigPtr MakeTestIoConfig(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    ui64 stripeBytes)
{
    return std::make_shared<TVolumeConfig>(TVolumeConfig{
        .DiskId = volumeMetadata.GetDiskId(),
        .BlockSize = volumeMetadata.GetBlockSize(),
        .BlockCount = volumeMetadata.PartitionsSize()
                          ? volumeMetadata.GetPartitions(0).GetBlockCount()
                          : 0,
        .BlocksPerStripe = volumeMetadata.GetBlockSize()
                               ? stripeBytes / volumeMetadata.GetBlockSize()
                               : 0,
        .VChunkSize = TestVChunkSize,
    });
}

TResultOrError<TString> RegisterTestVolume(
    TFrontendTestEnv& env,
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    ui32* readCalls)
{
    auto storage = std::make_shared<TTestStorage>();
    const ui32 blockSize = volumeMetadata.GetBlockSize();
    storage->ReadBlocksLocalHandler =
        [readCalls, blockSize](TCallContextPtr context, auto request)
    {
        Y_UNUSED(context);
        if (readCalls) {
            ++*readCalls;
        }
        const auto guard = request->Sglist.Acquire();
        UNIT_ASSERT(guard);
        const TString data(request->Headers.Range.Size() * blockSize, 'x');
        UNIT_ASSERT_VALUES_EQUAL(
            SgListCopy(TBlockDataRef(data.data(), data.size()), guard.Get()),
            data.size());
        return NThreading::MakeFuture<TReadBlocksLocalResponse>();
    };
    return env.RegisterVolume(
        volumeMetadata,
        std::move(storage),
        MakeTestIoConfig(volumeMetadata));
}

}   // namespace NYdb::NBS::NBlockStore::NTests
