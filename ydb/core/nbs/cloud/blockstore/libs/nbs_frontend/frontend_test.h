#pragma once

#include "blockstore_facade.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <vector>

namespace NActors {
class TTestActorRuntimeBase;
}

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore::NProto {
class TMountVolumeRequest;
}

namespace NYdb::NBS::NBlockStore {

namespace NTests {

extern const TString TestDiskId;
extern const TString TestClientId;
constexpr ui64 TestBlocksCount = 33554432;
constexpr ui64 TestStripeBytes = 512_KB;
constexpr ui64 TestVChunkSize = 128_MB;

// Owns test partition actors and their registrations around a real frontend.
class TFrontendTestEnv final
{
public:
    // Starts the actor system for test partition owners.
    TFrontendTestEnv();
    ~TFrontendTestEnv();

    // Creates a session owner actor and publishes it to the real registry.
    TResultOrError<TString> RegisterVolume(
        const NKikimrBlockStore::TVolumeConfig& metadata,
        IStoragePtr storage,
        TVolumeConfigPtr geometry);

    // Simulates partition teardown; an old token cannot remove a replacement.
    void UnregisterVolume(const TString& diskId, const TString& registrationId);

    // Tests exercise the production facade directly.
    const INbsBlockStoreFacadePtr Facade = CreateNbsBlockStoreFacade(TLog{});

private:
    struct TRegistration;
    std::unique_ptr<NActors::TTestActorRuntimeBase> Actors;
    std::vector<TRegistration> Registrations;
};

// Supplies common metadata for frontend tests.
NKikimrBlockStore::TVolumeConfig MakeTestVolumeConfig(
    ui32 blockSize = DefaultBlockSize,
    ui64 blocksCount = TestBlocksCount);

// Forms a mount request for the default test disk and client.
NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest MakeTestMountRequest();

// Supplies the same native geometry that a real partition publishes.
TVolumeConfigPtr MakeTestIoConfig(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    ui64 stripeBytes = TestStripeBytes);

// Creates a test partition whose backend returns 'x' for any read range.
// The optional read counter must outlive the partition registration.
TResultOrError<TString> RegisterTestVolume(
    TFrontendTestEnv& env,
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    ui32* readCalls = nullptr);

}   // namespace NTests

}   // namespace NYdb::NBS::NBlockStore
