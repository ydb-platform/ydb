#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore::NProto {
class TMountVolumeRequest;
}

namespace NYdb::NBS::NBlockStore {

class TFrontendState;
class TNbsFrontendRuntime;

namespace NTests {

extern const TString TestDiskId;
extern const TString TestClientId;
constexpr ui64 TestBlocksCount = 33554432;
constexpr ui64 TestStripeBytes = 512_KB;
constexpr ui64 TestVChunkSize = 128_MB;

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

// Control-only tests need a backend registration but must not issue native I/O.
TResultOrError<TString> RegisterTestVolume(
    TFrontendState& frontend,
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata);

// Registers the same control-only backend through the runtime boundary.
TResultOrError<TString> RegisterTestVolume(
    TNbsFrontendRuntime& frontend,
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata);

}   // namespace NTests

}   // namespace NYdb::NBS::NBlockStore
