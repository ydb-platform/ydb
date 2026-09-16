#include "frontend_test.h"

#include "frontend_runtime.h"
#include "frontend_state.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>

namespace NYdb::NBS::NBlockStore::NTests {

const TString TestDiskId = "disk1";
const TString TestClientId = "client1";

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
    TFrontendState& frontend,
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata)
{
    return frontend.RegisterVolume(
        volumeMetadata,
        std::make_shared<TTestStorage>(),
        MakeTestIoConfig(volumeMetadata));
}

TResultOrError<TString> RegisterTestVolume(
    TNbsFrontendRuntime& frontend,
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata)
{
    return frontend.RegisterVolume(
        volumeMetadata,
        std::make_shared<TTestStorage>(),
        MakeTestIoConfig(volumeMetadata));
}

}   // namespace NYdb::NBS::NBlockStore::NTests
