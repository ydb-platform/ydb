#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/actions.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/checkpoints.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/cms.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/discovery.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/disk.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/endpoints.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/io.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/local_nvme.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/local_ssd.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/metrics.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/mount.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/ping.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/placement.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/volume.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/volume_throttling.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

namespace NProto {

using namespace NCloud::NProto;
using NYdb::NBS::TGuardedSgList;
using NYdb::NBS::TSgList;
using NYdb::NBS::NProto::TError;

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksLocalRequest
    : public TReadBlocksRequest
{
    TGuardedSgList Sglist;
    ui64 CommitId = 0;
    bool ShouldReportFailedRangesOnFailure = false;
};

struct TExtendedFailInfo
{
    TVector<TString> FailedRanges;
};

struct TReadBlocksLocalResponse: public TReadBlocksResponse
{
    TExtendedFailInfo FailInfo;

    TReadBlocksLocalResponse() = default;

    explicit TReadBlocksLocalResponse(TReadBlocksResponse&& base)
        : TReadBlocksResponse(std::move(base))
    {}
};

////////////////////////////////////////////////////////////////////////////////

// TWriteBlocksLocalRequest has two modes:
//
//   Dependent (OwnsSglist=false) — Sglist points to external memory.
//   Owner     (OwnsSglist=true)  — data is copied into Blocks, Sglist
//                                  points to it. Destructor closes Sglist
//                                  before Blocks are freed.
//
// Use TakeDataOwnership() to promote dependent → owner.

struct TWriteBlocksLocalRequest: public TWriteBlocksRequest
{
    // Tag struct for constructor disambiguation (tag dispatch).
    // Pass TDependentTag{} to select the constructor that creates a dependent
    // (non-owning) copy: proto fields are copied, Sglist is shared with the
    // source, OwnsSglist stays false.
    struct TDependentTag
    {
    };

    TGuardedSgList Sglist;
    ui32 BlocksCount = 0;

    TWriteBlocksLocalRequest() = default;

    TWriteBlocksLocalRequest(const TWriteBlocksLocalRequest& request) = delete;

    TWriteBlocksLocalRequest(TWriteBlocksLocalRequest&& other) noexcept;

    TWriteBlocksLocalRequest(
        const TWriteBlocksLocalRequest& source,
        TDependentTag);

    TWriteBlocksLocalRequest& operator=(
        const TWriteBlocksLocalRequest& request) = delete;

    TWriteBlocksLocalRequest& operator=(
        TWriteBlocksLocalRequest&& other) noexcept;

    // If owner, closes Sglist before Blocks are freed.
    ~TWriteBlocksLocalRequest();

    // Copies data from Sglist into Blocks, rebuilds Sglist to point into those
    // Blocks buffers, and sets OwnsSglist=true.
    // No-op if already owner (OwnsSglist=true) or if the object was moved from.
    void TakeDataOwnership();

    bool IsOwner() const
    {
        return OwnsSglist;
    }

private:
    bool OwnsSglist = false;

    void CloseOwnedSglist();
};

using TWriteBlocksLocalResponse = TWriteBlocksResponse;

////////////////////////////////////////////////////////////////////////////////

TWriteBlocksLocalRequest CopyRequest(const TWriteBlocksLocalRequest& request);

TWriteBlocksRequest CopyRequest(const TWriteBlocksRequest& request);

TZeroBlocksRequest CopyRequest(const TZeroBlocksRequest& request);

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_GRPC_STORAGE_SERVICE(xxx, ...)                              \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(CreateVolume,                       __VA_ARGS__)                       \
    xxx(DestroyVolume,                      __VA_ARGS__)                       \
    xxx(ResizeVolume,                       __VA_ARGS__)                       \
    xxx(StatVolume,                         __VA_ARGS__)                       \
    xxx(AssignVolume,                       __VA_ARGS__)                       \
    xxx(MountVolume,                        __VA_ARGS__)                       \
    xxx(UnmountVolume,                      __VA_ARGS__)                       \
    xxx(ReadBlocks,                         __VA_ARGS__)                       \
    xxx(WriteBlocks,                        __VA_ARGS__)                       \
    xxx(ZeroBlocks,                         __VA_ARGS__)                       \
    xxx(CreateCheckpoint,                   __VA_ARGS__)                       \
    xxx(DeleteCheckpoint,                   __VA_ARGS__)                       \
    xxx(AlterVolume,                        __VA_ARGS__)                       \
    xxx(GetChangedBlocks,                   __VA_ARGS__)                       \
    xxx(GetCheckpointStatus,                __VA_ARGS__)                       \
    xxx(DescribeVolume,                     __VA_ARGS__)                       \
    xxx(ListVolumes,                        __VA_ARGS__)                       \
    xxx(UploadClientMetrics,                __VA_ARGS__)                       \
    xxx(DiscoverInstances,                  __VA_ARGS__)                       \
    xxx(ExecuteAction,                      __VA_ARGS__)                       \
    xxx(DescribeVolumeModel,                __VA_ARGS__)                       \
    xxx(UpdateDiskRegistryConfig,           __VA_ARGS__)                       \
    xxx(DescribeDiskRegistryConfig,         __VA_ARGS__)                       \
    xxx(CreatePlacementGroup,               __VA_ARGS__)                       \
    xxx(DestroyPlacementGroup,              __VA_ARGS__)                       \
    xxx(AlterPlacementGroupMembership,      __VA_ARGS__)                       \
    xxx(DescribePlacementGroup,             __VA_ARGS__)                       \
    xxx(ListPlacementGroups,                __VA_ARGS__)                       \
    xxx(CmsAction,                          __VA_ARGS__)                       \
    xxx(QueryAvailableStorage,              __VA_ARGS__)                       \
    xxx(CreateVolumeFromDevice,             __VA_ARGS__)                       \
    xxx(ResumeDevice,                       __VA_ARGS__)                       \
    xxx(QueryAgentsInfo,                    __VA_ARGS__)                       \
    xxx(ListDiskStates,                     __VA_ARGS__)                       \
    xxx(CreateVolumeLink,                   __VA_ARGS__)                       \
    xxx(DestroyVolumeLink,                  __VA_ARGS__)                       \
    xxx(RemoveVolumeClient,                 __VA_ARGS__)                       \
    xxx(UpdateVolumeThrottlingConfig,       __VA_ARGS__)                       \
// BLOCKSTORE_GRPC_STORAGE_SERVICE

#define BLOCKSTORE_ENDPOINT_SERVICE(xxx, ...)                                  \
    xxx(StartEndpoint,                      __VA_ARGS__)                       \
    xxx(StopEndpoint,                       __VA_ARGS__)                       \
    xxx(ListEndpoints,                      __VA_ARGS__)                       \
    xxx(KickEndpoint,                       __VA_ARGS__)                       \
    xxx(ListKeyrings,                       __VA_ARGS__)                       \
    xxx(DescribeEndpoint,                   __VA_ARGS__)                       \
    xxx(RefreshEndpoint,                    __VA_ARGS__)                       \
// BLOCKSTORE_ENDPOINT_SERVICE

#define BLOCKSTORE_LOCAL_NVME_SERVICE(xxx, ...)                                \
    xxx(ListNVMeDevices,                    __VA_ARGS__)                       \
    xxx(AcquireNVMeDevice,                  __VA_ARGS__)                       \
    xxx(ReleaseNVMeDevice,                  __VA_ARGS__)                       \
// BLOCKSTORE_LOCAL_NVME_SERVICE

#define BLOCKSTORE_GRPC_SERVICE(xxx, ...)                                      \
    BLOCKSTORE_GRPC_STORAGE_SERVICE(xxx,    __VA_ARGS__)                       \
    BLOCKSTORE_ENDPOINT_SERVICE(xxx,        __VA_ARGS__)                       \
    BLOCKSTORE_LOCAL_NVME_SERVICE(xxx,      __VA_ARGS__)                       \
// BLOCKSTORE_GRPC_SERVICE

#define BLOCKSTORE_GRPC_DATA_SERVICE(xxx, ...)                                 \
    xxx(MountVolume,                        __VA_ARGS__)                       \
    xxx(UnmountVolume,                      __VA_ARGS__)                       \
    xxx(ReadBlocks,                         __VA_ARGS__)                       \
    xxx(WriteBlocks,                        __VA_ARGS__)                       \
    xxx(ZeroBlocks,                         __VA_ARGS__)                       \
    xxx(UploadClientMetrics,                __VA_ARGS__)                       \
// BLOCKSTORE_GRPC_DATA_SERVICE

#define BLOCKSTORE_LOCAL_SERVICE(xxx, ...)                                     \
    xxx(ReadBlocksLocal,                    __VA_ARGS__)                       \
    xxx(WriteBlocksLocal,                   __VA_ARGS__)                       \
// BLOCKSTORE_LOCAL_SERVICE

#define BLOCKSTORE_STORAGE_SERVICE(xxx, ...)                                   \
    BLOCKSTORE_GRPC_STORAGE_SERVICE(xxx,    __VA_ARGS__)                       \
    BLOCKSTORE_LOCAL_SERVICE(xxx,           __VA_ARGS__)                       \
// BLOCKSTORE_STORAGE_SERVICE

#define BLOCKSTORE_SERVICE(xxx, ...)                                           \
    BLOCKSTORE_STORAGE_SERVICE(xxx,         __VA_ARGS__)                       \
    BLOCKSTORE_ENDPOINT_SERVICE(xxx,        __VA_ARGS__)                       \
    BLOCKSTORE_LOCAL_NVME_SERVICE(xxx,      __VA_ARGS__)                       \
// BLOCKSTORE_SERVICE

#define BLOCKSTORE_DATA_SERVICE(xxx, ...)                                      \
    BLOCKSTORE_GRPC_DATA_SERVICE(xxx,       __VA_ARGS__)                       \
    BLOCKSTORE_LOCAL_SERVICE(xxx,           __VA_ARGS__)                       \
// BLOCKSTORE_DATA_SERVICE

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...) name,

enum class EBlockStoreRequest
{
    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)
    MAX
};

#undef BLOCKSTORE_DECLARE_METHOD

constexpr size_t BlockStoreRequestsCount = (size_t)EBlockStoreRequest::MAX;

const TString& GetBlockStoreRequestName(EBlockStoreRequest requestType);

template<typename TRequest>
TString GetBlockStoreRequestName();

enum class ESysRequestType
{
    Compaction = 10000,
    Flush = 10001,
    ConvertToMixedIndex = 10002,
    ConvertToRangeMap = 10003,
    Cleanup = 10004,
    MigrationRead = 10005,
    WriteDeviceBlocks = 10006,
    ZeroDeviceBlocks = 10007,
    ResyncChecksum = 10008,
    ConfirmBlobs = 10009,
    ReadDeviceBlocks = 10010,
    ResyncRead = 10011,
    ResyncWrite = 10012,
    Scrubbing = 10013,
    MigrationWrite = 10014,
    MAX
};

TString GetSysRequestName(ESysRequestType requestType);

enum class EPrivateRequestType
{
    DescribeBlocks = 20000,
    MAX
};

TStringBuf GetPrivateRequestName(EPrivateRequestType requestType);

}   // namespace NCloud::NBlockStore

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NProto::TReadBlocksLocalResponse>(
    IOutputStream& out,
    const NCloud::NBlockStore::NProto::TReadBlocksLocalResponse& value);
