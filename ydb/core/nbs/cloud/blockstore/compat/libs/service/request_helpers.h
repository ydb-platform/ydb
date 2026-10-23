#pragma once

#include "public.h"

#include "context.h"
#include "request.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/random.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TRequestInfo
{
    const EBlockStoreRequest Request;
    const ui64 RequestId;
    const TString DiskId;
    const TString ClientId;
    const TString InstanceId;

    TRequestInfo(
            EBlockStoreRequest request,
            ui64 requestId,
            TString diskId,
            TString clientId = {},
            TString instanceId = {})
        : Request(request)
        , RequestId(requestId)
        , DiskId(std::move(diskId))
        , ClientId(std::move(clientId))
        , InstanceId(std::move(instanceId))
    {}
};

IOutputStream& operator<<(IOutputStream& out, const TRequestInfo& info);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept HasBlockSize = requires (T& r)
{
    { r.BlockSize } -> std::same_as<ui32&>;
    { r.BlockSize = ui32() };
};

template <typename T>
concept HasProtoBlockSize = requires (T& r)
{
    { r.SetBlockSize(ui32()) };
    { r.GetBlockSize() } -> std::same_as<ui32>;
};

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId();

template <typename T>
ui64 GetRequestId(const T& request)
{
    return request.GetHeaders().GetRequestId();
}

template <typename T>
ui64 EnsureRequestId(T& request)
{
    auto& headers = *request.MutableHeaders();

    if (!headers.GetRequestId()) {
        headers.SetRequestId(CreateRequestId());
    }

    return headers.GetRequestId();
}

////////////////////////////////////////////////////////////////////////////////

// NBS-2966. We can't get BlocksCount for TWriteBlocksRequest.
ui32 GetBlocksCount(const NProto::TWriteBlocksRequest& request) = delete;

template <typename T>
    requires requires(const T& t) { t.BlocksCount; }
ui32 GetBlocksCount(const T& request)
{
    return request.BlocksCount;
}

template <typename T>
    requires requires(const T& t) { t.GetBlocksCount(); }
ui32 GetBlocksCount(const T& request)
{
    return request.GetBlocksCount();
}

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateBytesCount(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize);

template <typename T>
    requires requires(const T& t) { GetBlocksCount(t); }
ui64 CalculateBytesCount(const T& request, const ui32 blockSize)
{
    return GetBlocksCount(request) * static_cast<ui64>(blockSize);
}

////////////////////////////////////////////////////////////////////////////////

ui32 CalculateWriteRequestBlockCount(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize);
ui32 CalculateWriteRequestBlockCount(
    const NProto::TWriteBlocksLocalRequest& request,
    const ui32 blockSize);
ui32 CalculateWriteRequestBlockCount(
    const NProto::TZeroBlocksRequest& request,
    const ui32 blockSize);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires requires(const T& t) { t.GetStartIndex(); }
ui64 GetStartIndex(const T& request)
{
    return request.GetStartIndex();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetRequestDetails(const T& request)
{
    constexpr bool IsTWriteBlocksRequest =
        std::is_base_of_v<T, NProto::TWriteBlocksRequest>;

    constexpr bool HasGetStartIndex =
        requires(const T& t) { t.GetStartIndex(); };

    if constexpr (IsTWriteBlocksRequest) {
        ui64 bytes = 0;
        for (const auto& buffer: request.GetBlocks().GetBuffers()) {
            bytes += buffer.size();
        }

        return TStringBuilder()
               << " (offset: " << request.GetStartIndex()
               << ", size: " << bytes
               << ", buffers: " << request.GetBlocks().BuffersSize()
               << ")";
    } else if constexpr (HasGetStartIndex) {
        return TStringBuilder()
               << " (offset: " << request.GetStartIndex()
               << ", count: " << GetBlocksCount(request) << ")";
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetDiskId(const T& request)
{
    constexpr bool HasGetDiskId = requires { request.GetDiskId(); };
    if constexpr (HasGetDiskId) {
        return request.GetDiskId();
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString GetClientId(const T& request)
{
    return request.GetHeaders().GetClientId();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr EBlockStoreRequest GetBlockStoreRequest();

constexpr EBlockStoreRequest TranslateLocalRequestType(
    EBlockStoreRequest requestType);

constexpr bool IsControlRequest(EBlockStoreRequest requestType);

constexpr bool IsDataChannel(NProto::ERequestSource source);

constexpr bool IsReadRequest(EBlockStoreRequest requestType);
constexpr bool IsWriteRequest(EBlockStoreRequest requestType);
constexpr bool IsReadWriteRequest(EBlockStoreRequest requestType);
constexpr bool IsNonLocalReadWriteRequest(EBlockStoreRequest requestType);

////////////////////////////////////////////////////////////////////////////////

bool IsReadWriteMode(const NProto::EVolumeAccessMode mode);

TString GetIpcTypeString(NProto::EClientIpcType ipcType);

template <typename T>
bool IsThrottlingDisabled(const T& request);

template <typename T>
consteval bool ShouldBeThrottled();

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {
    template <typename T>
    struct TRequest {};

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    template <>                                                                \
    struct TRequest<NProto::T##name##Request>                                  \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

}   // namespace NImpl

template <typename T>
constexpr EBlockStoreRequest GetBlockStoreRequest()
{
    return NImpl::TRequest<T>::Request;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsReadRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
            return true;

        default:
            return false;
    }
}

constexpr bool IsWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::WriteBlocksLocal:
            return true;

        default:
            return false;
    }
}

constexpr bool IsReadWriteRequest(EBlockStoreRequest requestType)
{
    return IsReadRequest(requestType) || IsWriteRequest(requestType);
}

constexpr bool IsNonLocalReadWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
            return true;

        default:
            return false;
    }
}

constexpr EBlockStoreRequest TranslateLocalRequestType(
    EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocksLocal:
            return EBlockStoreRequest::ReadBlocks;
        case EBlockStoreRequest::WriteBlocksLocal:
            return EBlockStoreRequest::WriteBlocks;

        default:
            return requestType;
    }
}

constexpr bool IsControlRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::Ping:
        case EBlockStoreRequest::MountVolume:
        case EBlockStoreRequest::UnmountVolume:
        case EBlockStoreRequest::UploadClientMetrics:
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
        case EBlockStoreRequest::WriteBlocksLocal:
        case EBlockStoreRequest::QueryAvailableStorage:
            return false;
        case EBlockStoreRequest::CreateVolume:
        case EBlockStoreRequest::DestroyVolume:
        case EBlockStoreRequest::ResizeVolume:
        case EBlockStoreRequest::StatVolume:
        case EBlockStoreRequest::AssignVolume:
        case EBlockStoreRequest::CreateCheckpoint:
        case EBlockStoreRequest::DeleteCheckpoint:
        case EBlockStoreRequest::AlterVolume:
        case EBlockStoreRequest::GetChangedBlocks:
        case EBlockStoreRequest::GetCheckpointStatus:
        case EBlockStoreRequest::DescribeVolume:
        case EBlockStoreRequest::ListVolumes:
        case EBlockStoreRequest::DiscoverInstances:
        case EBlockStoreRequest::ExecuteAction:
        case EBlockStoreRequest::DescribeVolumeModel:
        case EBlockStoreRequest::UpdateDiskRegistryConfig:
        case EBlockStoreRequest::UpdateVolumeThrottlingConfig:
        case EBlockStoreRequest::DescribeDiskRegistryConfig:
        case EBlockStoreRequest::CreatePlacementGroup:
        case EBlockStoreRequest::DestroyPlacementGroup:
        case EBlockStoreRequest::AlterPlacementGroupMembership:
        case EBlockStoreRequest::DescribePlacementGroup:
        case EBlockStoreRequest::ListPlacementGroups:
        case EBlockStoreRequest::CmsAction:
        case EBlockStoreRequest::CreateVolumeFromDevice:
        case EBlockStoreRequest::ResumeDevice:
        case EBlockStoreRequest::StartEndpoint:
        case EBlockStoreRequest::StopEndpoint:
        case EBlockStoreRequest::ListEndpoints:
        case EBlockStoreRequest::KickEndpoint:
        case EBlockStoreRequest::ListKeyrings:
        case EBlockStoreRequest::DescribeEndpoint:
        case EBlockStoreRequest::RefreshEndpoint:
        case EBlockStoreRequest::QueryAgentsInfo:
        case EBlockStoreRequest::CreateVolumeLink:
        case EBlockStoreRequest::DestroyVolumeLink:
        case EBlockStoreRequest::RemoveVolumeClient:
        case EBlockStoreRequest::ListDiskStates:
        case EBlockStoreRequest::ListNVMeDevices:
        case EBlockStoreRequest::AcquireNVMeDevice:
        case EBlockStoreRequest::ReleaseNVMeDevice:
            return true;
        case EBlockStoreRequest::MAX:
            Y_DEBUG_ABORT_UNLESS(false);
            break;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsDataChannel(NProto::ERequestSource source)
{
    switch (source) {
        case NProto::SOURCE_TCP_DATA_CHANNEL:
        case NProto::SOURCE_FD_DATA_CHANNEL:
            return true;

        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool IsThrottlingDisabled(const T& request)
{
    return HasProtoFlag(request.GetMountFlags(), NProto::MF_THROTTLING_DISABLED);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
consteval bool ShouldBeThrottled()
{
    return IsReadWriteRequest(GetBlockStoreRequest<T>());
}

}   // namespace NCloud::NBlockStore
