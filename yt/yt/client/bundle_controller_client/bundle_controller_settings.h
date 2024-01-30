#pragma once

#include "public.h"

#include <optional>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt_proto/yt/client/bundle_controller/proto/bundle_controller_service.pb.h>

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

struct TCpuLimits
    : public NYTree::TYsonStruct
{
    std::optional<int> LookupThreadPoolSize;
    std::optional<int> QueryThreadPoolSize;
    std::optional<int> WriteThreadPoolSize;

    REGISTER_YSON_STRUCT(TCpuLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCpuLimits)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryLimits
    : public NYTree::TYsonStruct
{
    std::optional<i64> CompressedBlockCache;
    std::optional<i64> KeyFilterBlockCache;
    std::optional<i64> LookupRowCache;
    std::optional<i64> TabletDynamic;
    std::optional<i64> TabletStatic;
    std::optional<i64> UncompressedBlockCache;
    std::optional<i64> VersionedChunkMeta;

    REGISTER_YSON_STRUCT(TMemoryLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryLimits)

////////////////////////////////////////////////////////////////////////////////

struct TInstanceResources
    : public NYTree::TYsonStruct
{
    i64 Memory;
    std::optional<i64> Net;

    TString Type;
    int Vcpu;

    bool operator==(const TInstanceResources& resources) const;

    void Clear();

    REGISTER_YSON_STRUCT(TInstanceResources);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInstanceResources)

////////////////////////////////////////////////////////////////////////////////

struct TBundleTargetConfig
    : public NYTree::TYsonStruct
{
    TCpuLimitsPtr CpuLimits;
    TMemoryLimitsPtr MemoryLimits;
    std::optional<int> RpcProxyCount;
    TInstanceResourcesPtr RpcProxyResourceGuarantee;
    std::optional<int> TabletNodeCount;
    TInstanceResourcesPtr TabletNodeResourceGuarantee;

    REGISTER_YSON_STRUCT(TBundleTargetConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleTargetConfig)

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TCpuLimits* protoCpuLimits, const TCpuLimitsPtr cpuLimits);
void FromProto(TCpuLimitsPtr cpuLimits, const NBundleController::NProto::TCpuLimits* protoCpuLimits);

void ToProto(NBundleController::NProto::TMemoryLimits* protoMemoryLimits, const TMemoryLimitsPtr memoryLimits);
void FromProto(TMemoryLimitsPtr memoryLimits, const NBundleController::NProto::TMemoryLimits* protoMemoryLimits);

void ToProto(NBundleController::NProto::TInstanceResources* protoInstanceResources, const TInstanceResourcesPtr instanceResources);
void FromProto(TInstanceResourcesPtr instanceResources, const NBundleController::NProto::TInstanceResources* protoInstanceResources);

void ToProto(NBundleController::NProto::TInstanceResources* protoInstanceResources, const TInstanceResourcesPtr instanceResources);
void FromProto(TInstanceResourcesPtr instanceResources, const NBundleController::NProto::TInstanceResources* protoInstanceResources);

void ToProto(NBundleController::NProto::TBundleConfig* protoBundleConfig, const TBundleTargetConfigPtr bundleConfig);
void FromProto(TBundleTargetConfigPtr bundleConfig, const NBundleController::NProto::TBundleConfig* protoBundleConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NYT::NBundleControllerClient
