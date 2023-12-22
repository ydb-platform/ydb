#pragma once

#include "public.h"

#include <optional>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt_proto/yt/client/bundle_controller/proto/bundle_controller_service.pb.h>

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

struct TCpuLimits
    : public NYTree::TYsonStruct
{
    int LookupThreadPoolSize;
    int QueryThreadPoolSize;
    int WriteThreadPoolSize;

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

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TCpuLimits* protoCpuLimits, const NBundleControllerClient::TCpuLimitsPtr cpuLimits);
void FromProto(NBundleControllerClient::TCpuLimitsPtr cpuLimits, const NBundleController::NProto::TCpuLimits* protoCpuLimits);

void ToProto(NBundleController::NProto::TMemoryLimits* protoMemoryLimits, const NBundleControllerClient::TMemoryLimitsPtr memoryLimits);
void FromProto(NBundleControllerClient::TMemoryLimitsPtr memoryLimits, const NBundleController::NProto::TMemoryLimits* protoMemoryLimits);

void ToProto(NBundleController::NProto::TInstanceResources* protoInstanceResources, const NBundleControllerClient::TInstanceResourcesPtr instanceResources);
void FromProto(NBundleControllerClient::TInstanceResourcesPtr instanceResources, const NBundleController::NProto::TInstanceResources* protoInstanceResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NYT::NBundleControllerClient
