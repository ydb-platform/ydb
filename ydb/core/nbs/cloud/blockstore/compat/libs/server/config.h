#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/config/server.pb.h>

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/dumpable.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/affinity.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NServer {

using NYdb::NBS::TAffinity;

////////////////////////////////////////////////////////////////////////////////

struct TCertificate
{
    TString CertFile;
    TString CertPrivateKeyFile;
};

////////////////////////////////////////////////////////////////////////////////

class TServerAppConfig
    : public NYdb::NBS::NBlockStore::IDumpable
{
private:
    const NProto::TServerAppConfig AppConfig;

    const NProto::TServerConfig* ServerConfig = nullptr;
    const NProto::TKikimrServiceConfig* KikimrServiceConfig = nullptr;
    const NProto::TLocalServiceConfig* LocalServiceConfig = nullptr;
    const NProto::TNullServiceConfig* NullServiceConfig = nullptr;

public:
    TServerAppConfig(NProto::TServerAppConfig appConfig = {});

    const NProto::TServerConfig* GetServerConfig() const
    {
        return ServerConfig;
    }

    const NProto::TKikimrServiceConfig* GetKikimrServiceConfig() const
    {
        return KikimrServiceConfig;
    }

    const NProto::TLocalServiceConfig* GetLocalServiceConfig() const
    {
        return LocalServiceConfig;
    }

    const NProto::TNullServiceConfig* GetNullServiceConfig() const
    {
        return NullServiceConfig;
    }

    TString GetHost() const;
    ui32 GetPort() const;
    TString GetDataHost() const;
    ui32 GetDataPort() const;
    ui32 GetMaxMessageSize() const;
    ui32 GetThreadsCount() const;
    ui32 GetPreparedRequestsCount() const;
    ui32 GetMemoryQuotaBytes() const;
    TString GetSecureHost() const;
    ui32 GetSecurePort() const;
    TString GetRootCertsFile() const;
    TString GetCertFile() const;
    TString GetCertPrivateKeyFile() const;
    TVector<TCertificate> GetCerts() const;
    TVector<TCertificate> GetCertsWithLegacyFallback() const;
    bool GetKeepAliveEnabled() const;
    TDuration GetKeepAliveIdleTimeout() const;
    TDuration GetKeepAliveProbeTimeout() const;
    ui32 GetKeepAliveProbesCount() const;
    bool GetStrictContractValidation() const;
    bool GetLoadCmsConfigs() const;
    TDuration GetShutdownTimeout() const;
    TDuration GetRequestTimeout() const;
    TString GetUnixSocketPath() const;
    ui32 GetUnixSocketBacklog() const;
    ui32 GetGrpcThreadsLimit() const;
    bool GetVhostEnabled() const;
    ui32 GetVhostThreadsCount() const;
    bool GetNvmfInitiatorEnabled() const;
    TString GetNodeType() const;
    TString GetRootKeyringName() const;
    TString GetEndpointsKeyringName() const;
    bool GetNbdEnabled() const;
    ui32 GetNbdThreadsCount() const;
    bool GetNbdLimiterEnabled() const;
    ui64 GetMaxInFlightBytesPerThread() const;
    TAffinity GetVhostAffinity() const;
    TAffinity GetNbdAffinity() const;
    ui32 GetNodeRegistrationMaxAttempts() const;
    TDuration GetNodeRegistrationTimeout() const;
    TDuration GetNodeRegistrationErrorTimeout() const;
    TString GetNbdSocketSuffix() const;
    ui32 GetGrpcKeepAliveTime() const;
    ui32 GetGrpcKeepAliveTimeout() const;
    bool GetGrpcKeepAlivePermitWithoutCalls() const;
    ui32 GetGrpcHttp2MinRecvPingIntervalWithoutData() const;
    ui32 GetGrpcHttp2MinSentPingIntervalWithoutData() const;
    bool GetNVMeEndpointEnabled() const;
    TString GetNVMeEndpointNqn() const;
    TVector<TString> GetNVMeEndpointTransportIDs() const;
    bool GetSCSIEndpointEnabled() const;
    TString GetSCSIEndpointName() const;
    TString GetSCSIEndpointListenAddress() const;
    ui32 GetSCSIEndpointListenPort() const;
    bool GetRdmaEndpointEnabled() const;
    TString GetRdmaEndpointListenAddress() const;
    ui32 GetRdmaEndpointListenPort() const;
    bool GetThrottlingEnabled() const;
    ui64 GetMaxReadBandwidth() const;
    ui64 GetMaxWriteBandwidth() const;
    ui32 GetMaxReadIops() const;
    ui32 GetMaxWriteIops() const;
    TDuration GetMaxBurstTime() const;
    bool DeprecatedGetRdmaClientEnabled() const;
    const ::NCloud::NProto::TRdmaClient& DeprecatedGetRdmaClientConfig() const;
    bool GetUseFakeRdmaClient() const;
    bool GetDisableClientThrottlers() const;
    NCloud::NProto::EEndpointStorageType GetEndpointStorageType() const;
    TString GetEndpointStorageDir() const;
    TString GetVhostServerPath() const;
    TString GetNbdDevicePrefix() const;
    ui32 GetSocketAccessMode() const;
    bool GetNbdNetlink() const;
    TDuration GetNbdRequestTimeout() const;
    TDuration GetNbdConnectionTimeout() const;
    bool GetAllowAllRequestsViaUDS() const;
    bool GetEndpointStorageNotImplementedErrorIsFatal() const;
    TDuration GetVhostServerTimeoutAfterParentExit() const;
    TString GetNodeRegistrationToken() const;
    NProto::TChecksumFlags GetChecksumFlags() const;
    bool GetVhostDiscardEnabled() const;
    bool GetVhostDiscardOnlyEnabled() const;
    bool GetVhostWriteZeroesEnabled() const;
    bool GetDropDiscardRequests() const;
    ui32 GetVhostOptimalIoSize() const;
    ui32 GetMaxZeroBlocksSubRequestSize() const;
    NProto::EEncryptZeroPolicy GetEncryptZeroPolicy() const;
    ui64 GetVhostPteFlushByteThreshold() const;
    bool GetAutomaticNbdDeviceManagement() const;
    bool GetEnableOverlappingRequestsGuard() const;
    bool GetEnableRequestSplitter() const;
    ui64 GetExternalVhostServerThreadPoolSize() const;
    TDuration GetRefreshCertsPeriod() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;

private:
    bool GetRdmaClientEnabled() const;
};

}   // namespace NCloud::NBlockStore::NServer
