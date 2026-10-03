#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/config/client.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/dumpable.h>

#include <ydb/core/nbs/cloud/storage/core/compat/config/iam.pb.h>
#include <ydb/core/nbs/cloud/storage/core/compat/libs/diagnostics/trace_reader.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NClient {

using NYdb::NBS::EWellKnownResultCodes;

////////////////////////////////////////////////////////////////////////////////

class TClientAppConfig: public NYdb::NBS::NBlockStore::IDumpable
{
private:
    const NProto::TClientAppConfig AppConfig;

    const NProto::TClientConfig& ClientConfig;
    const NProto::TLogConfig& LogConfig;
    const NProto::TMonitoringConfig& MonitoringConfig;
    const NCloud::NProto::TIamClientConfig& IamConfig;

public:
    TClientAppConfig(NProto::TClientAppConfig appConfig = {});

    const NProto::TClientConfig& GetClientConfig() const
    {
        return ClientConfig;
    }

    const NProto::TLogConfig& GetLogConfig() const
    {
        return LogConfig;
    }

    const NProto::TMonitoringConfig& GetMonitoringConfig() const
    {
        return MonitoringConfig;
    }

    TString GetHost() const;
    ui32 GetPort() const;
    ui32 GetInsecurePort() const;
    ui32 GetMaxMessageSize() const;
    ui32 GetThreadsCount() const;
    TDuration GetRequestTimeout() const;
    TDuration GetRequestTimeoutIncrementOnRetry() const;
    TDuration GetRequestTimeoutMax() const;
    TDuration GetRetryTimeout() const;
    TDuration GetRetryTimeoutIncrement() const;
    TDuration GetConnectionErrorMaxRetryTimeout() const;
    TDuration GetGrpcReconnectBackoff() const;
    TDuration GetDiskRegistryBasedDiskInitialRetryTimeout() const;
    TDuration GetYDBBasedDiskInitialRetryTimeout() const;
    bool GetEnableListBasedRetryRules() const;
    TVector<EWellKnownResultCodes>
    GetNonRetriableErrorsForReliableMedia() const;
    TVector<EWellKnownResultCodes>
    GetNonRetriableErrorsForUnreliableMedia() const;
    bool GetEnableNonBlockingRemount() const;
    ui32 GetMemoryQuotaBytes() const;
    ui32 GetSecurePort() const;
    bool GetSkipCertVerification() const;
    TString GetRootCertsFile() const;
    TString GetCertFile() const;
    TString GetCertPrivateKeyFile() const;
    TString GetAuthToken() const;
    TString GetUnixSocketPath() const;
    ui32 GetGrpcThreadsLimit() const;
    TString GetInstanceId() const;
    ui32 GetMaxRequestSize() const;
    TString GetClientId() const;
    NProto::EClientIpcType GetIpcType() const;
    ui32 GetNbdThreadsCount() const;
    TString GetNbdSocketSuffix() const;
    bool GetNbdStructuredReply() const;
    bool GetNbdUseNbsErrors() const;
    TDuration GetRemountDeadline() const;
    TString GetNvmeDeviceTransportId() const;
    TString GetNvmeDeviceNqn() const;
    TString GetScsiDeviceUrl() const;
    TString GetScsiInitiatorIqn() const;
    TString GetRdmaDeviceAddress() const;
    ui32 GetRdmaDevicePort() const;

    bool GetLocalNonreplDisableDurableClient() const;
    TRequestThresholds GetRequestThresholds() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

}   // namespace NCloud::NBlockStore::NClient
