#pragma once

#include "logger.h"

#include <kikimr/yndx/api/protos/persqueue.pb.h>

#include <library/cpp/tvmauth/client/facade.h>

namespace NPersQueue {

const TString& GetTvmPqServiceName();

class ICredentialsProvider {
public:
    virtual ~ICredentialsProvider() = default;
    virtual void FillAuthInfo(NPersQueueCommon::TCredentials* authInfo) const = 0;
};


class ITVMCredentialsForwarder : public ICredentialsProvider {
public:
    virtual ~ITVMCredentialsForwarder() = default;
    virtual void SetTVMServiceTicket(const TString& ticket) = 0;
};

std::shared_ptr<ICredentialsProvider> CreateInsecureCredentialsProvider();

/*
 * Builder for direct usage of TVM. For Qloud environment use CreateTVMQloudCredentialsProvider
 */
std::shared_ptr<ICredentialsProvider> CreateTVMCredentialsProvider(const TString& secret, const ui32 srcClientId, const ui32 dstClientId, TIntrusivePtr<ILogger> logger = nullptr);
std::shared_ptr<ICredentialsProvider> CreateTVMCredentialsProvider(const NTvmAuth::NTvmApi::TClientSettings& settings, TIntrusivePtr<ILogger> logger = nullptr, const TString& dstAlias = GetTvmPqServiceName());

/*
 * forward TVM service ticket with ITMVCredentialsProvider method SetTVMServiceTicket ; threadsafe
 */
std::shared_ptr<ITVMCredentialsForwarder> CreateTVMCredentialsForwarder();

/*
 * tvmClient settings must contain LogBroker client id under an alias
 */
std::shared_ptr<ICredentialsProvider> CreateTVMCredentialsProvider(std::shared_ptr<NTvmAuth::TTvmClient> tvmClient, TIntrusivePtr<ILogger> logger = nullptr, const TString& dstAlias = GetTvmPqServiceName());

std::shared_ptr<ICredentialsProvider> CreateTVMQloudCredentialsProvider(const TString& srcAlias, const TString& dstAlias, TIntrusivePtr<ILogger> logger = nullptr, const TDuration refreshPeriod = TDuration::Minutes(10), ui32 port = 1);
std::shared_ptr<ICredentialsProvider> CreateTVMQloudCredentialsProvider(const ui32 srcId, const ui32 dstId, TIntrusivePtr<ILogger> logger = nullptr, const TDuration refreshPeriod = TDuration::Minutes(10), ui32 port = 1);
std::shared_ptr<ICredentialsProvider> CreateOAuthCredentialsProvider(const TString& token);

/*
 * Provider that takes IAM ticket from metadata service.
 */
std::shared_ptr<ICredentialsProvider> CreateIAMCredentialsForwarder(TIntrusivePtr<ILogger> logger);

/*
 * Provider that makes IAM ticket from JWT token.
 */
std::shared_ptr<ICredentialsProvider> CreateIAMJwtFileCredentialsForwarder(const TString& jwtKeyFilename, TIntrusivePtr<ILogger> logger,
    const TString& endpoint = "iam.api.cloud.yandex.net", const TDuration& refreshPeriod = TDuration::Hours(1),
    const TDuration& requestTimeout = TDuration::Seconds(10));

std::shared_ptr<ICredentialsProvider> CreateIAMJwtParamsCredentialsForwarder(const TString& jwtParams, TIntrusivePtr<ILogger> logger,
    const TString& endpoint = "iam.api.cloud.yandex.net", const TDuration& refreshPeriod = TDuration::Hours(1),
    const TDuration& requestTimeout = TDuration::Seconds(10));
} // namespace NPersQueue
