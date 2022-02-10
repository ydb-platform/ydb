/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProvider.h> 
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/platform/Environment.h> 
#include <aws/core/platform/OSVersionInfo.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/StringUtils.h> 
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/Version.h>
#include <aws/core/config/AWSProfileConfigLoader.h> 
#include <aws/core/utils/logging/LogMacros.h> 

namespace Aws
{
namespace Auth 
{ 
    AWS_CORE_API Aws::String GetConfigProfileFilename(); 
} 
namespace Client
{

static const char* CLIENT_CONFIG_TAG = "ClientConfiguration"; 

AWS_CORE_API Aws::String ComputeUserAgentString()
{
  Aws::StringStream ss;
  ss << "aws-sdk-cpp/" << Version::GetVersionString() << " " <<  Aws::OSVersionInfo::ComputeOSVersionString()
      << " " << Version::GetCompilerVersionString();
  return ss.str();
}

ClientConfiguration::ClientConfiguration() : 
    scheme(Aws::Http::Scheme::HTTPS), 
    useDualStack(false),
    maxConnections(25), 
    httpRequestTimeoutMs(0), 
    requestTimeoutMs(3000), 
    connectTimeoutMs(1000),
    enableTcpKeepAlive(true), 
    tcpKeepAliveIntervalMs(30000), 
    lowSpeedLimit(1), 
    proxyScheme(Aws::Http::Scheme::HTTP),
    proxyPort(0),
    executor(Aws::MakeShared<Aws::Utils::Threading::DefaultExecutor>(CLIENT_CONFIG_TAG)), 
    verifySSL(true),
    writeRateLimiter(nullptr),
    readRateLimiter(nullptr),
    httpLibOverride(Aws::Http::TransferLibType::DEFAULT_CLIENT),
    followRedirects(FollowRedirectsPolicy::DEFAULT), 
    disableExpectHeader(false),
    enableClockSkewAdjustment(true),
    enableHostPrefixInjection(true),
    enableEndpointDiscovery(false), 
    profileName(Aws::Auth::GetConfigProfileName()) 
{
    AWS_LOGSTREAM_DEBUG(CLIENT_CONFIG_TAG, "ClientConfiguration will use SDK Auto Resolved profile: [" << profileName << "] if not specified by users."); 
 
    // Initialize Retry Strategy 
    int maxAttempts; 
    Aws::String maxAttemptsString = Aws::Environment::GetEnv("AWS_MAX_ATTEMPTS"); 
    if (maxAttemptsString.empty()) 
    { 
        maxAttemptsString = Aws::Config::GetCachedConfigValue("max_attempts"); 
    } 
    // In case users specify 0 explicitly to disable retry. 
    if (maxAttemptsString == "0") 
    { 
        maxAttempts = 0; 
    } 
    else 
    { 
        maxAttempts = static_cast<int>(Aws::Utils::StringUtils::ConvertToInt32(maxAttemptsString.c_str())); 
        if (maxAttempts == 0) 
        { 
            AWS_LOGSTREAM_WARN(CLIENT_CONFIG_TAG, "Retry Strategy will use the default max attempts."); 
            maxAttempts = -1; 
        } 
    } 
 
    Aws::String retryMode = Aws::Environment::GetEnv("AWS_RETRY_MODE"); 
    if (retryMode.empty()) 
    { 
        retryMode = Aws::Config::GetCachedConfigValue("retry_mode"); 
    } 
    if (retryMode == "standard") 
    { 
        if (maxAttempts < 0) 
        { 
            retryStrategy = Aws::MakeShared<StandardRetryStrategy>(CLIENT_CONFIG_TAG); 
        } 
        else 
        { 
            retryStrategy = Aws::MakeShared<StandardRetryStrategy>(CLIENT_CONFIG_TAG, maxAttempts); 
        } 
    } 
    else 
    { 
        retryStrategy = Aws::MakeShared<DefaultRetryStrategy>(CLIENT_CONFIG_TAG); 
    } 
 
    // Automatically determine the AWS region from environment variables, configuration file and EC2 metadata. 
    region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION"); 
    if (!region.empty()) 
    { 
        return; 
    } 
 
    region = Aws::Environment::GetEnv("AWS_REGION"); 
    if (!region.empty()) 
    { 
        return; 
    } 
 
    region = Aws::Config::GetCachedConfigValue("region"); 
    if (!region.empty()) 
    { 
        return; 
    } 
 
    if (Aws::Utils::StringUtils::ToLower(Aws::Environment::GetEnv("AWS_EC2_METADATA_DISABLED").c_str()) != "true") 
    { 
        auto client = Aws::Internal::GetEC2MetadataClient(); 
        if (client) 
        { 
            region = client->GetCurrentRegion(); 
        } 
    } 
 
    if (!region.empty()) 
    { 
        return; 
    } 
 
    region = Aws::String(Aws::Region::US_EAST_1); 
}

ClientConfiguration::ClientConfiguration(const char* profile) : ClientConfiguration() 
{ 
    if (profile && Aws::Config::HasCachedConfigProfile(profile)) 
    { 
        this->profileName = Aws::String(profile); 
        AWS_LOGSTREAM_DEBUG(CLIENT_CONFIG_TAG, "Use user specified profile: [" << this->profileName << "] for ClientConfiguration."); 
        auto tmpRegion = Aws::Config::GetCachedConfigProfile(this->profileName).GetRegion(); 
        if (!tmpRegion.empty()) 
        { 
            region = tmpRegion; 
        } 
        return; 
    } 
    AWS_LOGSTREAM_WARN(CLIENT_CONFIG_TAG, "User specified profile: [" << profile << "] is not found, will use the SDK resolved one."); 
} 
 
} // namespace Client
} // namespace Aws
