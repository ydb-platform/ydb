/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/core/Version.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/Aws.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/CRTLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/Globals.h>
#include <aws/core/external/cjson/cJSON.h>
#include <aws/core/monitoring/MonitoringManager.h>
#include <aws/core/net/Net.h>
#include <aws/core/config/AWSProfileConfigLoader.h>
#include <aws/core/internal/AWSHttpResourceClient.h>

namespace Aws
{
    static const char* ALLOCATION_TAG = "Aws_Init_Cleanup";

    void InitAPI(const SDKOptions &options)
    {
#ifdef USE_AWS_MEMORY_MANAGEMENT
        if(options.memoryManagementOptions.memoryManager)
        {
            Aws::Utils::Memory::InitializeAWSMemorySystem(*options.memoryManagementOptions.memoryManager);
        }
#endif // USE_AWS_MEMORY_MANAGEMENT
        Aws::InitializeCrt();
        Aws::Client::CoreErrorsMapper::InitCoreErrorsMapper();
        if(options.loggingOptions.logLevel != Aws::Utils::Logging::LogLevel::Off)
        {
            if(options.loggingOptions.logger_create_fn)
            {
                Aws::Utils::Logging::InitializeAWSLogging(options.loggingOptions.logger_create_fn());
            }
            else
            {
                Aws::Utils::Logging::InitializeAWSLogging(
                        Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(ALLOCATION_TAG, options.loggingOptions.logLevel, options.loggingOptions.defaultLogPrefix));
            }
            if(options.loggingOptions.crt_logger_create_fn)
            {
                Aws::Utils::Logging::InitializeCRTLogging(options.loggingOptions.crt_logger_create_fn());
            }
            else
            {
                Aws::Utils::Logging::InitializeCRTLogging(
                        Aws::MakeShared<Aws::Utils::Logging::DefaultCRTLogSystem>(ALLOCATION_TAG, options.loggingOptions.logLevel));
            }
            // For users to better debugging in case multiple versions of SDK installed
            AWS_LOGSTREAM_INFO(ALLOCATION_TAG, "Initiate AWS SDK for C++ with Version:" << Aws::String(Aws::Version::GetVersionString()));
        }

        Aws::Config::InitConfigAndCredentialsCacheManager();

        if (options.ioOptions.clientBootstrap_create_fn)
        {
            Aws::SetDefaultClientBootstrap(options.ioOptions.clientBootstrap_create_fn());
        }
        else
        {
            Aws::Crt::Io::EventLoopGroup eventLoopGroup;
            Aws::Crt::Io::DefaultHostResolver defaultHostResolver(eventLoopGroup, 8, 30);
            auto clientBootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(ALLOCATION_TAG, eventLoopGroup, defaultHostResolver);
            clientBootstrap->EnableBlockingShutdown();
            Aws::SetDefaultClientBootstrap(clientBootstrap);
        }

        if (options.ioOptions.tlsConnectionOptions_create_fn)
        {
            Aws::SetDefaultTlsConnectionOptions(options.ioOptions.tlsConnectionOptions_create_fn());
        }
        else
        {
            Aws::Crt::Io::TlsContextOptions tlsCtxOptions = Aws::Crt::Io::TlsContextOptions::InitDefaultClient();
            Aws::Crt::Io::TlsContext tlsContext(tlsCtxOptions, Aws::Crt::Io::TlsMode::CLIENT);
            auto tlsConnectionOptions = Aws::MakeShared<Aws::Crt::Io::TlsConnectionOptions>(ALLOCATION_TAG, tlsContext.NewConnectionOptions());
            Aws::SetDefaultTlsConnectionOptions(tlsConnectionOptions);
        }

        if (options.cryptoOptions.aes_CBCFactory_create_fn)
        {
            Aws::Utils::Crypto::SetAES_CBCFactory(options.cryptoOptions.aes_CBCFactory_create_fn());
        }

        if(options.cryptoOptions.aes_CTRFactory_create_fn)
        {
            Aws::Utils::Crypto::SetAES_CTRFactory(options.cryptoOptions.aes_CTRFactory_create_fn());
        }

        if(options.cryptoOptions.aes_GCMFactory_create_fn)
        {
            Aws::Utils::Crypto::SetAES_GCMFactory(options.cryptoOptions.aes_GCMFactory_create_fn());
        }

        if(options.cryptoOptions.md5Factory_create_fn)
        {
            Aws::Utils::Crypto::SetMD5Factory(options.cryptoOptions.md5Factory_create_fn());
        }

        if(options.cryptoOptions.sha1Factory_create_fn)
        {
            Aws::Utils::Crypto::SetSha1Factory(options.cryptoOptions.sha1Factory_create_fn());
        }

        if(options.cryptoOptions.sha256Factory_create_fn)
        {
            Aws::Utils::Crypto::SetSha256Factory(options.cryptoOptions.sha256Factory_create_fn());
        }

        if(options.cryptoOptions.sha256HMACFactory_create_fn)
        {
            Aws::Utils::Crypto::SetSha256HMACFactory(options.cryptoOptions.sha256HMACFactory_create_fn());
        }

        if (options.cryptoOptions.aes_KeyWrapFactory_create_fn)
        {
            Aws::Utils::Crypto::SetAES_KeyWrapFactory(options.cryptoOptions.aes_KeyWrapFactory_create_fn());
        }

        if(options.cryptoOptions.secureRandomFactory_create_fn)
        {
            Aws::Utils::Crypto::SetSecureRandomFactory(options.cryptoOptions.secureRandomFactory_create_fn());
        }

        Aws::Utils::Crypto::SetInitCleanupOpenSSLFlag(options.cryptoOptions.initAndCleanupOpenSSL);
        Aws::Utils::Crypto::InitCrypto();

        if(options.httpOptions.httpClientFactory_create_fn)
        {
            Aws::Http::SetHttpClientFactory(options.httpOptions.httpClientFactory_create_fn());
        }

        Aws::Http::SetInitCleanupCurlFlag(options.httpOptions.initAndCleanupCurl);
        Aws::Http::SetInstallSigPipeHandlerFlag(options.httpOptions.installSigPipeHandler);
        Aws::Http::SetCompliantRfc3986Encoding(options.httpOptions.compliantRfc3986Encoding);
        Aws::Http::InitHttp();
        Aws::InitializeEnumOverflowContainer();
        cJSON_AS4CPP_Hooks hooks;
        hooks.malloc_fn = [](size_t sz) { return Aws::Malloc("cJSON_AS4CPP_Tag", sz); };
        hooks.free_fn = Aws::Free;
        cJSON_AS4CPP_InitHooks(&hooks);
        Aws::Net::InitNetwork();
        Aws::Internal::InitEC2MetadataClient();
        Aws::Monitoring::InitMonitoring(options.monitoringOptions.customizedMonitoringFactory_create_fn);
    }

    void ShutdownAPI(const SDKOptions& options)
    {
        Aws::Monitoring::CleanupMonitoring();
        Aws::Internal::CleanupEC2MetadataClient();
        Aws::Net::CleanupNetwork();
        Aws::CleanupEnumOverflowContainer();
        Aws::Http::CleanupHttp();
        Aws::Utils::Crypto::CleanupCrypto();

        Aws::Config::CleanupConfigAndCredentialsCacheManager();

        Aws::Client::CoreErrorsMapper::CleanupCoreErrorsMapper();
        Aws::CleanupCrt();
        if (options.loggingOptions.logLevel != Aws::Utils::Logging::LogLevel::Off)
        {
            Aws::Utils::Logging::ShutdownCRTLogging();
            Aws::Utils::Logging::ShutdownAWSLogging();
        }
#ifdef USE_AWS_MEMORY_MANAGEMENT
        if(options.memoryManagementOptions.memoryManager)
        {
            Aws::Utils::Memory::ShutdownAWSMemorySystem();
        }
#endif // USE_AWS_MEMORY_MANAGEMENT
    }
}
