/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/auth/SSOCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/LogMacros.h>

using namespace Aws::Auth;

static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";
static const char DefaultCredentialsProviderChainTag[] = "DefaultAWSCredentialsProviderChain";

AWSCredentials AWSCredentialsProviderChain::GetAWSCredentials()
{
    for (auto&& credentialsProvider : m_providerChain)
    {
        AWSCredentials credentials = credentialsProvider->GetAWSCredentials();
        if (!credentials.GetAWSAccessKeyId().empty() && !credentials.GetAWSSecretKey().empty())
        {
            return credentials;
        }
    }

    return AWSCredentials();
}

DefaultAWSCredentialsProviderChain::DefaultAWSCredentialsProviderChain() : AWSCredentialsProviderChain()
{
    AddProvider(Aws::MakeShared<EnvironmentAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<ProfileConfigFileAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<ProcessCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<STSAssumeRoleWebIdentityCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<SSOCredentialsProvider>(DefaultCredentialsProviderChainTag));
    
    //ECS TaskRole Credentials only available when ENVIRONMENT VARIABLE is set
    const auto relativeUri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI);
    AWS_LOGSTREAM_DEBUG(DefaultCredentialsProviderChainTag, "The environment variable value " << AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI
            << " is " << relativeUri);

    const auto absoluteUri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI);
    AWS_LOGSTREAM_DEBUG(DefaultCredentialsProviderChainTag, "The environment variable value " << AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI
            << " is " << absoluteUri);

    const auto ec2MetadataDisabled = Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
    AWS_LOGSTREAM_DEBUG(DefaultCredentialsProviderChainTag, "The environment variable value " << AWS_EC2_METADATA_DISABLED
            << " is " << ec2MetadataDisabled);

    if (!relativeUri.empty())
    {
        AddProvider(Aws::MakeShared<TaskRoleCredentialsProvider>(DefaultCredentialsProviderChainTag, relativeUri.c_str()));
        AWS_LOGSTREAM_INFO(DefaultCredentialsProviderChainTag, "Added ECS metadata service credentials provider with relative path: ["
                << relativeUri << "] to the provider chain.");
    }
    else if (!absoluteUri.empty())
    {
        const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
        AddProvider(Aws::MakeShared<TaskRoleCredentialsProvider>(DefaultCredentialsProviderChainTag,
                    absoluteUri.c_str(), token.c_str()));

        //DO NOT log the value of the authorization token for security purposes.
        AWS_LOGSTREAM_INFO(DefaultCredentialsProviderChainTag, "Added ECS credentials provider with URI: ["
                << absoluteUri << "] to the provider chain with a" << (token.empty() ? "n empty " : " non-empty ")
                << "authorization token.");
    }
    else if (Aws::Utils::StringUtils::ToLower(ec2MetadataDisabled.c_str()) != "true")
    {
        AddProvider(Aws::MakeShared<InstanceProfileCredentialsProvider>(DefaultCredentialsProviderChainTag));
        AWS_LOGSTREAM_INFO(DefaultCredentialsProviderChainTag, "Added EC2 metadata service credentials provider to the provider chain.");
    }
}

DefaultAWSCredentialsProviderChain::DefaultAWSCredentialsProviderChain(const DefaultAWSCredentialsProviderChain& chain) {
    for (const auto& provider: chain.GetProviders()) {
        AddProvider(provider);
    }
}
