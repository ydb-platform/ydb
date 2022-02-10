/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
    /**
     * AWS Regions
     */
    namespace Region
    {
        // AWS_GLOBAL is a pseudo region that can be used to tell SDK to use the service global endpoint if there is any.
        // You can specify this region to corresponding environment variable, config file item and in your code.
        // For services without global region, the request will be directed to us-east-1
        static const char AWS_GLOBAL[] = "aws-global";
        static const char US_EAST_1[] = "us-east-1";
        static const char US_EAST_2[] = "us-east-2";
        static const char US_WEST_1[] = "us-west-1";
        static const char US_WEST_2[] = "us-west-2";
        static const char AF_SOUTH_1[] = "af-south-1";
        static const char EU_WEST_1[] = "eu-west-1";
        static const char EU_WEST_2[] = "eu-west-2";
        static const char EU_WEST_3[] = "eu-west-3";
        static const char EU_CENTRAL_1[] = "eu-central-1";
        static const char EU_NORTH_1[] = "eu-north-1";
        static const char AP_EAST_1[] = "ap-east-1";
        static const char AP_SOUTH_1[] = "ap-south-1";
        static const char AP_SOUTHEAST_1[] = "ap-southeast-1";
        static const char AP_SOUTHEAST_2[] = "ap-southeast-2";
        static const char AP_NORTHEAST_1[] = "ap-northeast-1";
        static const char AP_NORTHEAST_2[] = "ap-northeast-2";
        static const char AP_NORTHEAST_3[] = "ap-northeast-3";
        static const char SA_EAST_1[] = "sa-east-1";
        static const char CA_CENTRAL_1[] = "ca-central-1";
        static const char CN_NORTH_1[] = "cn-north-1";
        static const char CN_NORTHWEST_1[] = "cn-northwest-1";
        static const char ME_SOUTH_1[] = "me-south-1";
        static const char US_GOV_WEST_1[] = "us-gov-west-1";
        static const char US_GOV_EAST_1[] = "us-gov-east-1";

        // If a pseudo region, for example, aws-global or us-east-1-fips is provided, it should be converted to the region name used for signing.
        Aws::String AWS_CORE_API ComputeSignerRegion(const Aws::String& region);
    }

} // namespace Aws

