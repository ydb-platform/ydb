/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

namespace Aws
{
    namespace Utils
    {
        class EnumParseOverflowContainer;
    }
    /**
     * This is used to handle the Enum round tripping problem
     * for when a service updates their enumerations, but the user does not
     * have an up to date client. This container will be initialized during Aws::InitAPI
     * and will be cleaned on Aws::ShutdownAPI.
     */
    AWS_CORE_API Utils::EnumParseOverflowContainer* GetEnumOverflowContainer();

    /**
     * Initializes a global overflow container to a new instance.
     * This should only be called once from within Aws::InitAPI
     */
    void InitializeEnumOverflowContainer();

    /**
     * Destroys the global overflow container instance.
     * This should only be called once from within Aws::ShutdownAPI
     */
    void CleanupEnumOverflowContainer();
}
