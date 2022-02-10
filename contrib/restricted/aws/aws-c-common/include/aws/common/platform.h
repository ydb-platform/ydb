#ifndef AWS_COMMON_PLATFORM_H
#define AWS_COMMON_PLATFORM_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/config.h>

#ifdef _WIN32
#    define AWS_OS_WINDOWS
#elif __APPLE__
#    define AWS_OS_APPLE
#    include "TargetConditionals.h"
#    if defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE
#        define AWS_OS_IOS
#    else
#        define AWS_OS_MACOS
#    endif
#elif __linux__
#    define AWS_OS_LINUX
#endif

#if defined(_POSIX_VERSION)
#    define AWS_OS_POSIX
#endif

#endif /* AWS_COMMON_PLATFORM_H */
